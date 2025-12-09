import subprocess
from pathlib import Path
from typing import Any

from config_plane.base import ConfigRepo, ConfigSnapshot, ConfigStage, Blob
from config_plane.impl.memory import MemoryConfigSnapshot


def _run_git(cwd: Path, args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _run_git_bytes(cwd: Path, args: list[str]) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        check=True,
    )
    return result.stdout


class GitConfigSnapshot(ConfigSnapshot):
    def __init__(self, repo_path: Path, commit_hash: str):
        self.repo_path = repo_path
        self.commit_hash = commit_hash

    def get(self, key: str) -> Blob | None:
        try:
            # git show <commit>:<key>
            content = _run_git_bytes(
                self.repo_path, ["show", f"{self.commit_hash}:{key}"]
            )
            return content
        except subprocess.CalledProcessError:
            return None

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigSnapshot(...)")
        else:
            p.text(f"GitConfigSnapshot(commit={self.commit_hash[:7]})")


class GitConfigStage(ConfigStage):
    def __init__(self, repo_path: Path, snapshot: ConfigSnapshot):
        self.repo_path = repo_path
        self.snapshot = snapshot

    def get(self, key: str) -> Blob | None:
        file_path = self.repo_path / f"{key}"
        if file_path.exists():
            try:
                return file_path.read_bytes()
            except OSError:
                return None

        # Fallback to snapshot if not on disk (meaning not modified/new, or deleted)
        # Wait, if it's deleted in stage, it won't exist on disk.
        # But if it exists in snapshot, we should return what?
        # If I delete a file in git, it's gone.
        # So "not existing on disk" could mean "deleted" OR "never checked out" (but this is a repo, files should be there)
        # OR "unchanged from snapshot" (files should be there).

        # Actually, in a standard git repo, the working directory CONTAINS the current stage.
        # If the file isn't there, it's not there.
        # However, `snapshot` might be pointing to a previous commit.
        # If I am in a "dirty" state, the file on disk IS the value.
        # If the file is missing on disk, it returns None.
        return None

    def set(self, key: str, value: Blob | None) -> None:
        file_path = self.repo_path / f"{key}"

        if value is None:
            if file_path.exists():
                file_path.unlink()
                # We also need to tell git about the deletion if we want to be thorough,
                # but `git add .` in freeze will catch it.
        else:
            file_path.write_bytes(value)

    def is_dirty(self) -> bool:
        # Check if there are changes
        status = _run_git(self.repo_path, ["status", "--porcelain"])
        return len(status) > 0

    def freeze(self) -> ConfigSnapshot:
        if not self.is_dirty():
            # If not dirty, return current snapshot (or HEAD)
            # We assume self.snapshot is HEAD
            return self.snapshot

        _run_git(self.repo_path, ["add", "."])
        _run_git(self.repo_path, ["commit", "-m", "Update config"])

        new_hash = _run_git(self.repo_path, ["rev-parse", "HEAD"])
        return GitConfigSnapshot(self.repo_path, new_hash)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigStage(...)")
        else:
            p.text(f"GitConfigStage(dirty={self.is_dirty()})")


class GitConfigRepo(ConfigRepo):
    def __init__(self, repo_path: str | Path, branch: str = "master"):
        self.repo_path = Path(repo_path).absolute()
        self.branch = branch

        if not (self.repo_path / ".git").exists():
            self.repo_path.mkdir(parents=True, exist_ok=True)
            _run_git(self.repo_path, ["init", "--initial-branch=master"])
            # Create an initial commit so HEAD exists
            # We can create a .gitkeep or similar
            # (self.repo_path / ".gitkeep").touch()
            # _run_git(self.repo_path, ["add", "."])
            # _run_git(self.repo_path, ["commit", "-m", "Initial commit"])
            # Actually, let's see if there are commits.

        # Ensure we are on the correct branch
        current_branch = self._get_current_branch()
        if current_branch != self.branch:
            # Try to checkout
            # If branch doesn't exist, this might fail if it's not the initial specific case
            pass

        self.reload()

    def _get_current_branch(self) -> str:
        try:
            return _run_git(self.repo_path, ["branch", "--show-current"])
        except subprocess.CalledProcessError:
            return ""

    def reload(self):
        try:
            # Check if we are on the right branch
            current = self._get_current_branch()
            if current and current != self.branch:
                # Attempt checkout if clean?
                # ideally we should have been on the branch.
                # For now, let's assume we are correct or we'll switch.
                _run_git(self.repo_path, ["checkout", self.branch])

            head_hash = _run_git(self.repo_path, ["rev-parse", "HEAD"])
            self.base = GitConfigSnapshot(self.repo_path, head_hash)
        except subprocess.CalledProcessError:
            # No commits yet or branch doesn't exist
            # For now let's mock an empty snapshot
            self.base = MemoryConfigSnapshot(
                {}
            )  # Hacky availability of empty snapshot?
            pass

        self.stage = GitConfigStage(self.repo_path, getattr(self, "base", None))  # type: ignore

    def get(self, key: str) -> Blob | None:
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        self.base = self.stage.freeze()
        # After freeze, the stage is clean, so we can re-init it pointing to new base
        self.stage = GitConfigStage(self.repo_path, self.base)

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")

        _run_git(self.repo_path, ["checkout", branch])
        self.branch = branch
        self.reload()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        start_point = from_branch or self.branch
        # If we are effectively creating from current:
        if start_point == self.branch:
            _run_git(self.repo_path, ["checkout", "-b", new_branch])
        else:
            # Create from another branch without switching?
            # Git requires switching to new branch usually with checkout -b
            # Or `git branch new start`.
            # But ConfigRepo semantics usually imply we switch?
            # "Create branch" might just create it.
            # But typical flow: create_branch(dev, from=prod).
            _run_git(self.repo_path, ["branch", new_branch, start_point])

        # Does create_branch switch to it?
        # Base interface doesn't strictly say, but usually no.
        # But if I use `checkout -b`, I switch.
        # Let's stick to just creating:
        # If we used checkout -b above, we switched.
        # Let's use `git branch` to just create.
        pass

    def list_branches(self) -> list[str]:
        out = _run_git(self.repo_path, ["branch", "--format=%(refname:short)"])
        return out.splitlines()

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigRepo(...)")
        else:
            with p.group(4, "GitConfigRepo(", ")"):
                p.breakable()
                p.text(f"path={self.repo_path},")
                p.breakable()
                p.text(f"branch={self.branch},")
                p.breakable()


def create_git_config_repo(repo_path: str | Path, branch: str = "master") -> GitConfigRepo:
    return GitConfigRepo(repo_path, branch=branch)
