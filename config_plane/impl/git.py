import json
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


class GitConfigSnapshot(ConfigSnapshot):
    def __init__(self, repo_path: Path, commit_hash: str):
        self.repo_path = repo_path
        self.commit_hash = commit_hash

    def get(self, key: str) -> dict | None:
        try:
            # git show <commit>:<key>.json
            content = _run_git(
                self.repo_path, ["show", f"{self.commit_hash}:{key}.json"]
            )
            return json.loads(content)
        except subprocess.CalledProcessError:
            return None
        except json.JSONDecodeError:
            # Should not happen if we control writes, but safety first
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
        file_path = self.repo_path / f"{key}.json"
        if file_path.exists():
            try:
                with open(file_path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
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
        file_path = self.repo_path / f"{key}.json"

        if value is None:
            if file_path.exists():
                file_path.unlink()
                # We also need to tell git about the deletion if we want to be thorough,
                # but `git add .` in freeze will catch it.
        else:
            with open(file_path, "w") as f:
                json.dump(value, f, indent=2)

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
    def __init__(self, repo_path: str | Path):
        self.repo_path = Path(repo_path).absolute()

        if not (self.repo_path / ".git").exists():
            self.repo_path.mkdir(parents=True, exist_ok=True)
            _run_git(self.repo_path, ["init"])
            # Create an initial commit so HEAD exists
            # We can create a .gitkeep or similar
            # (self.repo_path / ".gitkeep").touch()
            # _run_git(self.repo_path, ["add", "."])
            # _run_git(self.repo_path, ["commit", "-m", "Initial commit"])
            # Actually, let's see if there are commits.

        self.reload()

    def reload(self):
        try:
            head_hash = _run_git(self.repo_path, ["rev-parse", "HEAD"])
            self.base = GitConfigSnapshot(self.repo_path, head_hash)
        except subprocess.CalledProcessError:
            # No commits yet
            # We can use a dummy snapshot or handle empty repo
            # Let's assume empty repo has no data
            # We can't really validly implement "get" on a non-existent commit.
            # But the Stage can effectively write the first files.
            # We'll use a special "empty" snapshot behavior if needed,
            # or just rely on stage writing files.

            # For now let's mock an empty snapshot
            self.base = MemoryConfigSnapshot(
                {}
            )  # Hacky availability of empty snapshot?
            # Better: GitConfigSnapshot with None hash?
            pass

        self.stage = GitConfigStage(self.repo_path, getattr(self, "base", None))  # type: ignore

    def get(self, key: str) -> Blob | None:
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def commit(self) -> None:
        self.base = self.stage.freeze()
        # After freeze, the stage is clean, so we can re-init it pointing to new base
        self.stage = GitConfigStage(self.repo_path, self.base)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigRepo(...)")
        else:
            with p.group(4, "GitConfigRepo(", ")"):
                p.breakable()
                p.text(f"path={self.repo_path},")
                p.breakable()


def create_git_config_repo(repo_path: str | Path) -> ConfigRepo:
    return GitConfigRepo(repo_path)
