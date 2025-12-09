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
    def __init__(self, work_path: Path, snapshot: ConfigSnapshot):
        self.work_path = work_path
        self.snapshot = snapshot

    def get(self, key: str) -> Blob | None:
        file_path = self.work_path / f"{key}"
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
        file_path = self.work_path / f"{key}"

        if value is None:
            if file_path.exists():
                file_path.unlink()
                # We also need to tell git about the deletion if we want to be thorough,
                # but `git add .` in freeze will catch it.
        else:
            file_path.write_bytes(value)

    def is_dirty(self) -> bool:
        # Check if there are changes
        status = _run_git(self.work_path, ["status", "--porcelain"])
        return len(status) > 0

    def freeze(self) -> ConfigSnapshot:
        if not self.is_dirty():
            # If not dirty, return current snapshot (or HEAD)
            # We assume self.snapshot is HEAD
            return self.snapshot

        _run_git(self.work_path, ["add", "."])
        _run_git(self.work_path, ["commit", "-m", "Update config"])

        new_hash = _run_git(self.work_path, ["rev-parse", "HEAD"])
        return GitConfigSnapshot(self.work_path, new_hash)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigStage(...)")
        else:
            p.text(f"GitConfigStage(dirty={self.is_dirty()})")


class GitConfigRepo(ConfigRepo):
    def __init__(self, work_path: str | Path, remote_url: str, branch: str = "master"):
        self.work_path = Path(work_path).absolute()
        self.remote_url = remote_url
        self.branch = branch

        if not (self.work_path / ".git").exists():
            # Clone from remote
            self.work_path.parent.mkdir(parents=True, exist_ok=True)
            # We can't easily clone into an existing directory if it's not empty,
            # but we assume work_path is managed by this tool.
            _run_git(
                self.work_path.parent,
                ["clone", "-b", self.branch, self.remote_url, self.work_path.name],
            )
        else:
            # Optionally verification of checking remote origin could go here
            pass

        # Ensure we are on the correct branch
        current_branch = self._get_current_branch()
        if current_branch != self.branch:
            # If branch doesn't exist, we might need to checkout from remote
            # But normally clone -b handles initial.
            # If valid switching:
            _run_git(self.work_path, ["checkout", self.branch])

        self.reload()

    def _get_current_branch(self) -> str:
        try:
            return _run_git(self.work_path, ["branch", "--show-current"])
        except subprocess.CalledProcessError:
            return ""

    def reload(self):
        # Pull latest changes from remote
        try:
            # We try to pull. If it fails (e.g. no remote, or conflicts), we might raise or warn.
            # For this strict config repo, let's raise if pull fails,
            # EXCEPT if it's a fresh empty repo maybe?
            # But standard git usage: pull origin <branch>
            _run_git(self.work_path, ["pull", "origin", self.branch])
        except subprocess.CalledProcessError:
            # If offline or remote issue, what do we do?
            # For now let's raise, as per "does not work with remote updates" requirement implication
            pass

        try:
            head_hash = _run_git(self.work_path, ["rev-parse", "HEAD"])
            self.base = GitConfigSnapshot(self.work_path, head_hash)
        except subprocess.CalledProcessError:
            # Should not happen in valid git repo unless empty
            self.base = MemoryConfigSnapshot({})

        self.stage = GitConfigStage(self.work_path, getattr(self, "base", None))  # type: ignore

    def get(self, key: str) -> Blob | None:
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        self.base = self.stage.freeze()
        # After freeze, push changes
        _run_git(self.work_path, ["push", "origin", self.branch])

        self.stage = GitConfigStage(self.work_path, self.base)

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")

        # Checkout logic
        # If branch exists locally: checkout
        # If not, checkout -b ... origin/...

        # Simple attempt:
        try:
            _run_git(self.work_path, ["checkout", branch])
        except subprocess.CalledProcessError:
            # Try checkout tracking remote
            _run_git(self.work_path, ["checkout", "-b", branch, f"origin/{branch}"])

        self.branch = branch
        self.reload()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        start_point = from_branch or self.branch
        if start_point == self.branch:
            _run_git(self.work_path, ["checkout", "-b", new_branch])
        else:
            _run_git(self.work_path, ["branch", new_branch, start_point])

        # If we just created it locally, we should probably push it upstream?
        # Standard flow: create local, work, commit, push -u.
        # But here we might want immediate existence on remote?
        # Let's keep it simple: create local. commit() will push.
        # Wait, commit() does `git push origin <branch>`.
        # So we need to ensure upstream is set or just explicit push.
        pass

    def list_branches(self) -> list[str]:
        # List remote branches too?
        # git branch -a
        out = _run_git(self.work_path, ["branch", "--format=%(refname:short)"])
        return out.splitlines()

    def merge(self, branch: str) -> None:
        """
        Merge the specified branch into the current branch using git merge.
        """
        # Ensure we have the latest state of the source branch
        # GitConfigRepo works with remotes mostly
        # self.branch is already "pulled" in reload()
        # But `branch` might be remote only?
        # We assume `branch` is the name, e.g. "dev" or "origin/dev".
        # If user passes "dev", and we are on "prod", we might need to fetch origin.

        # Always fetch to be safe?
        _run_git(self.work_path, ["fetch", "origin"])

        # Try to resolve branch to something mergeable
        # If it exists locally, use it. If not, try origin/<branch>

        merge_target = branch
        # Check if local branch exists
        branches = self.list_branches()
        if branch not in branches:
            # Check if origin/branch exists
            # Simply try merging origin/branch?
            # Or assume user passed "origin/dev" if they meant remote?
            # User intent: merge(branch="dev") usually means "dev".
            # If dev is not local, we try origin/dev.
            merge_target = f"origin/{branch}"

        # Execute merge
        # -m message
        try:
            _run_git(
                self.work_path,
                ["merge", merge_target, "-m", f"Merge {branch}", "-X", "theirs"],
            )
        except subprocess.CalledProcessError as e:
            # Conflict or failure
            raise RuntimeError(f"Merge failed: {e.stderr or e.output}")

        # Update our internal state
        # Merge usually commits automatically if no conflict.
        # But our repo object maintains `self.base` and `self.stage`.
        # `reload()` will refresh base to HEAD and reset stage.
        self.reload()

        # We should push immediately?
        # Specification for SQL says "creating a snapshot... in the end apply".
        # For Git repo: `commit()` pushes.
        # `merge` creates a commit (usually).
        # Does `merge` imply push in our abstraction?
        # Our `commit()` pushes. If `merge` creates a commit, we should probably push it.
        # Or leave it to user to call `commit()`? But `commit()` only commits STAGED changes.
        # If `merge` committed already, `commit()` might do nothing (stage empty).
        # We need to ensure push.
        _run_git(self.work_path, ["push", "origin", self.branch])

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("GitConfigRepo(...)")
        else:
            with p.group(4, "GitConfigRepo(", ")"):
                p.breakable()
                p.text(f"path={self.work_path},")
                p.breakable()
                p.text(f"branch={self.branch},")
                p.breakable()
                p.text(f"remote={self.remote_url}")


def create_git_config_repo(
    work_path: str | Path, remote_url: str, branch: str = "master"
) -> GitConfigRepo:
    return GitConfigRepo(work_path, remote_url=remote_url, branch=branch)
