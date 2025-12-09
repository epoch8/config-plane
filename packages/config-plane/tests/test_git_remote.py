import pytest
import subprocess
from pathlib import Path
from config_plane.impl.git import create_git_config_repo, GitConfigRepo


@pytest.fixture
def remote_repo(tmp_path):
    """Creates a bare git repo to serve as remote."""
    origin = tmp_path / "origin"
    origin.mkdir()
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=master"],
        cwd=origin,
        check=True,
        capture_output=True,
    )

    # Init commit
    init_dir = tmp_path / "init"
    init_dir.mkdir()
    subprocess.run(
        ["git", "clone", str(origin), "."],
        cwd=init_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "config", "user.name", "Test"], cwd=init_dir, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@test"], cwd=init_dir, check=True
    )
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "Init"], cwd=init_dir, check=True
    )
    subprocess.run(["git", "push", "origin", "master"], cwd=init_dir, check=True)

    return str(origin)


def test_git_remote_sync(tmp_path, remote_repo):
    path_a = tmp_path / "repo_a"
    path_b = tmp_path / "repo_b"

    # 1. User A inits repo
    repo_a = create_git_config_repo(path_a, remote_url=remote_repo)
    # Configure user for repo_a to allow committing
    subprocess.run(
        ["git", "config", "user.name", "User A"], cwd=repo_a.work_path, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "a@test"], cwd=repo_a.work_path, check=True
    )

    repo_a.set("foo", b"bar")
    repo_a.commit()  # Should push

    # 2. User B inits repo (should clone)
    repo_b = create_git_config_repo(path_b, remote_url=remote_repo)
    subprocess.run(
        ["git", "config", "user.name", "User B"], cwd=repo_b.work_path, check=True
    )
    subprocess.run(
        ["git", "config", "user.email", "b@test"], cwd=repo_b.work_path, check=True
    )

    assert repo_b.get("foo") == b"bar"

    # 3. User B modifies
    repo_b.set("foo", b"baz")
    repo_b.commit()  # Push

    # 4. User A reloads (should pull)
    assert repo_a.get("foo") == b"bar"  # Old state
    repo_a.reload()
    assert repo_a.get("foo") == b"baz"  # New state
