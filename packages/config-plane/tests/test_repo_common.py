from pathlib import Path
from typing import Callable
import subprocess


import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config_plane.base import ConfigRepo
from config_plane.impl.memory import create_memory_config_repo, MemoryRepoData
from config_plane.impl.git import create_git_config_repo

from config_plane.impl.sql import create_sql_config_repo, Base

# Factory function signature: (tmp_path: Path) -> ConfigRepo
RepoFactory = Callable[[Path], ConfigRepo]
# Persistence factory signature: (tmp_path: Path) -> Generator[ConfigRepo, None, None]
# The generator yields the repo. The setup is done before yield, teardown (if any) after.
# Actually, for persistence tests, we need to create *two* instances on the same data.
# So the fixture should probably return a helper or we parametrize the test logic itself.

# Better approach for persistence:
# We pass a "RepoProvider" object/function that can create a repo at a location.
# For persistence test, we call it, do stuff, close it (if needed), then call it again on same location.


class RepoProvider:
    def create(self, path: Path) -> ConfigRepo:
        raise NotImplementedError()

    def cleanup(self, repo: ConfigRepo) -> None:
        pass


class MemoryRepoProvider(RepoProvider):
    def __init__(self):
        self.data: MemoryRepoData = {}

    def create(self, path: Path) -> ConfigRepo:
        # Memory repo ignores path, but uses shared dict
        return create_memory_config_repo(self.data)


class GitRepoProvider(RepoProvider):
    def create(self, path: Path) -> ConfigRepo:
        origin_path = path / "origin"
        if not origin_path.exists():
            origin_path.mkdir()
            # Init bare repo
            subprocess.run(
                ["git", "init", "--bare", "--initial-branch=master"],
                cwd=origin_path,
                check=True,
                capture_output=True,
            )
            # To have 'master' branch available for cloning, we need a commit.
            # We can clone it, commit, push.
            init_cwd = path / "init_temp"
            init_cwd.mkdir()
            subprocess.run(
                ["git", "clone", str(origin_path), "."],
                cwd=init_cwd,
                check=True,
                capture_output=True,
            )
            # Config user
            subprocess.run(
                ["git", "config", "user.name", "Test"], cwd=init_cwd, check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "test@test"], cwd=init_cwd, check=True
            )

            # Commit
            subprocess.run(
                ["git", "commit", "--allow-empty", "-m", "Init"],
                cwd=init_cwd,
                check=True,
            )
            subprocess.run(
                ["git", "push", "origin", "master"], cwd=init_cwd, check=True
            )

            # Cleanup
            import shutil

            shutil.rmtree(init_cwd)

        # We need a unique path for each repo instance if we want them to cooperate or be distinct
        # But this provider mainly tests single repo lifecycle or persistence on same path.
        # Persistence test calls create() twice.
        # If we use strict persistence test which reuses the same path, we need to handle that.

        # However, `create_git_config_repo` now clones if empty.
        # If we pass the same `work_path`, it will just reuse it.
        # But `test_repo_persistence` uses `tmp_path`.

        # We should use a specific subdir for the "local" repo
        work_path = path / "work-repo"

        return create_git_config_repo(work_path, remote_url=str(origin_path))


class SqlRepoProvider(RepoProvider):
    def __init__(self):
        self.engine = None

    def create(self, path: Path) -> ConfigRepo:
        db_path = path / "config.db"
        db_url = f"sqlite:///{db_path}"

        # We need to recreate engine/sessionmaker to simulate fresh start if called multiple times?
        # Or just creating a new session is enough?
        # For persistence check, we want to simulate application restart.

        if self.engine:
            self.engine.dispose()

        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        return create_sql_config_repo(Session)

    def cleanup(self, repo: ConfigRepo) -> None:
        if hasattr(repo, "session"):
            repo.session.close()  # type: ignore
        if self.engine:
            self.engine.dispose()


PROVIDERS = [
    MemoryRepoProvider,
    GitRepoProvider,
    SqlRepoProvider,
]
PROVIDER_IDS = ["memory", "git", "sql"]


@pytest.mark.parametrize("provider_cls", PROVIDERS, ids=PROVIDER_IDS)
def test_repo_lifecycle(tmp_path: Path, provider_cls: type[RepoProvider]):
    repo_provider = provider_cls()
    repo = repo_provider.create(tmp_path)
    try:
        # Test 1: Set initial value and check dirty
        val1 = b'{"name": "MyApp", "version": 1}'
        repo.set("app", val1)
        assert repo.is_dirty() is True, "Repo should be dirty after set"

        # Test 2: Commit
        repo.commit()
        assert repo.is_dirty() is False, "Repo should be clean after commit"

        # Test 3: Read value
        val = repo.get("app")
        assert val == val1, "Value mismatch after commit"

        # Test 4: Modify value
        val2 = b'{"name": "MyApp", "version": 2}'
        repo.set("app", val2)
        assert repo.is_dirty() is True, "Repo should be dirty after modification"

        # Test 5: Commit again
        repo.commit()
        val = repo.get("app")
        assert val == val2, "New value mismatch after update"
    finally:
        repo_provider.cleanup(repo)


@pytest.mark.parametrize("provider_cls", PROVIDERS, ids=PROVIDER_IDS)
def test_repo_persistence(tmp_path: Path, provider_cls: type[RepoProvider]):
    repo_provider = provider_cls()
    # Setup initial state
    try:
        repo1 = repo_provider.create(tmp_path)
        repo1.set("db", b'{"host": "localhost"}')
        repo1.commit()
        repo_provider.cleanup(repo1)
    except Exception:
        pass

    # Basic cleanup robustness improvement
    repo2 = None
    try:
        # Re-open repo
        repo2 = repo_provider.create(tmp_path)
        val = repo2.get("db")
        assert val == b'{"host": "localhost"}', "Data should persist across instances"
    finally:
        if repo2:
            repo_provider.cleanup(repo2)
