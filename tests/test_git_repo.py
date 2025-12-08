from pathlib import Path
from config_plane.impl.git import GitConfigRepo


def test_git_repo_lifecycle(tmp_path: Path):
    repo_path = tmp_path / "config-repo"
    repo = GitConfigRepo(repo_path)

    # Test 1: Set initial value and check dirty
    repo.set("app", {"name": "MyApp", "version": 1})
    assert repo.stage.is_dirty() is True, "Repo should be dirty after set"

    # Test 2: Commit
    repo.commit()
    assert repo.stage.is_dirty() is False, "Repo should be clean after commit"

    # Test 3: Read value
    val = repo.get("app")
    assert val == {"name": "MyApp", "version": 1}, "Value mismatch after commit"

    # Test 4: Modify value
    repo.set("app", {"name": "MyApp", "version": 2})
    assert repo.stage.is_dirty() is True, "Repo should be dirty after modification"

    # Test 5: Commit again
    repo.commit()
    val = repo.get("app")
    assert val == {"name": "MyApp", "version": 2}, "New value mismatch after update"


def test_git_repo_persistence(tmp_path: Path):
    repo_path = tmp_path / "config-repo"

    # Setup initial state
    repo1 = GitConfigRepo(repo_path)
    repo1.set("db", {"host": "localhost"})
    repo1.commit()

    # Re-open repo
    repo2 = GitConfigRepo(repo_path)
    val = repo2.get("db")
    assert val == {"host": "localhost"}, "Data should persist across instances"


def test_git_repo_reload(tmp_path: Path):
    repo_path = tmp_path / "config-repo"
    repo = GitConfigRepo(repo_path)

    repo.set("cache", {"enabled": True})
    repo.commit()

    # Modify behind the scenes (simulating external git change?)
    # For now, just test internal reload logic if we were to support it
    # Currently reload() is called in init.
    # Let's just ensure reload works without error on clean state
    repo.reload()
    assert repo.get("cache") == {"enabled": True}
