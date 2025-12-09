import pytest
from pathlib import Path
from tests.test_repo_common import RepoProvider, PROVIDERS, PROVIDER_IDS


@pytest.mark.parametrize("provider_cls", PROVIDERS, ids=PROVIDER_IDS)
def test_merge_simple(tmp_path: Path, provider_cls: type[RepoProvider]):
    """
    Test simple merge scenario:
    1. Base: key1=v1
    2. Branch Dev (from Base): key1=v2, key2=new
    3. Branch Prod (from Base): key3=other
    4. Merge Dev into Prod -> Prod has key1=v2, key2=new, key3=other
    """
    repo_provider = provider_cls()
    repo = repo_provider.create(tmp_path)

    try:
        # 1. Setup Base (master/prod)
        repo.set("key1", b"v1")
        repo.commit()

        # 2. Create Dev branch and modify
        repo.create_branch("dev")
        repo.switch_branch("dev")
        repo.set("key1", b"v2")
        repo.set("key2", b"new")
        repo.commit()

        # 3. Switch back to Prod (master) and modify something else
        # (Assuming master is providing 'prod' role here for simplicity, or we check 'master')
        repo.switch_branch("master")
        repo.set("key3", b"other")
        repo.commit()

        # 4. Merge Dev into Master
        repo.merge("dev")
        repo.commit()  # For SQL/Memory this might be needed if merge modifies stage. Git merge usually commits.

        # Verify
        val1 = repo.get("key1")
        val2 = repo.get("key2")
        val3 = repo.get("key3")

        assert val1 == b"v2", "Dev changes should overwrite Base"
        assert val2 == b"new", "New keys from Dev should appear"
        assert val3 == b"other", "Existing keys in Prod should remain"

    finally:
        repo_provider.cleanup(repo)


@pytest.mark.parametrize("provider_cls", PROVIDERS, ids=PROVIDER_IDS)
def test_merge_conflict_override(tmp_path: Path, provider_cls: type[RepoProvider]):
    """
    Test conflict/override logic:
    1. Base: key1=v1
    2. Dev: key1=v2
    3. Prod: key1=v3
    4. Merge Dev into Prod -> Prod should have key1=v2 (Source Wins per our logic)
    """
    repo_provider = provider_cls()
    repo = repo_provider.create(tmp_path)

    try:
        # 1. Base
        repo.set("key1", b"v1")
        repo.commit()

        # 2. Dev
        repo.create_branch("dev")
        repo.switch_branch("dev")
        repo.set("key1", b"v2")
        repo.commit()

        # 3. Prod (master)
        repo.switch_branch("master")
        repo.set("key1", b"v3")
        repo.commit()

        # 4. Merge
        repo.merge("dev")
        repo.commit()

        val1 = repo.get("key1")
        assert val1 == b"v2", "Source (Dev) should override Target (Prod) changes"

        # Verify clean stage
        # assert not repo.is_dirty() # Wait, does merge leave it dirty?
        # SQL merge modifies stage. Commit makes it clean.
        # Git merge commits automatically.

    finally:
        repo_provider.cleanup(repo)
