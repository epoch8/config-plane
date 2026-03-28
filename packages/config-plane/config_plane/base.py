from typing import Mapping

Blob = bytes


class ConfigSnapshot:
    """
    Immutable snapshot of configuration data at a specific point in time.
    """

    def get(self, key: str) -> Blob | None:
        """Retrieve the content of a blob by its key."""
        raise NotImplementedError()


class ConfigStage:
    """
    Current work-in-progress configuration changes that
    are not yet committed to the base snapshot.

    Stage owns some blobs that override the base snapshot.
    These blobs can be modified in place when changed.
    """

    def get(self, key: str) -> Blob | None:
        """
        Retrieve the content of a blob by its key, checking staged changes
        first.
        """
        raise NotImplementedError()

    def set(self, key: str, value: Blob | None) -> None:
        """
        Update or create a blob in the stage. If the value is the same as the
        base snapshot, does nothing. Pass None to mark as deleted.
        """
        self.set_many({key: value})

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        """
        Update multiple blobs in the stage. If the value is the same as the base
        snapshot, does nothing. Pass None to mark as deleted.
        """
        raise NotImplementedError()

    def is_dirty(self) -> bool:
        """
        Check if there are any staged changes.
        """
        raise NotImplementedError()

    def freeze(self) -> ConfigSnapshot:
        """
        Create a new immutable snapshot from the current stage.
        """
        raise NotImplementedError()


class ConfigRepo:
    """
    Repository managing configuration snapshots and stages.

    Repo is configured with a shared state of snapshots/branches/blobs and local
    state of staged changes
    """

    def get(self, key: str, snapshot_id: str | None = None) -> Blob | None:
        """
        Retrieve the content of a blob.
        If snapshot_id is provided, fetches from that specific snapshot.
        Otherwise, fetches from the current stage.
        """
        raise NotImplementedError()

    def set(self, key: str, value: Blob | None) -> None:
        """
        Update or create a blob in the stage. If the value is the same as the
        base snapshot, does nothing. Pass None to mark as deleted.
        """
        self.set_many({key: value})

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        """
        Update multiple blobs in the stage. If the value is the same as the base
        snapshot, does nothing. Pass None to mark as deleted.
        """
        raise NotImplementedError()

    def commit(self) -> None:
        """
        Commit the current stage to the repository history.
        """
        raise NotImplementedError()

    def is_dirty(self) -> bool:
        """
        Check if the current stage has uncommitted changes.
        """
        raise NotImplementedError()

    def switch_branch(self, branch: str) -> None:
        """
        Switch the current working branch.
        """
        raise NotImplementedError()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        """
        Create a new branch, optionally starting from an existing one.
        """
        raise NotImplementedError()

    def list_branches(self) -> list[str]:
        """
        List all available branches in the repository.
        """
        raise NotImplementedError()

    def merge(self, branch: str) -> None:
        """
        Merge another branch into the current branch.
        """
        raise NotImplementedError()

    def get_branch_snapshot_id(self, branch: str | None = None) -> str | None:
        """
        Return snapshot id for branch head, if any. If branch is None, returns
        current branch head.
        """
        raise NotImplementedError()

    def set_branch_snapshot_id(
        self, snapshot_id: str, branch: str | None = None
    ) -> None:
        """
        Force branch head to specific snapshot id. If branch is None, updates
        current branch. Warning: This is a forceful operation (reset).
        """
        raise NotImplementedError()
