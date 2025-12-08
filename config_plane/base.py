Blob = bytes


class ConfigSnapshot:
    """
    Immutable snapshot of configuration data at a specific point in time.
    """

    def get(self, key: str) -> Blob | None:
        raise NotImplementedError()


class ConfigStage:
    """
    Current work-in-progress configuration changes that
    are not yet committed to the base snapshot.

    Stage owns some blobs that override the base snapshot.
    These blobs can be modified in place when changed.
    """

    def get(self, key: str) -> Blob | None:
        raise NotImplementedError()

    def set(self, key: str, value: Blob | None) -> None:
        raise NotImplementedError()

    def is_dirty(self) -> bool:
        raise NotImplementedError()

    def freeze(self) -> ConfigSnapshot:
        raise NotImplementedError()


class ConfigRepo:
    """
    Repository managing configuration snapshots and stages.

    Repo is configured with a shared state of snapshots/branches/blobs and local
    state of staged changes
    """

    def get(self, key: str) -> Blob | None:
        raise NotImplementedError()

    def set(self, key: str, value: Blob | None) -> None:
        raise NotImplementedError()

    def commit(self) -> None:
        raise NotImplementedError()

    def is_dirty(self) -> bool:
        raise NotImplementedError()

    # def switch_branch_and_pull(self, branch: str) -> SnapshotDiff:
    #     raise NotImplementedError()
