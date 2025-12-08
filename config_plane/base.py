from typing import Any

Blob = dict
SnapshotData = dict[str, Blob]


class ConfigSnapshot:
    """
    Immutable snapshot of configuration data at a specific point in time.
    """

    def __init__(self, data: SnapshotData) -> None:
        self.data = data

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigSnapshot(...)")
        else:
            with p.group(4, "ConfigSnapshot(", ")"):
                p.breakable()
                p.text(f"data={self.data},")
                p.breakable()

    def get(self, key: str) -> dict | None:
        return self.data.get(key)


class ConfigStage:
    """
    Current work-in-progress configuration changes that
    are not yet committed to the base snapshot.

    Stage owns some blobs that override the base snapshot.
    These blobs can be modified in place when changed.
    """

    def __init__(self, snapshot: ConfigSnapshot) -> None:
        self.snapshot = snapshot
        self.data: SnapshotData = {}

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigStage(...)")
        else:
            with p.group(4, "ConfigStage(", ")"):
                p.breakable()
                p.text("snapshot=")
                p.pretty(self.snapshot)
                p.text(",")
                p.breakable()
                p.text("data=")
                p.pretty(self.data)
                p.breakable()

    def get(self, key: str) -> Blob | None:
        if key in self.data:
            return self.data[key]
        else:
            return self.snapshot.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        if value is None:
            if key in self.data:
                del self.data[key]
        else:
            self.data[key] = value

    def is_dirty(self) -> bool:
        return len(self.data) > 0

    def freeze(self) -> ConfigSnapshot:
        return ConfigSnapshot(
            {
                **self.snapshot.data,
                **self.data,
            }
        )


class ConfigRepo:
    """
    Repository managing configuration snapshots and stages.

    Repo is configured with a shared state of snapshots/branches/blobs and local
    state of staged changes
    """

    @classmethod
    def create(cls) -> "ConfigRepo":
        return cls()

    def __init__(self) -> None:
        self.repo: dict[str, ConfigSnapshot] = {
            "master": ConfigSnapshot({}),
        }

        self.base = self.repo["master"]
        self.stage = ConfigStage(self.base)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigRepo(...)")
        else:
            with p.group(4, "ConfigRepo(", ")"):
                p.breakable()
                p.text("base=")
                p.pretty(self.base)
                p.text(",")
                p.breakable()
                p.text("stage=")
                p.pretty(self.stage)
                p.text(",")
                p.breakable()

    def get(self, key: str) -> Blob | None:
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def commit(self) -> None:
        if not self.stage.is_dirty():
            return

        new_base = self.stage.freeze()
        self.repo["master"] = new_base
        self.base = new_base

        self.stage = ConfigStage(self.base)

    # def switch_branch_and_pull(self, branch: str) -> SnapshotDiff:
    #     raise NotImplementedError()

