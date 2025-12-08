from typing import Any

from config_plane.base import ConfigRepo, ConfigSnapshot, ConfigStage, Blob

MemoryRepoData = dict[str, dict[str, Any]]
MemorySnapshotData = dict[str, Blob]


class MemoryConfigSnapshot(ConfigSnapshot):
    def __init__(self, data: MemorySnapshotData) -> None:
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


class MemoryConfigStage(ConfigStage):
    def __init__(self, snapshot: MemoryConfigSnapshot) -> None:
        self.snapshot = snapshot
        self.data: MemorySnapshotData = {}

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
        return MemoryConfigSnapshot(
            {
                **self.snapshot.data,
                **self.data,
            }
        )


class MemoryConfigRepo(ConfigRepo):
    def __init__(self, repo_data: MemoryRepoData) -> None:
        self.repo = repo_data

        self.base = MemoryConfigSnapshot(self.repo.get("master", {}))
        self.stage = MemoryConfigStage(self.base)

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

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        if not self.stage.is_dirty():
            return

        new_base = self.stage.freeze()
        assert isinstance(new_base, MemoryConfigSnapshot)
        self.repo["master"] = new_base.data
        self.base = new_base

        self.stage = MemoryConfigStage(self.base)


def create_memory_config_repo(repo: MemoryRepoData) -> ConfigRepo:
    return MemoryConfigRepo(repo)
