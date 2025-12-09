from typing import Any

from config_plane.base import ConfigRepo, ConfigSnapshot, ConfigStage, Blob

MemoryRepoData = dict[str, dict[str, Blob]]
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

    def get(self, key: str) -> Blob | None:
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
    def __init__(self, repo_data: MemoryRepoData, branch: str = "master") -> None:
        self.repo = repo_data
        self.branch = branch

        self.reload()

    def reload(self) -> None:
        self.base = MemoryConfigSnapshot(self.repo.get(self.branch, {}))
        self.stage = MemoryConfigStage(self.base)

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigRepo(...)")
        else:
            with p.group(4, "ConfigRepo(", ")"):
                p.breakable()
                p.text(f"branch='{self.branch}',")
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
        self.repo[self.branch] = new_base.data
        self.base = new_base

        self.stage = MemoryConfigStage(self.base)

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")
        self.branch = branch
        self.reload()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        if new_branch in self.repo:
            raise ValueError(f"Branch '{new_branch}' already exists")

        source = from_branch or self.branch
        if source not in self.repo and source != "master":
            # If source is master and empty, it defaults to empty dict in reload,
            # but here strict check might be better.
            # However, existing code assumes lazy creation of master in some sense?
            # Actually repo_data is passed in.
            pass

        data = self.repo.get(source, {})
        # Deep copy needed? Blobs are bytes (immutable), dict shallow copy is enough
        self.repo[new_branch] = data.copy()

    def list_branches(self) -> list[str]:
        return list(self.repo.keys())


def create_memory_config_repo(repo: MemoryRepoData) -> MemoryConfigRepo:
    return MemoryConfigRepo(repo)
