from typing import Any

from config_plane.base import ConfigRepo, ConfigSnapshot, ConfigStage, Blob

import uuid
from typing import Mapping

MemoryRepoData = dict[str, dict[str, Blob]]
MemorySnapshotData = dict[str, Blob]
# We need to store snapshots for MemoryRepo to fully implement ConfigRepo
MemorySnapshots = dict[str, "MemoryConfigSnapshot"]


class MemoryConfigSnapshot(ConfigSnapshot):
    def __init__(
        self, data: MemorySnapshotData, snapshot_id: str | None = None
    ) -> None:
        self.data = data
        self.snapshot_id = snapshot_id or str(uuid.uuid4())

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("ConfigSnapshot(...)")
        else:
            p.text(f"ConfigSnapshot(id={self.snapshot_id})")

    def get(self, key: str) -> Blob | None:
        return self.data.get(key)


class MemoryConfigStage(ConfigStage):
    def __init__(self, snapshot: MemoryConfigSnapshot) -> None:
        self.snapshot = snapshot
        self.data: MemorySnapshotData = {}

    def get(self, key: str) -> Blob | None:
        if key in self.data:
            return self.data[key]
        else:
            return self.snapshot.get(key)

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        for key, value in blobs.items():
            current = self.get(key)
            if current == value:
                continue

            # We delegate actual removal to `freeze`. Here we just mark as
            # deleted by setting to None.
            self.data[key] = value

    def is_dirty(self) -> bool:
        return len(self.data) > 0

    def freeze(self) -> ConfigSnapshot:
        # Merge snapshot data and stage data (handling None as delete)
        new_data = self.snapshot.data.copy()
        for k, v in self.data.items():
            if v is None:
                new_data.pop(k, None)
            else:
                new_data[k] = v

        return MemoryConfigSnapshot(new_data)


class MemoryConfigRepo(ConfigRepo):
    def __init__(
        self, repo_data: MemoryRepoData | None = None, branch: str = "master"
    ) -> None:
        # Upgrade internal storage to support snapshots
        # self.branches: dict[str, str] (branch_name -> snapshot_id)
        # self.snapshots: dict[str, MemoryConfigSnapshot]

        self.branches: dict[str, str] = {}
        self.snapshots: dict[str, MemoryConfigSnapshot] = {}
        self.branch = branch
        self._backing_store = repo_data

        # Init with provided data as initial commits?
        if repo_data:
            for br, data in repo_data.items():
                snap = MemoryConfigSnapshot(data)
                self.snapshots[snap.snapshot_id] = snap
                self.branches[br] = snap.snapshot_id

        # Ensure current branch exists or init empty?
        if self.branch not in self.branches:
            # Create empty root snapshot
            empty = MemoryConfigSnapshot({})
            self.snapshots[empty.snapshot_id] = empty
            self.branches[self.branch] = empty.snapshot_id

        self.reload()

    def reload(self) -> None:
        endpoint = self.branches.get(self.branch)
        if endpoint:
            self.base = self.snapshots[endpoint]
        else:
            # Should not happen given init logic
            self.base = MemoryConfigSnapshot({})

        self.stage = MemoryConfigStage(self.base)

    def get(self, key: str, snapshot_id: str | None = None) -> Blob | None:
        if snapshot_id:
            snap = self.snapshots.get(snapshot_id)
            return snap.get(key) if snap else None
        return self.stage.get(key)

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        self.stage.set_many(blobs)

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        if not self.stage.is_dirty():
            return

        new_base = self.stage.freeze()
        self.snapshots[new_base.snapshot_id] = new_base
        self.branches[self.branch] = new_base.snapshot_id
        self.base = new_base
        self.stage = MemoryConfigStage(self.base)

        # Simulate persistence/push
        if self._backing_store is not None:
            self._backing_store[self.branch] = new_base.data

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")
        if branch not in self.branches:
            # Create? No, switch fails if not exists usually?
            # Git repo: switch_branch also handles creation sometimes or fails.
            # Base doc: "Switch the current working branch."
            # Implementation choice: raise if not exists?
            pass

        # If we permit switching to non-existent (creating partial?),
        # let's assume valid branches for now or auto-create empty (like git checkout -b)
        # But for generic consistency: switch implies existing. create implies new.

        # But here we handle "lazy" existence?
        # Let's say we require create_branch first.
        if branch not in self.branches:
            # Auto-create empty?
            # Let's be consistent with git impl which tries checkout logic.
            # If git checkout branch fails, it fails.
            # But here we are simple memory repo.
            # Let's allowed strict switch.
            # But default test usage might rely on "just works".
            # Let's init empty if missing?
            empty = MemoryConfigSnapshot({})
            self.snapshots[empty.snapshot_id] = empty
            self.branches[branch] = empty.snapshot_id

        self.branch = branch
        self.reload()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        if new_branch in self.branches:
            raise ValueError(f"Branch '{new_branch}' already exists")

        source = from_branch or self.branch
        if source not in self.branches:
            raise ValueError(f"Source '{source}' not found")

        snap_id = self.branches[source]
        self.branches[new_branch] = snap_id

    def list_branches(self) -> list[str]:
        return list(self.branches.keys())

    def merge(self, branch: str) -> None:
        if branch not in self.branches:
            raise ValueError(f"Branch '{branch}' does not exist")

        # Merge logic: Take other branch content, apply to stage?
        # Or proper merge commit?
        # Memory repo: Simple overlay
        other_snap_id = self.branches[branch]
        other_snap = self.snapshots[other_snap_id]

        # We need to compute diff and apply to stage?
        # or simplified: just set everything from other on stage?
        # That overwrites current stage?
        # Let's iterate and set.
        self.stage.set_many(other_snap.data)

    def get_branch_snapshot_id(self, branch: str | None = None) -> str | None:
        return self.branches.get(branch or self.branch)

    def snapshot_exists(self, snapshot_id: str) -> bool:
        return snapshot_id in self.snapshots


def create_memory_config_repo(repo: MemoryRepoData) -> MemoryConfigRepo:
    return MemoryConfigRepo(repo)
