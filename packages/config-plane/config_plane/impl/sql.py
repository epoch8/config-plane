"""
Sql implementation of ConfigRepo.

This implementation assumes that snapshots are FULL snapshots, not incremental.
Each snapshot contains references to all keys present in that snapshot;
traversing the parent tree is not required to reconstruct the state.
"""

from typing import Callable, Any, Mapping


from sqlalchemy import (
    LargeBinary,
    ForeignKey,
    select,
    insert,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, Mapped, mapped_column

from config_plane.base import ConfigRepo, ConfigSnapshot, ConfigStage, Blob


class Base(DeclarativeBase):
    pass


class BlobModel(Base):
    __tablename__ = "blobs"
    id: Mapped[int] = mapped_column(primary_key=True)
    content: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class SnapshotModel(Base):
    __tablename__ = "snapshots"
    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("snapshots.id"), nullable=True
    )
    committed: Mapped[bool] = mapped_column(default=False)


class SnapshotItemModel(Base):
    __tablename__ = "snapshot_items"
    snapshot_id: Mapped[int] = mapped_column(
        ForeignKey("snapshots.id"), primary_key=True
    )
    key: Mapped[str] = mapped_column(primary_key=True)
    blob_id: Mapped[int | None] = mapped_column(ForeignKey("blobs.id"), nullable=True)

    blob: Mapped[BlobModel] = relationship(BlobModel)


class BranchModel(Base):
    __tablename__ = "branches"
    name: Mapped[str] = mapped_column(primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id"))


class SqlConfigSnapshot(ConfigSnapshot):
    def __init__(self, session_maker: Callable[[], Session], snapshot_id: int) -> None:
        self.session_maker = session_maker
        self.snapshot_id = snapshot_id

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("SqlConfigSnapshot(...)")
        else:
            with p.group(4, "SqlConfigSnapshot(", ")"):
                p.breakable()
                p.text(f"id={self.snapshot_id},")
                p.breakable()

    def get(self, key: str) -> Blob | None:
        with self.session_maker() as session:
            stmt = select(SnapshotItemModel).where(
                SnapshotItemModel.snapshot_id == self.snapshot_id,
                SnapshotItemModel.key == key,
            )
            item = session.execute(stmt).scalar_one_or_none()
            if item is None:
                return None
            if item.blob_id is None:
                return None

            # We need to fetch the blob content.
            # Since we have the relationship accessed, we might need it eagerly loaded or just query it.
            # But item.blob is Mapped[BlobModel], so accessing it lazily should work if session is active.
            # However, to be safe and explicit:
            if item.blob:
                return item.blob.content

            # Fallback if relation not loaded but id is present
            blob_stmt = select(BlobModel).where(BlobModel.id == item.blob_id)
            blob = session.execute(blob_stmt).scalar_one_or_none()
            return blob.content if blob else None


class SqlConfigStage(ConfigStage):
    def __init__(
        self,
        session_maker: Callable[[], Session],
        parent_snapshot: SqlConfigSnapshot | None,
        stage_snapshot_id: int,
    ) -> None:
        self.session_maker = session_maker
        self.parent = parent_snapshot
        self.snapshot_id = stage_snapshot_id
        self.merge_parent_id: int | None = None

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("SqlConfigStage(...)")
        else:
            with p.group(4, "SqlConfigStage(", ")"):
                p.breakable()
                p.text(f"snapshot_id={self.snapshot_id},")
                p.breakable()
                p.text("parent=")
                p.pretty(self.parent)
                p.breakable()

    def get(self, key: str) -> Blob | None:
        with self.session_maker() as session:
            # Check current sparse snapshot first
            stmt = select(SnapshotItemModel).where(
                SnapshotItemModel.snapshot_id == self.snapshot_id,
                SnapshotItemModel.key == key,
            )
            item = session.execute(stmt).scalar_one_or_none()

            if item is not None:
                # Explicitly set in this stage
                if item.blob_id is None:
                    return None  # Deleted

                if item.blob:
                    return item.blob.content

                blob_stmt = select(BlobModel).where(BlobModel.id == item.blob_id)
                blob = session.execute(blob_stmt).scalar_one_or_none()
                return blob.content if blob else None

            # Not found in stage, check parent
            if self.parent:
                return self.parent.get(key)

            return None

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        with self.session_maker() as session:
            # Optimize: check existing values to avoid dirtying stage if unchanged
            # We can use self.get(key) but that might be slow in loop (fetches one by one)
            # Better: fetch all keys in one query?
            # For now, let's just use self.get() logic or simple optimization:

            # Fetch current values for all keys in blobs
            # If changed, apply update.

            keys = list(blobs.keys())

            # This is complex to do in bulk optimally without duplicating get logic.
            # Let's iterate and use self.get() for correctness first, unless perf is critical.
            # But we are inside a `set_many`, implementation should be somewhat efficient.

            # To avoid N queries, we can try to fetch all relevant items from stage.
            stage_items_stmt = select(SnapshotItemModel).where(
                SnapshotItemModel.snapshot_id == self.snapshot_id,
                SnapshotItemModel.key.in_(keys),
            )
            stage_items = {
                item.key: item
                for item in session.execute(stage_items_stmt).scalars().all()
            }

            # For items not in stage, we need to check parent.
            # Optimization: If we just write to stage, we mark it dirty.
            # Requirement: "check for changes before applying to avoid dirtiness"

            for key, value in blobs.items():
                current_value = self.get(
                    key
                )  # This uses existing logic (stage -> parent)

                if current_value == value:
                    continue

                # Needs update
                item = stage_items.get(key)
                if item:
                    # Update existing item in stage
                    if value is None:
                        item.blob_id = None
                    else:
                        if item.blob_id is not None:
                            # Update blob content?
                            # Blobs are immutable concept?
                            # No, implementation of set says "Update existing blob in place"
                            # But wait, if we share blobs, we shouldn't update in place?
                            # Definition: "Stage owns some blobs... These blobs can be modified in place"
                            blob_stmt = select(BlobModel).where(
                                BlobModel.id == item.blob_id
                            )
                            blob = session.execute(blob_stmt).scalar_one_or_none()
                            if blob:
                                blob.content = value
                            else:
                                new_blob = BlobModel(content=value)
                                session.add(new_blob)
                                session.flush()
                                item.blob_id = new_blob.id
                        else:
                            new_blob = BlobModel(content=value)
                            session.add(new_blob)
                            session.flush()
                            item.blob_id = new_blob.id
                else:
                    # Create new item in stage
                    blob_id = None
                    if value is not None:
                        new_blob = BlobModel(content=value)
                        session.add(new_blob)
                        session.flush()
                        blob_id = new_blob.id

                    new_item = SnapshotItemModel(
                        snapshot_id=self.snapshot_id, key=key, blob_id=blob_id
                    )
                    session.add(new_item)
                    # Add to local cache if we were looping (but we re-query or use separate logic)
                    # Here we just add to session

            session.flush()
            session.commit()

    def is_dirty(self) -> bool:
        with self.session_maker() as session:
            # Check if any items exist in the sparse snapshot
            stmt = select(SnapshotItemModel).where(
                SnapshotItemModel.snapshot_id == self.snapshot_id
            )
            result = session.execute(stmt).first()
            return result is not None

    def freeze(self) -> ConfigSnapshot:
        # This implementation of freeze is slightly different than memory one because
        # we are not just returning a snapshot, but "committing" logic happens in Repo.commit().
        # However, ConfigStage.freeze() implies returning a snapshot that represents the current stage state.
        # But this stage is MUTABLE.
        # If we need a frozen snapshot, we would technically need to commit or fork?
        # The base interface says `freeze() -> ConfigSnapshot`.
        # For now, let's treat the current stage view as a snapshot read.
        return SqlConfigSnapshot(self.session_maker, self.snapshot_id)

    def _finalize_commit(self, session: Session) -> None:
        """Helper to fill in gaps from parent before marking committed."""
        if self.parent:
            # Copy items from parent that are NOT in current snapshot
            # Insert into snapshot_items (snapshot_id, key, blob_id)
            # Select key, blob_id from snapshot_items where snapshot_id = parent_id
            # AND key NOT IN (select key from snapshot_items where snapshot_id = current_id)

            parent_items_stmt = (
                select(SnapshotItemModel.key, SnapshotItemModel.blob_id)
                .where(SnapshotItemModel.snapshot_id == self.parent.snapshot_id)
                .where(
                    SnapshotItemModel.key.not_in(
                        select(SnapshotItemModel.key).where(
                            SnapshotItemModel.snapshot_id == self.snapshot_id
                        )
                    )
                )
            )

            # Using bulk insert via connection/core if possible or manual
            # To be DB-agnostic and safe within ORM session:
            rows_to_insert = session.execute(parent_items_stmt).all()
            if rows_to_insert:
                session.execute(
                    insert(SnapshotItemModel),
                    [
                        {
                            "snapshot_id": self.snapshot_id,
                            "key": row.key,
                            "blob_id": row.blob_id,
                        }
                        for row in rows_to_insert
                    ],
                )

        # Mark as committed
        snap = session.execute(
            select(SnapshotModel).where(SnapshotModel.id == self.snapshot_id)
        ).scalar_one()
        snap.committed = True
        session.flush()


class SqlConfigRepo(ConfigRepo):
    def __init__(
        self,
        session_maker: Callable[[], Session],
        stage_snapshot_id: int | None = None,
        branch: str = "master",
    ) -> None:
        self.session_maker = session_maker
        self.branch = branch
        self.parent_snapshot: SqlConfigSnapshot | None = None

        with self.session_maker() as session:
            if stage_snapshot_id:
                # Resuming
                self.stage_snapshot_id = stage_snapshot_id
                # Determine parent from the snapshot
                snap = session.execute(
                    select(SnapshotModel).where(SnapshotModel.id == stage_snapshot_id)
                ).scalar_one()
                if snap.committed:
                    raise ValueError("Cannot resume a committed snapshot as stage")

                parent_id = snap.parent_id
                self.parent_snapshot = (
                    SqlConfigSnapshot(session_maker, parent_id) if parent_id else None
                )
            else:
                self._init_stage_from_branch(session)

            session.commit()

        self._refresh_stage_object()

    def _init_stage_from_branch(self, session: Session) -> None:
        # Try to get branch
        branch_model = session.execute(
            select(BranchModel).where(BranchModel.name == self.branch)
        ).scalar_one_or_none()

        parent_id = None
        if branch_model:
            parent_id = branch_model.snapshot_id
            self.parent_snapshot = SqlConfigSnapshot(self.session_maker, parent_id)
        else:
            self.parent_snapshot = None

        # Create new ephemeral snapshot
        new_snap = SnapshotModel(parent_id=parent_id, committed=False)
        session.add(new_snap)
        session.flush()
        self.stage_snapshot_id = new_snap.id

    def _refresh_stage_object(self) -> None:
        self.stage = SqlConfigStage(
            self.session_maker, self.parent_snapshot, self.stage_snapshot_id
        )

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("SqlConfigRepo(...)")
        else:
            with p.group(4, "SqlConfigRepo(", ")"):
                p.breakable()
                p.text(f"branch='{self.branch}',")
                p.breakable()
                p.text("stage=")
                p.pretty(self.stage)
                p.breakable()

    def get(self, key: str, snapshot_id: str | None = None) -> Blob | None:
        if snapshot_id is not None:
            return self.get_snapshot_blob(int(snapshot_id), key)
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def set_many(self, blobs: Mapping[str, Blob | None]) -> None:
        self.stage.set_many(blobs)

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        if not self.is_dirty():
            return

        with self.session_maker() as session:
            # Finalize the current stage (snapshot)
            self.stage._finalize_commit(session)

            # Update branch pointer
            branch_model = session.execute(
                select(BranchModel).where(BranchModel.name == self.branch)
            ).scalar_one_or_none()

            new_snap_id = self.stage.snapshot_id

            if branch_model:
                branch_model.snapshot_id = new_snap_id
            else:
                branch_model = BranchModel(name=self.branch, snapshot_id=new_snap_id)
                session.add(branch_model)

            session.flush()

            # Reset stage: Create new ephemeral snapshot on top of new commit
            self.parent_snapshot = SqlConfigSnapshot(self.session_maker, new_snap_id)
            new_stage_snap = SnapshotModel(parent_id=new_snap_id, committed=False)
            session.add(new_stage_snap)
            session.flush()
            self.stage_snapshot_id = new_stage_snap.id

            session.commit()

        self._refresh_stage_object()

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")

        with self.session_maker() as session:
            branch_exists = session.execute(
                select(BranchModel).where(BranchModel.name == branch)
            ).scalar_one_or_none()

            if not branch_exists:
                # Auto-create empty if not exists (matching MemoryRepo behavior for robustness)
                # Create empty root snapshot
                empty_snap = SnapshotModel(parent_id=None, committed=True)
                session.add(empty_snap)
                session.flush()
                new_branch_model = BranchModel(name=branch, snapshot_id=empty_snap.id)
                session.add(new_branch_model)
                session.commit()

        self.branch = branch
        with self.session_maker() as session:
            self._init_stage_from_branch(session)
            session.commit()
        self._refresh_stage_object()

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        with self.session_maker() as session:
            # Check if new branch exists
            exists = session.execute(
                select(BranchModel).where(BranchModel.name == new_branch)
            ).scalar_one_or_none()
            if exists:
                raise ValueError(f"Branch '{new_branch}' already exists")

            start_point = from_branch or self.branch

            # Get source snapshot ID
            source = session.execute(
                select(BranchModel).where(BranchModel.name == start_point)
            ).scalar_one_or_none()

            if not source:
                raise ValueError(f"Source '{start_point}' not found")

            # Create branch pointing to same snapshot
            new_branch_model = BranchModel(
                name=new_branch, snapshot_id=source.snapshot_id
            )
            session.add(new_branch_model)
            session.commit()

    def list_branches(self) -> list[str]:
        with self.session_maker() as session:
            branches = session.execute(select(BranchModel.name)).scalars().all()
            return list(branches)

    def merge(self, branch: str) -> None:
        # Simple overlay merge: apply all keys from other branch to current stage
        with self.session_maker() as session:
            other_branch = session.execute(
                select(BranchModel).where(BranchModel.name == branch)
            ).scalar_one_or_none()

            if not other_branch:
                raise ValueError(f"Branch '{branch}' does not exist")

            other_snap_id = other_branch.snapshot_id

            all_data = self._dump_snapshot(session, other_snap_id)
            self.stage.set_many(all_data)

    def _dump_snapshot(self, session: Session, snapshot_id: int) -> dict[str, Blob]:
        """Get all kv pairs in a snapshot.

        Since snapshots are full (not incremental), we only need to query the specific snapshot_id.
        """
        result = {}
        # Use simple query with the provided session
        items = (
            session.execute(
                select(SnapshotItemModel).where(
                    SnapshotItemModel.snapshot_id == snapshot_id
                )
            )
            .scalars()
            .all()
        )

        for item in items:
            if item.blob_id is not None:
                if item.blob:
                    content = item.blob.content
                else:
                    content = session.execute(
                        select(BlobModel.content).where(BlobModel.id == item.blob_id)
                    ).scalar_one()
                result[item.key] = content

        return result

    def get_branch_snapshot_id(self, branch: str | None = None) -> str | None:
        """Return snapshot id for branch head, if any"""
        with self.session_maker() as session:
            branch_model = session.execute(
                select(BranchModel).where(BranchModel.name == (branch or self.branch))
            ).scalar_one_or_none()
            return str(branch_model.snapshot_id) if branch_model else None

    def get_snapshot_blob(self, snapshot_id: int, key: str) -> Blob | None:
        """Fetch a blob payload by snapshot id + key"""
        with self.session_maker() as session:
            item = session.execute(
                select(SnapshotItemModel).where(
                    SnapshotItemModel.snapshot_id == snapshot_id,
                    SnapshotItemModel.key == key,
                )
            ).scalar_one_or_none()
            if item is None or item.blob_id is None:
                return None

            if item.blob:
                return item.blob.content

            blob = session.execute(
                select(BlobModel).where(BlobModel.id == item.blob_id)
            ).scalar_one_or_none()
            return blob.content if blob else None

    def snapshot_exists(self, snapshot_id: str) -> bool:
        """Return True if snapshot id exists"""
        try:
            snap_id_int = int(snapshot_id)
        except ValueError:
            return False

        with self.session_maker() as session:
            found = session.execute(
                select(SnapshotModel.id).where(SnapshotModel.id == snap_id_int)
            ).scalar_one_or_none()
            return found is not None


def create_sql_config_repo(
    session_maker: Callable[[], Session],
    stage_snapshot_id: int | None = None,
    branch: str = "master",
) -> SqlConfigRepo:
    return SqlConfigRepo(session_maker, stage_snapshot_id, branch=branch)
