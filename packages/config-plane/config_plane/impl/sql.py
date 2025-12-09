from typing import Callable, Any


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

    def set(self, key: str, value: Blob | None) -> None:
        with self.session_maker() as session:
            stmt = select(SnapshotItemModel).where(
                SnapshotItemModel.snapshot_id == self.snapshot_id,
                SnapshotItemModel.key == key,
            )
            item = session.execute(stmt).scalar_one_or_none()

            if item:
                # Item exists in stage
                if value is None:
                    item.blob_id = None
                else:
                    # Update existing blob in place
                    if item.blob_id is not None:
                        blob_stmt = select(BlobModel).where(
                            BlobModel.id == item.blob_id
                        )
                        blob = session.execute(blob_stmt).scalar_one_or_none()
                        if blob:
                            blob.content = value
                        else:
                            # Should not happen ideally
                            new_blob = BlobModel(content=value)
                            session.add(new_blob)
                            session.flush()
                            item.blob_id = new_blob.id
                    else:
                        # Was deleted, now setting value -> create new blob
                        new_blob = BlobModel(content=value)
                        session.add(new_blob)
                        session.flush()
                        item.blob_id = new_blob.id
            else:
                # Item missing in stage, create new entry
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

        self.stage = SqlConfigStage(
            self.session_maker, self.parent_snapshot, self.stage_snapshot_id
        )

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

    def get(self, key: str) -> Blob | None:
        return self.stage.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self.stage.set(key, value)

    def is_dirty(self) -> bool:
        return self.stage.is_dirty()

    def commit(self) -> None:
        with self.session_maker() as session:
            # Finalize the stage
            self.stage._finalize_commit(session)

            # Update branch
            branch_model = session.execute(
                select(BranchModel).where(BranchModel.name == self.branch)
            ).scalar_one_or_none()

            if branch_model:
                branch_model.snapshot_id = self.stage_snapshot_id
            else:
                branch_model = BranchModel(
                    name=self.branch, snapshot_id=self.stage_snapshot_id
                )
            session.add(branch_model)

            # Start new stage from this new commit
            parent_id = self.stage_snapshot_id
            self.parent_snapshot = SqlConfigSnapshot(self.session_maker, parent_id)

            new_snap = SnapshotModel(parent_id=parent_id, committed=False)
            session.add(new_snap)
            session.flush()
            self.stage_snapshot_id = new_snap.id

            self.stage = SqlConfigStage(
                self.session_maker, self.parent_snapshot, self.stage_snapshot_id
            )

            session.commit()

    def switch_branch(self, branch: str) -> None:
        if self.is_dirty():
            raise RuntimeError("Cannot switch branch with dirty stage")

        self.branch = branch

        with self.session_maker() as session:
            self._init_stage_from_branch(session)
            session.commit()

        self.stage = SqlConfigStage(
            self.session_maker, self.parent_snapshot, self.stage_snapshot_id
        )

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        with self.session_maker() as session:
            # Check if already exists
            existing = session.execute(
                select(BranchModel).where(BranchModel.name == new_branch)
            ).scalar_one_or_none()
            if existing:
                raise ValueError(f"Branch '{new_branch}' already exists")

            source_name = from_branch or self.branch
            source = session.execute(
                select(BranchModel).where(BranchModel.name == source_name)
            ).scalar_one_or_none()

            snapshot_id = source.snapshot_id if source else None

            if snapshot_id is None and source_name != "master":
                # If source doesn't exist AND it's not master (which might be implicit empty)
                # But here we only create branch if we persist it?
                # Actually, we can create a branch pointing to nothing?
                # No, branch points to a snapshot.
                # If source is empty (no master yet), we can't really branch from it unless we point to NULL?
                # SnapshotModel parent_id can be null.
                # But BranchModel snapshot_id is not nullable in definition above?
                # `snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id"))` -> NOT NULL by default in Mapped[int]
                pass

            if snapshot_id is None:
                # If master doesn't exist, we can't easily branch off it unless we treat it as empty root?
                # But we need a snapshot ID.
                # If the repo is empty, we must create a root snapshot first?
                # Or we can't create branch until 1st commit?
                # Let's say we can't create branch if source doesn't exist.
                if source_name == "master":
                    # Allow creating from empty master?
                    # We need a dummy empty snapshot committed?
                    # For now, let's assume one must commit to master first.
                    raise ValueError(f"Source branch '{source_name}' does not exist")
                else:
                    raise ValueError(f"Source branch '{source_name}' does not exist")

            new_branch_model = BranchModel(name=new_branch, snapshot_id=snapshot_id)
            session.add(new_branch_model)
            session.commit()

    def list_branches(self) -> list[str]:
        stmt = select(BranchModel.name)
        with self.session_maker() as session:
            return list(session.execute(stmt).scalars().all())

    def reload(self) -> None:
        with self.session_maker() as session:
            """Reload the repository state from the storage."""
            # Refresh branch pointer
            branch_model = session.execute(
                select(BranchModel).where(BranchModel.name == self.branch)
            ).scalar_one_or_none()

            parent_id = None
            if branch_model:
                parent_id = branch_model.snapshot_id
                # Optimization: could check if self.parent_snapshot.id == parent_id
                # But creating SqlConfigSnapshot is cheap.
                self.parent_snapshot = SqlConfigSnapshot(self.session_maker, parent_id)
            else:
                self.parent_snapshot = None

            # Update stage parent
            self.stage.parent = self.parent_snapshot


def create_sql_config_repo(
    session_maker: Callable[[], Session],
    stage_snapshot_id: int | None = None,
    branch: str = "master",
) -> SqlConfigRepo:
    return SqlConfigRepo(session_maker, stage_snapshot_id, branch=branch)
