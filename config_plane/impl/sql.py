from typing import Callable, Any


from sqlalchemy import (
    JSON,
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
    content: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


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
    def __init__(self, session: Session, snapshot_id: int) -> None:
        self.session = session
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
        stmt = select(SnapshotItemModel).where(
            SnapshotItemModel.snapshot_id == self.snapshot_id,
            SnapshotItemModel.key == key,
        )
        item = self.session.execute(stmt).scalar_one_or_none()
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
        blob = self.session.execute(blob_stmt).scalar_one_or_none()
        return blob.content if blob else None


class SqlConfigStage(ConfigStage):
    def __init__(
        self,
        session: Session,
        parent_snapshot: SqlConfigSnapshot | None,
        stage_snapshot_id: int,
    ) -> None:
        self.session = session
        self.parent = parent_snapshot
        self.snapshot_id = stage_snapshot_id

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
        # Check current sparse snapshot first
        stmt = select(SnapshotItemModel).where(
            SnapshotItemModel.snapshot_id == self.snapshot_id,
            SnapshotItemModel.key == key,
        )
        item = self.session.execute(stmt).scalar_one_or_none()

        if item is not None:
            # Explicitly set in this stage
            if item.blob_id is None:
                return None  # Deleted

            if item.blob:
                return item.blob.content

            blob_stmt = select(BlobModel).where(BlobModel.id == item.blob_id)
            blob = self.session.execute(blob_stmt).scalar_one_or_none()
            return blob.content if blob else None

        # Not found in stage, check parent
        if self.parent:
            return self.parent.get(key)

        return None

    def set(self, key: str, value: Blob | None) -> None:
        stmt = select(SnapshotItemModel).where(
            SnapshotItemModel.snapshot_id == self.snapshot_id,
            SnapshotItemModel.key == key,
        )
        item = self.session.execute(stmt).scalar_one_or_none()

        if item:
            # Item exists in stage
            if value is None:
                item.blob_id = None
            else:
                # Update existing blob in place
                if item.blob_id is not None:
                    blob_stmt = select(BlobModel).where(BlobModel.id == item.blob_id)
                    blob = self.session.execute(blob_stmt).scalar_one_or_none()
                    if blob:
                        blob.content = value
                    else:
                        # Should not happen ideally
                        new_blob = BlobModel(content=value)
                        self.session.add(new_blob)
                        self.session.flush()
                        item.blob_id = new_blob.id
                else:
                    # Was deleted, now setting value -> create new blob
                    new_blob = BlobModel(content=value)
                    self.session.add(new_blob)
                    self.session.flush()
                    item.blob_id = new_blob.id
        else:
            # Item missing in stage, create new entry
            blob_id = None
            if value is not None:
                new_blob = BlobModel(content=value)
                self.session.add(new_blob)
                self.session.flush()
                blob_id = new_blob.id

            new_item = SnapshotItemModel(
                snapshot_id=self.snapshot_id, key=key, blob_id=blob_id
            )
            self.session.add(new_item)

        self.session.flush()

    def is_dirty(self) -> bool:
        # Check if any items exist in the sparse snapshot
        stmt = select(SnapshotItemModel).where(
            SnapshotItemModel.snapshot_id == self.snapshot_id
        )
        result = self.session.execute(stmt).first()
        return result is not None

    def freeze(self) -> ConfigSnapshot:
        # This implementation of freeze is slightly different than memory one because
        # we are not just returning a snapshot, but "committing" logic happens in Repo.commit().
        # However, ConfigStage.freeze() implies returning a snapshot that represents the current stage state.
        # But this stage is MUTABLE.
        # If we need a frozen snapshot, we would technically need to commit or fork?
        # The base interface says `freeze() -> ConfigSnapshot`.
        # For now, let's treat the current stage view as a snapshot read.
        return SqlConfigSnapshot(self.session, self.snapshot_id)

    def _finalize_commit(self) -> None:
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
            rows_to_insert = self.session.execute(parent_items_stmt).all()
            if rows_to_insert:
                self.session.execute(
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
        snap = self.session.execute(
            select(SnapshotModel).where(SnapshotModel.id == self.snapshot_id)
        ).scalar_one()
        snap.committed = True
        self.session.flush()


class SqlConfigRepo(ConfigRepo):
    def __init__(
        self, session_maker: Callable[[], Session], stage_snapshot_id: int | None = None
    ) -> None:
        self.session_maker = session_maker
        self.session = session_maker()

        if stage_snapshot_id:
            # Resuming
            self.stage_snapshot_id = stage_snapshot_id
            # Determine parent from the snapshot
            snap = self.session.execute(
                select(SnapshotModel).where(SnapshotModel.id == stage_snapshot_id)
            ).scalar_one()
            if snap.committed:
                raise ValueError("Cannot resume a committed snapshot as stage")

            parent_id = snap.parent_id
            self.parent_snapshot = (
                SqlConfigSnapshot(self.session, parent_id) if parent_id else None
            )
        else:
            # Start new
            # Try to get master branch
            branch = self.session.execute(
                select(BranchModel).where(BranchModel.name == "master")
            ).scalar_one_or_none()

            parent_id = None
            if branch:
                parent_id = branch.snapshot_id
                self.parent_snapshot = SqlConfigSnapshot(self.session, parent_id)
            else:
                self.parent_snapshot = None

            # Create new ephemeral snapshot
            new_snap = SnapshotModel(parent_id=parent_id, committed=False)
            self.session.add(new_snap)
            self.session.flush()
            self.stage_snapshot_id = new_snap.id

        self.stage = SqlConfigStage(
            self.session, self.parent_snapshot, self.stage_snapshot_id
        )

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        if cycle:
            p.text("SqlConfigRepo(...)")
        else:
            with p.group(4, "SqlConfigRepo(", ")"):
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
        # Finalize the stage
        self.stage._finalize_commit()

        # Update branch
        branch = self.session.execute(
            select(BranchModel).where(BranchModel.name == "master")
        ).scalar_one_or_none()
        if branch:
            branch.snapshot_id = self.stage_snapshot_id
        else:
            branch = BranchModel(name="master", snapshot_id=self.stage_snapshot_id)
            self.session.add(branch)

        self.session.commit()

        # Start new stage from this new commit
        parent_id = self.stage_snapshot_id
        self.parent_snapshot = SqlConfigSnapshot(self.session, parent_id)

        new_snap = SnapshotModel(parent_id=parent_id, committed=False)
        self.session.add(new_snap)
        self.session.flush()
        self.stage_snapshot_id = new_snap.id

        self.stage = SqlConfigStage(
            self.session, self.parent_snapshot, self.stage_snapshot_id
        )


def create_sql_config_repo(
    session_maker: Callable[[], Session], stage_snapshot_id: int | None = None
) -> ConfigRepo:
    return SqlConfigRepo(session_maker, stage_snapshot_id)
