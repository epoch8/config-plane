# Features

`config-plane` offers a robust set of features for managing complex configurations.

## Abstract Config Repository
A unified Python API (`ConfigRepo`) that decouples your application logic from the storage backend.

## Snapshot & Stage Model
-   **ConfigSnapshot**: Represents an immutable point-in-time view of the configuration (like a Git commit).
-   **ConfigStage**: A mutable workspace derived from a snapshot. You make changes here, validate them, and then commit.
-   **Atomic Commits**: Changes in the stage are applied atomically to create a new snapshot.

## Multiple Backends

### Memory
Ideal for unit tests and ephemeral processes. Fast and requires no external dependencies.

### Git
Stores configuration in a local Git repository.
-   **Audit Trail**: Every change is a Git commit.
-   **Human Readable**: Configurations are stored as standard files (JSON/YAML) that can be inspected with standard tools.
-   **Branching**: Supports standard Git operations.

### SQL
database-backed storage using SQLAlchemy.
-   **Transactional**: Leverages database transactions for consistency.
-   **Wide Support**: Works with SQLite, PostgreSQL, MySQL, and others supported by SQLAlchemy.

## Data Integrity
-   **Blobs**: Configuration values are stored as raw bytes (`Blob`), allowing storage of JSON, YAML, or any other format.
-   **Metadata**: Snapshots and Repositories tracks metadata like revisions and parents to ensure history graph consistency.
