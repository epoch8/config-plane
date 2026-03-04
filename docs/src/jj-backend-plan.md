# Implementation Plan: Replacing config-plane's VCS Engine with jj as a Library

> **Status:** Proposed  
> **Repository:** [epoch8/config-plane](https://github.com/epoch8/config-plane)  
> **Reference:** [jj-vcs/jj](https://github.com/jj-vcs/jj)

---

## Problem Statement

[`epoch8/config-plane`](https://github.com/epoch8/config-plane) is a Python library that provides a versioned key-value store for application configuration. It implements Git-like semantics — snapshots, staging, commits, branches, merge — across multiple storage backends: in-memory, Git subprocess, and a hand-rolled SQL implementation.

The SQL backend (`config_plane/impl/sql.py`) has independently converged on the same conceptual model as [`jj-vcs/jj`](https://github.com/jj-vcs/jj): content-addressed blobs, immutable snapshots (commits), a mutable stage (working copy), and branch pointers. This is not a coincidence — the problem domain (versioned structured data) maps directly onto what a VCS solves.

However, the custom implementation has several structural gaps that would require significant engineering effort to close correctly:

| Gap | Current State | Consequence |
|---|---|---|
| **No true 3-way merge** | Last-writer-wins overlay | Silent data loss when branches diverge |
| **No common ancestor tracking** | Single-parent linear chain only | Merge commits not representable; LCA not computable |
| **No content deduplication** | Blobs use auto-increment IDs | Identical values stored repeatedly |
| **O(keys × commits) storage** | `_finalize_commit()` copies all parent keys | Storage grows proportionally to history length |
| **Fragile `is_dirty()`** | Checks row existence, not content equality | False positives when setting the same value |
| **No commit metadata** | No author, timestamp, or description | No audit trail; cannot answer "who changed what" |
| **No operation log** | No history of mutations | Cannot undo, replay, or audit changes |

Rather than continuing to reinvent solutions to these problems incrementally, the proposal is to **replace the VCS engine entirely with `jj_lib`** — using all the design decisions the jj team has already solved and validated — while keeping the existing Python `ConfigRepo` / `ConfigStage` / `ConfigSnapshot` public API unchanged.

---

## Approach Overview

Use `jj_lib` (the core library of [jj-vcs/jj](https://github.com/jj-vcs/jj)) as a **pure Rust library dependency**, bypassing its CLI, filesystem workspace discovery, and all entry points. Expose it to Python via **PyO3** as a native extension module.

### Key Discovery: jj Is Fully Pluggable Without Its Entry Points

The jj architecture separates concerns through traits. Every storage component is an `Arc<dyn Trait>` and can be replaced independently:

- **`Backend` trait** — how blobs, trees, and commits are stored
- **`WorkingCopy` + `LockedWorkingCopy` traits** — how staged changes are tracked before commit
- **`OpStore` trait** — the operation log (audit trail of all mutations)
- **`OpHeadsStore` trait** — the pointer to the current operation head
- **`IndexStore` trait** — the commit graph index for ancestry queries

Critically, `RepoLoader::new()` accepts already-constructed instances of all five, and `Workspace::new_no_canonicalize()` assembles the top-level handle — **both with zero filesystem I/O**. This means:

- No `.jj/` directory is required
- No `StoreFactories` / type-file dispatch is needed (that is only for CLI workspace loading)
- No `Workspace::init_with_factories()` or `Workspace::load()` is needed
- All five components are backed by SQL; the only optional filesystem touch is a small cache directory for the commit graph index

### Architecture

```
Python (config-plane public API — unchanged)
    │  ConfigRepo / ConfigSnapshot / ConfigStage
    │
    │  PyO3 native extension (.so)
    ▼
Rust crate: config_plane_jj
    ├── SqlBackend           impl jj_lib::backend::Backend
    ├── SqlOpStore           impl jj_lib::op_store::OpStore
    ├── SqlOpHeadsStore      impl jj_lib::op_heads_store::OpHeadsStore
    ├── SqlWorkingCopy       impl jj_lib::working_copy::WorkingCopy
    ├── LockedSqlWorkingCopy impl jj_lib::working_copy::LockedWorkingCopy
    ├── SqlWorkingCopyFactory
    └── #[pyclass] ConfigRepo
            │
            └── jj_lib  (jj-vcs/jj)
                    ├── DAG engine (commit graph, ancestry, LCA)
                    ├── 3-way merge (Merge<T> — first-class conflict type)
                    ├── Content-addressed tree + blob model
                    ├── Operation log
                    └── ChangeId semantics (stable identity across rewrites)
```

### What config-plane Gets For Free

By delegating to `jj_lib`, the following problems are solved without writing any new algorithms:

- **True 3-way merge** via `MergedTree::merge(&base, &ours, &theirs)`
- **Least-common-ancestor lookup** via `repo.index().common_ancestors()`
- **Merge commit DAG** — commits can have multiple parent IDs
- **Content-addressed blobs** — identical values stored once, correct `is_dirty()` via hash comparison
- **Sparse tree model** — only changed keys stored per commit, O(changes) not O(all keys)
- **Conflict representation** — `Merge<T>` is a first-class type, not a silent overwrite
- **Full operation log** — every mutation is recorded; supports undo/replay/audit
- **ChangeId** — stable logical identity across amendments and rebases
- **Author + timestamp on every commit**

---

## Detailed Implementation Plan

### Phase 1: Rust Crate Scaffold

**Goal:** A buildable Rust crate with PyO3 and `jj_lib` dependencies that Python can import.

**Tasks:**

1. Create `packages/config-plane-jj/` as a new package in the monorepo workspace
2. Add `Cargo.toml`:

```toml
[package]
name = "config_plane_jj"
version = "0.1.0"
edition = "2021"

[lib]
name = "config_plane_jj"
crate-type = ["cdylib"]

[dependencies]
jj-lib = { git = "https://github.com/jj-vcs/jj", package = "jj-lib" }
pyo3       = { version = "0.22", features = ["extension-module"] }
sqlx       = { version = "0.8", features = ["sqlite", "postgres", "runtime-tokio"] }
tokio      = { version = "1", features = ["rt", "rt-multi-thread"] }
pollster   = "0.3"
async-trait = "0.1"
blake2     = "0.10"
hex        = "0.4"
```

3. Add `pyproject.toml` using `maturin` as the build backend
4. Stub `lib.rs` with a `#[pymodule]` returning an empty module
5. Add `maturin develop` to the CI pipeline

**Deliverable:** `import config_plane_jj` succeeds from Python.

**Estimated effort:** 0.5 days

---

### Phase 2: SQL Schema

**Goal:** Define the unified SQL schema backing all five jj components. One schema serves all stores — no separate per-component migrations.

```sql
-- Backend: content-addressed object store
CREATE TABLE IF NOT EXISTS jj_blobs (
    id      TEXT PRIMARY KEY,   -- hex(blake2b(content))
    content BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS jj_trees (
    id   TEXT PRIMARY KEY,      -- hex(blake2b(serialized entries))
    data BLOB NOT NULL          -- proto-serialized Tree
);

CREATE TABLE IF NOT EXISTS jj_commits (
    id   TEXT PRIMARY KEY,      -- hex(blake2b(serialized commit))
    data BLOB NOT NULL          -- proto-serialized Commit
);

-- OpStore: operation log
CREATE TABLE IF NOT EXISTS jj_operations (
    id   TEXT PRIMARY KEY,
    data BLOB NOT NULL          -- proto-serialized Operation
);

CREATE TABLE IF NOT EXISTS jj_views (
    id   TEXT PRIMARY KEY,
    data BLOB NOT NULL          -- proto-serialized View
);

-- OpHeadsStore: current operation head pointer(s)
CREATE TABLE IF NOT EXISTS jj_op_heads (
    id TEXT PRIMARY KEY         -- operation IDs that are current heads
);

-- WorkingCopy: per-workspace staged state
CREATE TABLE IF NOT EXISTS jj_working_copy (
    workspace_name TEXT PRIMARY KEY,
    operation_id   TEXT NOT NULL,
    tree_id        TEXT NOT NULL,  -- committed tree (from last check_out)
    staged_items   BLOB           -- serialized map: key → blob_id override (NULL = deleted)
);
```

**Notes:**
- All object IDs are content hashes — identical content is stored once across all commits
- `jj_working_copy.staged_items` stores a sparse delta on top of the committed tree, equivalent to the current `SnapshotItemModel` rows but scoped to the in-progress stage only
- Serialization format for `jj_trees`, `jj_commits`, `jj_operations`, `jj_views` uses jj's existing protobuf definitions (`simple_store.proto`, `simple_op_store.proto`) — no new wire format required

**Estimated effort:** 0.5 days

---

### Phase 3: `SqlBackend` — `impl Backend`

**Goal:** Implement [`jj_lib::backend::Backend`](https://github.com/jj-vcs/jj/blob/main/lib/src/backend.rs) — the content-addressed object store for blobs, trees, and commits.

| Method | SQL implementation |
|---|---|
| `name()` | `"sql"` |
| `read_file(id)` | `SELECT content FROM jj_blobs WHERE id = ?` |
| `write_file(contents)` | `blake2b(content)` → `INSERT OR IGNORE INTO jj_blobs` |
| `read_tree(id)` | `SELECT data FROM jj_trees WHERE id = ?` → deserialize proto |
| `write_tree(tree)` | serialize → hash → `INSERT OR IGNORE INTO jj_trees` |
| `read_commit(id)` | `SELECT data FROM jj_commits WHERE id = ?` → deserialize proto |
| `write_commit(commit, sign_fn)` | serialize → hash → `INSERT OR IGNORE INTO jj_commits` |
| `root_commit_id()` | fixed all-zeros `CommitId` (jj convention) |
| `root_change_id()` | fixed all-zeros `ChangeId` |
| `empty_tree_id()` | `write_tree(&Tree { entries: vec![] })` on init |
| `read_symlink()`, `write_symlink()` | `Err(BackendError::Unsupported)` — config has no symlinks |
| `read_copy_record_stream()` | `Ok(empty stream)` — not applicable |

**Serialization:** Reuse jj's `simple_store.proto` protobuf format from `jj_lib::protos`. This avoids inventing a new wire format and remains compatible with jj tooling if needed for debugging.

**Estimated effort:** 3 days

---

### Phase 4: `SqlOpStore` — `impl OpStore`

**Goal:** Implement [`jj_lib::op_store::OpStore`](https://github.com/jj-vcs/jj/blob/main/lib/src/op_store.rs) to persist jj's operation log in SQL.

The operation log records every mutation to the repo (commits written, bookmarks updated, merges performed). It is the foundation of the audit trail and supports undo/replay.

| Method | SQL implementation |
|---|---|
| `name()` | `"sql"` |
| `root_operation_id()` | fixed all-zeros `OperationId` |
| `read_operation(id)` | `SELECT data FROM jj_operations WHERE id = ?` → deserialize |
| `write_operation(op)` | serialize → `INSERT INTO jj_operations` |
| `read_view(id)` | `SELECT data FROM jj_views WHERE id = ?` → deserialize |
| `write_view(view)` | serialize → `INSERT INTO jj_views` |

**Serialization:** Use `simple_op_store.proto` (already in `jj_lib::protos`).

**Estimated effort:** 1.5 days

---

### Phase 5: `SqlOpHeadsStore` — `impl OpHeadsStore`

**Goal:** Implement [`jj_lib::op_heads_store::OpHeadsStore`](https://github.com/jj-vcs/jj/blob/main/lib/src/op_heads_store.rs) — tracks which operation IDs are the current "heads" (most recent operations, normally exactly one).

| Method | SQL implementation |
|---|---|
| `name()` | `"sql"` |
| `add_op_head(id)` | `INSERT INTO jj_op_heads (id) VALUES (?)` |
| `remove_op_head(id)` | `DELETE FROM jj_op_heads WHERE id = ?` |
| `get_op_heads()` | `SELECT id FROM jj_op_heads` |
| `update_op_heads(old_ids, new_id)` | transaction: delete all `old_ids`, insert `new_id` |

Multiple heads only arise from concurrent writes; jj resolves them automatically via operation merging.

**Estimated effort:** 0.5 days

---

### Phase 6: `SqlWorkingCopy` — `impl WorkingCopy` + `LockedWorkingCopy`

**Goal:** Implement the staged-changes concept entirely in SQL with no filesystem involvement. This is the direct replacement for `SqlConfigStage` in the current Python code.

**Design:**

The `WorkingCopy` trait has no filesystem requirements. The `tree()` method returns a `MergedTree` representing the last committed state. The staged delta — the set of key→value overrides not yet committed — is stored as `staged_items` in `jj_working_copy`.

The staged view at read time is: **committed tree** + **`staged_items` overlay**.

```
jj_working_copy row
  └── tree_id ──────────────────→ jj_trees → jj_blobs
  └── staged_items (sparse map)
        "app/debug"  → blob_id_A   (override)
        "app/theme"  → NULL         (deletion)
        (all other keys come from tree_id)
```

**`impl WorkingCopy` methods:**

| Method | Implementation |
|---|---|
| `name()` | `"sql"` |
| `workspace_name()` | stored field |
| `operation_id()` | stored field (`jj_working_copy.operation_id`) |
| `tree()` | `SELECT tree_id FROM jj_working_copy WHERE workspace_name = ?` → `store.get_root_tree()` |
| `sparse_patterns()` | `&[RepoPathBuf::root()]` — always everything (no sparse checkout) |
| `start_mutation()` | clone state into `LockedSqlWorkingCopy` |

**`impl LockedWorkingCopy` methods:**

| Method | Implementation |
|---|---|
| `old_operation_id()` | the operation_id at lock time |
| `old_tree()` | the committed tree at lock time |
| `snapshot()` | **no-op** — returns current staged tree; no filesystem to scan |
| `check_out(commit)` | `UPDATE jj_working_copy SET tree_id = ?, operation_id = ?`; clear `staged_items` |
| `reset(commit)` | same as `check_out` but preserve `staged_items` |
| `finish(op_id)` | flush `staged_items`; `UPDATE jj_working_copy SET operation_id = ?` |
| `sparse_patterns()` | `&[RepoPathBuf::root()]` |
| `set_sparse_patterns()` | no-op |
| `rename_workspace()` | update name field |

The `snapshot()` no-op is the key simplification: `LocalWorkingCopy` uses `snapshot()` to scan the filesystem for changes since the last snapshot. For `SqlWorkingCopy`, staged changes are always written explicitly via `set()` — there is no implicit filesystem state to discover.

**Estimated effort:** 2 days

---

### Phase 7: Wiring — `open_or_create()`

**Goal:** Assemble all five components into a live jj `Workspace` + `ReadonlyRepo` with zero filesystem initialization.

```rust
pub fn open_or_create(db_url: &str) -> Result<(Workspace, Arc<ReadonlyRepo>)> {
    let pool = create_pool(db_url).block_on()?;
    run_migrations(&pool).block_on()?;   // creates tables if not present

    let settings = UserSettings::from_config(StackedConfig::empty())?;

    // All five stores constructed directly — no filesystem
    let backend    = Box::new(SqlBackend::new(pool.clone()));
    let store      = Arc::new(Store::new(backend, Signer::none(), MergeOptions::default()));
    let op_store   = Arc::new(SqlOpStore::new(pool.clone()))      as Arc<dyn OpStore>;
    let op_heads   = Arc::new(SqlOpHeadsStore::new(pool.clone())) as Arc<dyn OpHeadsStore>;
    let index      = Arc::new(DefaultIndexStore::load_or_create(&index_cache_path(db_url)?)?)
                         as Arc<dyn IndexStore>;
    let submodules = Arc::new(NullSubmoduleStore)                  as Arc<dyn SubmoduleStore>;

    // RepoLoader::new — no .jj/ directory, no type-file dispatch
    let loader = RepoLoader::new(settings, store.clone(), op_store, op_heads, index, submodules);
    let repo   = loader.load_at_head().block_on()??;

    // SqlWorkingCopy — no filesystem
    let wc = Box::new(SqlWorkingCopy::load(pool.clone(), WorkspaceName::DEFAULT.to_owned(), &repo));

    // Workspace::new_no_canonicalize — no filesystem I/O at all
    let workspace = Workspace::new_no_canonicalize(
        PathBuf::from("."),  // workspace_root: irrelevant, not used
        PathBuf::from("."),  // repo_path: irrelevant, not used
        wc,
        loader,
    );

    Ok((workspace, repo))
}
```

**Note on `DefaultIndexStore`:** The default jj commit graph index uses segment files on disk for efficient ancestor queries. Use `dirs::cache_dir().join("config-plane/jj-index/{fingerprint_of_db_url}/")` as the path — a stable per-database directory. The index is rebuilt automatically and lazily from the operation log if the directory is missing or stale; it is purely a performance cache.

**Estimated effort:** 1 day

---

### Phase 8: PyO3 Bindings

**Goal:** Expose a Python API that exactly matches the existing `ConfigRepo` interface, making the Rust backend a drop-in replacement.

```rust
#[pyclass]
pub struct ConfigRepo {
    workspace: Workspace,
    repo: Arc<ReadonlyRepo>,
    pool: Pool,
    branch: String,
}

#[pymethods]
impl ConfigRepo {
    #[new]
    fn new(db_url: &str, branch: Option<&str>) -> PyResult<Self>;

    fn get(&self, key: &str) -> PyResult<Option<Vec<u8>>>;
    fn set(&mut self, key: &str, value: Option<Vec<u8>>) -> PyResult<()>;
    fn is_dirty(&self) -> PyResult<bool>;  // content hash comparison, not row existence
    fn commit(&mut self, description: Option<&str>) -> PyResult<()>;
    fn switch_branch(&mut self, branch: &str) -> PyResult<()>;
    fn create_branch(&mut self, new_branch: &str, from_branch: Option<&str>) -> PyResult<()>;
    fn list_branches(&self) -> PyResult<Vec<String>>;
    fn merge(&mut self, branch: &str) -> PyResult<()>;  // true 3-way merge
}
```

**The `commit()` flow:**

```rust
fn commit(&mut self, description: Option<&str>) -> PyResult<()> {
    // 1. Build new MergedTree from staged_items applied over parent tree
    let new_tree = self.build_tree_from_stage()?;
    // 2. Start jj transaction
    let mut tx = self.repo.start_transaction();
    // 3. Write new commit with current branch head as parent
    let parent_id = self.current_branch_head()?;
    let new_commit = tx.repo_mut()
        .new_commit(vec![parent_id], new_tree)
        .set_description(description.unwrap_or(""))
        .write().block_on()??;
    // 4. Advance branch bookmark to new commit
    tx.repo_mut().set_local_bookmark_target(
        &RefName::from(&self.branch),
        RefTarget::normal(new_commit.id().clone()),
    );
    // 5. Commit transaction (writes to op log)
    self.repo = tx.commit("commit").block_on()??;
    // 6. Update working copy to new commit
    self.wc_mut().check_out(&new_commit).block_on()??;
    Ok(())
}
```

**The `merge()` flow — true 3-way merge:**

```rust
fn merge(&mut self, branch: &str) -> PyResult<()> {
    let ours_id   = self.current_branch_head()?;
    let theirs_id = self.branch_head(branch)?;

    // 1. Find least-common ancestor via jj's index
    let lca_ids = self.repo.index().common_ancestors(&[ours_id.clone()], &[theirs_id.clone()])?;
    let lca_commit  = self.repo.store().get_commit(&lca_ids[0]).block_on()??;
    let ours_commit = self.repo.store().get_commit(&ours_id).block_on()??;
    let theirs_commit = self.repo.store().get_commit(&theirs_id).block_on()??;

    // 2. 3-way merge of trees (conflicts become Merge<T>, not silent overwrites)
    let merged_tree = ours_commit.tree()?
        .merge(&lca_commit.tree()?, &theirs_commit.tree()?)
        .block_on()??;

    // 3. Stage the merged tree as the new working state
    self.stage_tree(merged_tree)?;
    Ok(())
}
```

**Estimated effort:** 2 days

---

### Phase 9: Python Integration

**Goal:** Wire the Rust extension into the existing Python package with zero changes to the public API.

1. Add `config_plane_jj` as a dependency in `packages/config-plane/pyproject.toml`
2. Add thin Python wrapper:

```python
# packages/config-plane/config_plane/impl/jj.py
from config_plane.base import ConfigRepo, Blob
import config_plane_jj as _jj  # native extension

class JjConfigRepo(ConfigRepo):
    """ConfigRepo backed by jj_lib via Rust/PyO3.
    
    Drop-in replacement for SqlConfigRepo with correct merge semantics,
    content deduplication, and a full audit trail.
    """

    def __init__(self, db_url: str, branch: str = "master") -> None:
        self._inner = _jj.ConfigRepo(db_url, branch)

    def get(self, key: str) -> Blob | None:
        return self._inner.get(key)

    def set(self, key: str, value: Blob | None) -> None:
        self._inner.set(key, value)

    def is_dirty(self) -> bool:
        return self._inner.is_dirty()

    def commit(self) -> None:
        self._inner.commit()

    def switch_branch(self, branch: str) -> None:
        self._inner.switch_branch(branch)

    def create_branch(self, new_branch: str, from_branch: str | None = None) -> None:
        self._inner.create_branch(new_branch, from_branch)

    def list_branches(self) -> list[str]:
        return self._inner.list_branches()

    def merge(self, branch: str) -> None:
        self._inner.merge(branch)
```

3. Export `JjConfigRepo` from `config_plane/__init__.py` alongside existing backends
4. Update documentation

**Estimated effort:** 0.5 days

---

### Phase 10: Testing and Validation

**Goal:** Verify behavioral equivalence with the existing SQL backend, then validate correctness improvements unique to jj.

**Reuse existing tests:**

Parameterize `conftest.py` to include `JjConfigRepo` alongside `SqlConfigRepo` and `MemoryConfigRepo`. All tests in `tests/test_merge.py`, `tests/test_basic.py`, etc., must pass without modification.

**New tests for jj-specific correctness:**

```python
# packages/config-plane/tests/test_jj_backend.py

def test_true_3way_merge_non_conflicting(jj_repo):
    """Keys changed only in one branch are taken from that branch.
    Keys changed in neither branch are taken from the common ancestor."""

def test_true_3way_merge_conflict_detection(jj_repo):
    """Keys changed differently in both branches since LCA produce a conflict
    object, not a silent overwrite."""

def test_is_dirty_content_equality(jj_repo):
    """set(key, same_value_as_committed) should NOT mark the repo as dirty."""

def test_merge_commit_has_two_parents(jj_repo):
    """After merge + commit, the resulting commit has two parent commit IDs."""

def test_blob_deduplication(jj_repo):
    """Writing the same bytes for the same key across multiple commits
    results in a single row in jj_blobs."""

def test_audit_trail(jj_repo):
    """Every commit is recorded in jj_operations with author and timestamp."""

def test_history_traversal(jj_repo):
    """repo.index().walk_ancestors() yields all commits in topological order."""

def test_operation_log_undo(jj_repo):
    """The repo can be reloaded at a previous operation ID,
    returning the state as it was at that point."""
```

**Estimated effort:** 2 days

---

## What Gets Replaced vs. What Stays

| Component | Before (Python SQL) | After (jj_lib) |
|---|---|---|
| Blob storage | Auto-increment IDs, no deduplication | Content-addressed (`blake2b`), one row per unique value |
| Snapshot model | Single-parent linear chain | Full DAG with multi-parent merge commits |
| Commit storage | Copy all parent keys on every commit — O(keys × commits) | Sparse trees — O(changed keys) per commit |
| Branch pointers | `BranchModel` table | jj bookmarks in op store `View` |
| `merge()` | Last-writer-wins overlay | True 3-way merge with LCA, first-class conflict type |
| `is_dirty()` | Row existence check | Content hash comparison |
| Commit metadata | None | Author, timestamp, description on every commit |
| Audit trail | None | Full jj operation log (every mutation recorded) |
| `_finalize_commit()` | Copies all parent rows to new snapshot | Eliminated — jj trees are structurally shared |
| Concurrent write safety | Manual | jj op-heads merge resolution |
| **Python public API** | `ConfigRepo` / `ConfigStage` / `ConfigSnapshot` | **Unchanged** |
| **Deployment** | Pure Python | Requires Rust build step via `maturin` |

---

## Effort Estimate

| Phase | Description | Estimated Effort |
|---|---|---|
| 1 | Rust crate scaffold + PyO3 + CI | 0.5 days |
| 2 | SQL schema + migrations | 0.5 days |
| 3 | `SqlBackend` (blob/tree/commit store) | 3 days |
| 4 | `SqlOpStore` (operation log) | 1.5 days |
| 5 | `SqlOpHeadsStore` (head pointer) | 0.5 days |
| 6 | `SqlWorkingCopy` (staged changes) | 2 days |
| 7 | Wiring — `open_or_create()` | 1 day |
| 8 | PyO3 bindings | 2 days |
| 9 | Python integration + wrapper | 0.5 days |
| 10 | Tests and validation | 2 days |
| **Total** | | **~14 days** |

---

## Risks and Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| `jj_lib` API instability (no semver guarantee yet) | Medium | Pin to a specific commit SHA; upgrade deliberately |
| `DefaultIndexStore` requires filesystem | Low | Use a stable per-database cache directory; index is rebuilt automatically if missing |
| `maturin` build complexity in CI | Low | Well-established toolchain; GitHub Actions has first-class support |
| Proto serialization format changes in jj | Low | Pinned dependency; own the migration path |
| Rust compile times in development | Medium | Use `maturin develop` with incremental compilation; only recompile on Rust changes |

---

## References

- [`jj_lib::backend::Backend`](https://github.com/jj-vcs/jj/blob/main/lib/src/backend.rs) — trait definition for object storage
- [`jj_lib::working_copy::WorkingCopy`](https://github.com/jj-vcs/jj/blob/main/lib/src/working_copy.rs) — trait definition for staged changes
- [`jj_lib::repo::RepoLoader`](https://github.com/jj-vcs/jj/blob/main/lib/src/repo.rs) — `new()` constructor for library-first usage
- [`jj_lib::workspace::Workspace::new_no_canonicalize`](https://github.com/jj-vcs/jj/blob/main/lib/src/workspace.rs) — zero-filesystem-IO workspace construction
- [`cli/examples/custom-working-copy/main.rs`](https://github.com/jj-vcs/jj/blob/main/cli/examples/custom-working-copy/main.rs) — official example of a custom `WorkingCopy` implementation
- [`cli/examples/custom-backend/main.rs`](https://github.com/jj-vcs/jj/blob/main/cli/examples/custom-backend/main.rs) — official example of a custom `Backend` implementation
- [`epoch8/config-plane`](https://github.com/epoch8/config-plane) — this repository
