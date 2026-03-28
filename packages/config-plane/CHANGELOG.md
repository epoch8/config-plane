# Unreleased 0.2.0

* Expanded `ConfigRepo` and `ConfigStage` abstract interfaces (implemented for SQL, Git, and Memory backends):
    * **New**: `set_many(blobs)` (replaces SQL-specific `commit_if_changed`) - optimized bulk updates with change detection.
    * **New**: `get_branch_snapshot_id(branch)` - get the current snapshot ID for a branch.
    * **New**: `set_branch_snapshot_id(snapshot_id, branch)` - force branch pointer to a specific snapshot.
    * **New**: `snapshot_exists(snapshot_id)` - check for snapshot existence.
    * **Changed**: `get(key, snapshot_id=None)` - now accepts optional snapshot ID to read historical data.
* Added full **Git backend** (`create_git_config_repo`).

# 0.1.0

* Initial commit
