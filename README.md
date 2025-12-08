# Config Plane

A flexible configuration management library implementing a snapshot/stage model for configuration persistence.

## Features

- **Abstract Config Repository**: Unified interface for configuration management.
- **Snapshot & Stage Model**: Clean separation between committed state (Snapshot) and work-in-progress changes (Stage).
- **Multiple Backends**:
  - **Memory**: For testing and ephemeral use cases.
  - **Git**: Version-controlled configuration storage using a local Git repository.
  - **SQL**: Database-backed configuration using SQLAlchemy (supports SQLite, PostgreSQL, etc.).
- **Transactional Workflow**: Changes are staged and then committed, ensuring atomic updates.

## Installation

```bash
uv sync
# or
pip install .
```

## Usage

### Basic Example

```python
from config_plane.impl.memory import create_memory_config_repo

# Initialize repo
repo = create_memory_config_repo({})

# Set configuration values
repo.set("app", {"name": "MyService", "debug": True})

# Check dirty state
if repo.is_dirty():
    print("Uncommitted changes present")

# Commit changes
repo.commit()

# Read values
config = repo.get("app")
print(config)
```

### Git Backend

```python
from pathlib import Path
from config_plane.impl.git import create_git_config_repo

repo_path = Path("./my-config-repo")
repo = create_git_config_repo(repo_path)

repo.set("server", {"host": "0.0.0.0"})
repo.commit()
```

### SQL Backend

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config_plane.impl.sql import create_sql_config_repo, Base

# Setup Database
engine = create_engine("sqlite:///config.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Initialize Repo
repo = create_sql_config_repo(Session)
repo.set("feature_flags", {"dark_mode": True})
repo.commit()
```

## Development

Run tests using pytest:

```bash
uv run pytest
```
