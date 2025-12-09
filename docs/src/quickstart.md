# Quickstart

## Installation

```bash
uv sync
# or
pip install .
```

## Basic Usage (SQL + SQLite)

This example shows how to use `config-plane` with a SQLite database. This is a robust starting point for most applications.

```python
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config_plane.impl.sql import create_sql_config_repo, Base

# Setup Database (using SQLite for this example)
engine = create_engine("sqlite:///config.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Initialize Repo
repo = create_sql_config_repo(Session)

# Set configuration values (must be bytes)
config_data = {"name": "MyService", "debug": True}
# Values are stored as bytes
repo.set("app", json.dumps(config_data).encode("utf-8"))

# Check dirty state
if repo.is_dirty():
    print("Uncommitted changes present")

# Commit changes
repo.commit()

# Read values
raw_config = repo.get("app")
if raw_config:
    config = json.loads(raw_config)
    print(config)
```

## Git Backend

For persistent storage with history, ideal for configuration as code workflows.

```python
from pathlib import Path
from config_plane.impl.git import create_git_config_repo

repo_path = Path("./my-config-repo")
repo = create_git_config_repo(repo_path)

repo.set("server", b'{"host": "0.0.0.0"}')
repo.commit()
```
