# config-plane

A structured, transactional configuration management library for Python applications.

`config-plane` allows you to manage runtime user provided configuration as a code. It supports using SQL database or GIT as a storage backend for flexibility

## Key Features

- **Transactional Updates**: Safely stage, diff, and commit changes.
- **Backend Agnostic**: SQL (SQLite, Postgres), and Git backends.
- **Snapshot Model**: Immutable configuration snapshots ensure consistency.

## Documentation

Full documentation, including motivation and quickstart guides, is available in the `docs/` directory.

To build the documentation locally:

```bash
uv sync
mdbook build docs
```

Then open `docs/book/index.html` in your browser.

## Quick Example

```python
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config_plane.impl.sql import create_sql_config_repo, Base

# Setup Database
engine = create_engine("sqlite:///config.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Initialize Repo
repo = create_sql_config_repo(Session, branch="dev")

# Set configuration values (must be bytes)
config_data = {"name": "MyService", "debug": True}
repo.set("app", json.dumps(config_data).encode("utf-8"))

# Commit changes
repo.commit()

# Read values
raw_config = repo.get("app")
print(json.loads(raw_config))
```

## Quick Install

```bash
uv sync
# or
pip install packages/config-plane
```
