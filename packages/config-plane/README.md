# Config Plane

A flexible configuration management library implementing a snapshot/stage model for configuration persistence.

## Documentation

Full documentation is available in the `docs/` directory or at [Documentation Link](https://epoch8.github.io/config-plane/).

To build the documentation locally:

```bash
mdbook build docs
```

## Features

- **Abstract Config Repository**: Unified interface for configuration management.
- **Snapshot & Stage Model**: Clean separation between committed state (Snapshot) and work-in-progress changes (Stage).
- **Multiple Backends**: Memory, Git, SQL.

## Installation

```bash
uv sync
# or
pip install .
```
