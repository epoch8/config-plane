---
trigger: always_on
---

Run all commands using uv

```bash
uv run pytest
```

To run specific tests:

```bash
uv run pytest packages/config-plane/tests/test_repo_common.py
```