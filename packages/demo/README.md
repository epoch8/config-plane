# Demo

This package demonstrates the functionality of the `config-plane` library.

## Scenario: Dev to Prod Release Cycle

This demo simulates a typical software development lifecycle where configuration changes are tested in a development environment before being promoted to production.

### Setup

- **Repository**: A central configuration store using the **SQL backend**.
- **Shared Database**: The configuration repository database is shared between all environments (`dev`, `prod`).
- **Branches**:
    - `prod`: The stable branch used by production applications.
    - `dev`: A development branch for testing changes.

### Steps

1.  **Initialize Repository**
    - Create a repository with an initial config on `prod`.
    - Config: `{"feature_x_enabled": b"false", "theme": b"light"}` (Note: Values are bytes)

2.  **Start Production App**
    - The app reads from `prod`.
    - Output: `Feature X: Disabled`, `Theme: Light`.

3.  **Start Development Session**
    - Create `dev` branch from `prod`.
    - App (in dev mode) reads from `dev`.
    - Output: `Feature X: Disabled`, `Theme: Light` (Same as prod).

4.  **Modify Configuration (in Dev)**
    - Update `dev`: `{"feature_x_enabled": b"true", "theme": b"dark"}`.
    - App (in dev mode) reflects changes.
    - Output: `Feature X: Enabled`, `Theme: Dark`.

5.  **Verify Isolation**
    - Check Production App again.
    - Output: `Feature X: Disabled`, `Theme: Light`. (Unaffected by dev changes).

6.  **Promote to Production**
    - Merge `dev` into `prod`.

7.  **Production Update**
    - Production App picks up the new config.
    - Output: `Feature X: Enabled`, `Theme: Dark`.
