# Motivation

Modern applications often face complex configuration requirements that go beyond simple environment variables or static files.

## The Challenge

1.  **Multiple Environments**: Applications run in diverse environments (dev, staging, prod, on-prem) requiring distinct but often overlapping configurations.
2.  **Complex Configuration**: Configurations can be deeply nested structures, not just flat key-value pairs.
3.  **User-Provided Configuration**: In many SaaS or platform scenarios, end-users need to safely modify parts of the configuration without redeploying the application.
4.  **Audit & Rollback**: Changes need to be tracked, diff-ed, and reversible.

## The Config Plane Solution

`config-plane` addresses these challenges by introducing a structured, transactional approach to configuration management:

-   **Transactional Separation**: We distinguish between a **committed** state (safe, deployed) and a **stage** (work-in-progress). This allows extensive validation and diffing before applying changes.
-   **Abstract Storage**: Whether you are testing in memory, running lightly on SQLite, or need full Git-backed history, the API remains the same.
-   **Safeguards**: By treating configuration as a managed resource rather than raw files, we can enforce schemas and invariants.
