# ADR 0007: Azure infrastructure in three Terraform stacks, identities instead of keys

- **Status:** Accepted
- **Date:** 2026-11-21

## Context

Until now the pipeline ran locally and on Databricks Free Edition (ADR 0001). Module 7 moves it to real Azure, and the plan asks for:

- the platform built entirely from code;
- Terraform state in separate storage;
- Unity Catalog reaching the lake through a managed identity, not account keys;
- a destroy-and-rebuild proof;
- a budget alert before anything else.

## Decisions

### 1. Three stacks, applied in order

| Stack | Why it is separate |
|---|---|
| `bootstrap` | Creates the budget and the storage for Terraform state. It cannot keep its own state in storage it has not yet created, so its small state stays local. |
| `platform` | All Azure resources. |
| `unity-catalog` | Databricks objects. The Databricks provider needs the workspace URL, which exists only after `platform` is applied. In one stack, a provider configured from a resource that does not exist yet is a known source of failed plans. |

`unity-catalog` reads the platform's outputs through `terraform_remote_state`, so nothing is copied by hand between them.

### 2. Identities, never keys

- **Storage account keys are disabled** (`shared_access_key_enabled = false`) on both storage accounts. A leaked key cannot be used because no key works.
- **Unity Catalog** uses the Databricks access connector's system-assigned managed identity, with *Storage Blob Data Contributor* on the lake. Azure creates and rotates the identity's credentials, which never appear in code, config or a secret store.
- **Data Factory** has its own managed identity, with exactly three roles:
  - *Storage Blob Data Contributor* on the lake, to land raw files;
  - *Key Vault Secrets User*, to read connection secrets;
  - *Contributor* on the Databricks workspace, to start jobs.
- **Key Vault** uses Azure RBAC instead of access policies, so its permissions are role assignments like everything else.
- **Terraform** reads and writes state and storage with the deployer's Entra ID login (`use_azuread_auth`, `storage_use_azuread`).

*Alternative considered:* a service principal with a client secret in Key Vault. It works, but the secret must be created, stored, rotated and protected, which is exactly the burden managed identities remove.

### 3. State in versioned blob storage

The state container's account has blob versioning and 14-day soft delete, so a corrupted or deleted state file can be restored. Azure blob leases lock the state during runs, so two applies cannot write it at once.

### 4. Each medallion layer in its own container

`raw`, `bronze`, `silver` and `gold` are separate ADLS Gen2 containers, each an external location. Each schema's managed tables live in its layer's container. Access can therefore be granted per layer, and storage costs are visible per layer. The landing volume is external, in `raw/landing`, where Data Factory will drop files (Module 8).

### 5. Built to be destroyed

The plan's proof is apply → destroy → apply, and outside the 30-day credit the platform should run only when needed. So the Key Vault has purge protection off and is purged on destroy, and the catalog and schemas use `force_destroy`. Bronze can be rebuilt from the raw files. Production would turn purge protection on and remove `force_destroy`.

### 6. UAE North, Premium workspace

UAE North is the region closest to the data and its users. Unity Catalog needs the Premium Databricks tier; the workspace itself has no charge, and compute is billed by use.

### 7. Validated in CI without credentials

Every pull request runs `terraform fmt -check` and `terraform validate` on all three stacks with `-backend=false`. Unit tests read the `.tf` files and fail if a design choice drifts: account keys enabled, Key Vault without RBAC, a non-Premium workspace, a missing layer, or secrets in example files.

## Consequences

- The platform can be rebuilt from scratch with three `apply`s, and nothing secret lives in the repo or in Terraform variables.
- Applying needs an Azure login with rights to create role assignments (Owner, or Contributor plus User Access Administrator).
- New workspaces normally get a Unity Catalog metastore automatically. Where a region has none, an account admin must assign one once, outside Terraform.
- Destroying removes data. Keep the raw files; everything downstream is rebuilt from them.
