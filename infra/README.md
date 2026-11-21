# Infrastructure (Terraform)

The whole Azure platform, built from code. Design: [ADR 0007](../docs/adr/0007-azure-infrastructure.md).

| Stack | Creates | State |
|---|---|---|
| `bootstrap/` | Monthly **budget alert** (subscription), storage for Terraform state | Local (it creates the remote storage) |
| `platform/` | Resource group, ADLS Gen2 (`raw`, `bronze`, `silver`, `gold`), Databricks workspace (Premium), access connector (managed identity), Key Vault, Data Factory, role assignments | Remote: `platform.tfstate` |
| `unity-catalog/` | Storage credential, external locations, catalog `dubai`, schemas, volumes | Remote: `unity-catalog.tfstate` |

No service uses an account key or password: storage account keys are disabled, and every connection is a managed identity with one role assignment.

## Prerequisites

- An Azure subscription (the free account works). Activate it only when ready to deploy: the free credit lasts 30 days.
- [Terraform](https://developer.hashicorp.com/terraform/install) 1.6 or newer, and the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli).
- `az login`, then `az account show` to check the subscription.

## Build

```bash
# 1. Budget and state storage (once)
cd infra/bootstrap
cp example.tfvars terraform.tfvars        # set subscription_id and alert_email
terraform init
terraform apply
terraform output backend_config          # values for the next two stacks

# 2. Azure resources
cd ../platform
cp example.tfvars terraform.tfvars        # set subscription_id
cp backend.example.hcl backend.hcl        # paste the values from step 1, key = "platform.tfstate"
terraform init -backend-config=backend.hcl
terraform apply

# 3. Unity Catalog
cd ../unity-catalog
cp example.tfvars terraform.tfvars        # set state_storage_account
cp backend.example.hcl backend.hcl        # same values, key = "unity-catalog.tfstate"
terraform init -backend-config=backend.hcl
terraform apply
```

Then open the Databricks workspace (`terraform -chdir=infra/platform output databricks_workspace_url`), clone the repo as a Git folder, upload a file to `/Volumes/dubai/raw/landing/<dataset>/load_date=YYYY-MM-DD/`, and run `notebooks/06_run_pipeline`. With Terraform in place, `00_setup_unity_catalog` is not needed: it finds everything already there.

**Unity Catalog metastore:** new Azure Databricks workspaces get a Unity Catalog metastore automatically. If the `unity-catalog` stack reports that no metastore is assigned, an account admin assigns one in the Databricks account console (*Catalog → Metastores*), then apply again.

## Prove it rebuilds (plan, Phase 5)

```bash
terraform -chdir=infra/unity-catalog destroy
terraform -chdir=infra/platform destroy
terraform -chdir=infra/platform apply
terraform -chdir=infra/unity-catalog apply
```

Take a screenshot of the resource group (`rg-dpdp-dev`) for the README. The bootstrap stack (budget, state) stays.

**`destroy` deletes data:** the lake containers, the catalog and its tables, and the Key Vault (purged, so the name can be reused). Bronze can be rebuilt from the raw files, so keep a copy of what was uploaded to the landing volume.

## Costs

Idle, the platform costs little: storage by the GB, Key Vault and Data Factory per operation, and a Databricks workspace has no charge of its own. Compute is what costs: SQL warehouses and job clusters, billed while running. Keep clusters small with auto-termination, and run `terraform destroy` on `platform` and `unity-catalog` when not in use. The budget alert emails at 50% and 80% of the monthly budget, and when the forecast passes 100%.

## Files never committed

`terraform.tfvars` (subscription id, email), `backend.hcl`, `*.tfstate`, and `.terraform/` are git-ignored. Only the `example` files are committed.
