# Unity Catalog on top of the platform: how Databricks reaches the lake.
#
#   storage credential  (the access connector's managed identity: no keys)
#   └── external locations: abfss://raw|bronze|silver|gold@<lake>/
#       └── catalog "dubai"
#           ├── raw      volume landing      (external: where Data Factory drops files)
#           ├── bronze   volume checkpoints  (Auto Loader state)
#           ├── silver
#           ├── gold     volume exports      (Parquet for Power BI)
#           └── quality
#
# Each schema's managed tables live in its own container, so the medallion
# layers are separate in storage too. These are the objects
# notebooks/00_setup_unity_catalog creates by hand on Free Edition.

terraform {
  required_version = ">= 1.6"

  # Filled from backend.hcl: same storage as the platform stack, key unity-catalog.tfstate.
  backend "azurerm" {}

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

data "terraform_remote_state" "platform" {
  backend = "azurerm"
  config = {
    resource_group_name  = var.state_resource_group
    storage_account_name = var.state_storage_account
    container_name       = var.state_container
    key                  = "platform.tfstate"
    use_azuread_auth     = true
  }
}

locals {
  platform = data.terraform_remote_state.platform.outputs
  env      = local.platform.environment
  lake     = local.platform.lake_storage_account
  root     = { for c in local.platform.lake_containers : c => "abfss://${c}@${local.lake}.dfs.core.windows.net/" }

  schemas = {
    raw     = { storage = "${local.root.raw}_managed", comment = "Landing zone for raw DLD files" }
    bronze  = { storage = local.root.bronze, comment = "Raw data as delivered: all strings, append-only" }
    silver  = { storage = local.root.silver, comment = "Cleaned, typed, deduplicated" }
    gold    = { storage = "${local.root.gold}tables", comment = "Star schema and aggregates for Power BI" }
    quality = { storage = "${local.root.gold}quality", comment = "Data quality results" }
  }
}

provider "databricks" {
  host                        = "https://${local.platform.databricks_workspace_url}"
  azure_workspace_resource_id = local.platform.databricks_workspace_id
  auth_type                   = "azure-cli" # the same login as az / Terraform
}

resource "databricks_storage_credential" "lake" {
  name    = "lake-${local.env}"
  comment = "Managed identity of the Databricks access connector"

  azure_managed_identity {
    access_connector_id = local.platform.access_connector_id
  }
}

resource "databricks_external_location" "layer" {
  for_each        = local.root
  name            = "${each.key}-${local.env}"
  url             = each.value
  credential_name = databricks_storage_credential.lake.id
  comment         = "The ${each.key} container"
}

resource "databricks_catalog" "main" {
  name          = var.catalog
  storage_root  = "${local.root.gold}catalog"
  comment       = "Dubai Property Data Platform"
  force_destroy = true # destroy removes the catalog with its tables: rebuild from raw files
  depends_on    = [databricks_external_location.layer]
}

resource "databricks_schema" "layer" {
  for_each      = local.schemas
  catalog_name  = databricks_catalog.main.name
  name          = each.key
  storage_root  = each.value.storage
  comment       = each.value.comment
  force_destroy = true
}

resource "databricks_volume" "landing" {
  name             = "landing"
  catalog_name     = databricks_catalog.main.name
  schema_name      = databricks_schema.layer["raw"].name
  volume_type      = "EXTERNAL"
  storage_location = "${local.root.raw}landing"
  comment          = "Raw DLD files: <dataset>/load_date=YYYY-MM-DD/*.csv"
}

resource "databricks_volume" "checkpoints" {
  name         = "checkpoints"
  catalog_name = databricks_catalog.main.name
  schema_name  = databricks_schema.layer["bronze"].name
  volume_type  = "MANAGED"
  comment      = "Auto Loader checkpoints and inferred schemas"
}

resource "databricks_volume" "exports" {
  name         = "exports"
  catalog_name = databricks_catalog.main.name
  schema_name  = databricks_schema.layer["gold"].name
  volume_type  = "MANAGED"
  comment      = "Parquet exports for Power BI (notebooks/07_export_gold)"
}
