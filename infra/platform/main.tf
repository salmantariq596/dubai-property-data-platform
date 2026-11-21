# Platform: every Azure resource the pipeline runs on.
#
#   resource group
#   ├── ADLS Gen2 storage: raw, bronze, silver, gold containers (account keys disabled)
#   ├── Databricks workspace (Premium: needed for Unity Catalog)
#   ├── Databricks access connector: the managed identity Unity Catalog uses for storage
#   ├── Key Vault (RBAC)
#   └── Data Factory, with its own managed identity
#
# No service holds a key or password to another: each has a managed identity
# and a role assignment with only the rights it needs.

terraform {
  required_version = ">= 1.6"

  # Filled from backend.hcl: see backend.example.hcl and infra/README.md.
  backend "azurerm" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

provider "azurerm" {
  subscription_id     = var.subscription_id
  storage_use_azuread = true

  features {
    key_vault {
      # Destroy really deletes the vault, so the destroy-and-rebuild proof works.
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}

data "azurerm_client_config" "current" {}

locals {
  name = "${var.project}-${var.environment}"
  tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    stack       = "platform"
  }
  layers = toset(["raw", "bronze", "silver", "gold"])
}

resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# --- data lake -----------------------------------------------------------------------------

resource "azurerm_storage_account" "lake" {
  name                            = "st${var.project}${var.environment}${random_string.suffix.result}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true # ADLS Gen2: real directories, needed by abfss://
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  shared_access_key_enabled       = false # no account keys: identities only
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

# The person running Terraform creates the containers through Entra ID.
resource "azurerm_role_assignment" "deployer_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "time_sleep" "deployer_lake" {
  depends_on      = [azurerm_role_assignment.deployer_lake]
  create_duration = "60s"
}

resource "azurerm_storage_container" "layer" {
  for_each              = local.layers
  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
  depends_on            = [time_sleep.deployer_lake]
}

# --- Databricks ------------------------------------------------------------------------------

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${local.name}-databricks-managed"
  tags                        = local.tags
}

# Unity Catalog reaches the lake through this identity: no keys, no service principal secret.
resource "azurerm_databricks_access_connector" "main" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "connector_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# --- Key Vault -------------------------------------------------------------------------------

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.name}-${random_string.suffix.result}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # allows destroy-and-rebuild; turn on for production
  tags                       = local.tags
}

resource "azurerm_role_assignment" "deployer_key_vault" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# --- Data Factory ----------------------------------------------------------------------------

resource "azurerm_data_factory" "main" {
  name                = "adf-${local.name}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

# Data Factory lands raw files in the lake (Module 8) ...
resource "azurerm_role_assignment" "adf_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ... reads connection secrets from Key Vault ...
resource "azurerm_role_assignment" "adf_key_vault" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

# ... and starts Databricks jobs with its managed identity.
resource "azurerm_role_assignment" "adf_databricks" {
  scope                = azurerm_databricks_workspace.main.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}
