# CI/CD identity: lets GitHub Actions deploy the Databricks bundle without any secret.
#
# GitHub issues a short-lived token for each workflow run; Microsoft Entra ID
# trusts it through federated credentials scoped to this repository:
#   - pull requests              -> validate the bundle
#   - the "production" environment -> deploy the bundle (runs on merge to main)
# No client secret is created, so there is nothing to store, rotate or leak.
#
# The identity becomes a Databricks workspace admin (Contributor on the
# workspace) and gets ALL PRIVILEGES on the catalog, because the deployed jobs
# run as it.

terraform {
  required_version = ">= 1.6"

  # Filled from backend.hcl: same storage as the other stacks, key cicd.tfstate.
  backend "azurerm" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true
}

provider "azuread" {}

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
  github   = "https://token.actions.githubusercontent.com"
  audience = ["api://AzureADTokenExchange"]
}

provider "databricks" {
  host                        = "https://${local.platform.databricks_workspace_url}"
  azure_workspace_resource_id = local.platform.databricks_workspace_id
  auth_type                   = "azure-cli"
}

data "azuread_client_config" "current" {}

resource "azuread_application" "github" {
  display_name = "dpdp-github-actions-${local.platform.environment}"
  owners       = [data.azuread_client_config.current.object_id]
}

resource "azuread_service_principal" "github" {
  client_id = azuread_application.github.client_id
  owners    = [data.azuread_client_config.current.object_id]
}

resource "azuread_application_federated_identity_credential" "pull_requests" {
  application_id = azuread_application.github.id
  display_name   = "github-pull-requests"
  description    = "Validate the Databricks bundle on pull requests"
  issuer         = local.github
  audiences      = local.audience
  subject        = "repo:${var.github_repository}:pull_request"
}

resource "azuread_application_federated_identity_credential" "production" {
  application_id = azuread_application.github.id
  display_name   = "github-production"
  description    = "Deploy the Databricks bundle from the production environment"
  issuer         = local.github
  audiences      = local.audience
  subject        = "repo:${var.github_repository}:environment:production"
}

# Workspace admin on first sign-in: can deploy jobs and create job clusters.
resource "azurerm_role_assignment" "github_databricks" {
  scope                = local.platform.databricks_workspace_id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.github.object_id
}

# The deployed jobs run as this identity, so it needs the data too.
resource "databricks_service_principal" "github" {
  application_id = azuread_application.github.client_id
  display_name   = azuread_application.github.display_name
}

resource "databricks_grant" "catalog" {
  catalog    = var.catalog
  principal  = databricks_service_principal.github.application_id
  privileges = ["ALL_PRIVILEGES"]
}
