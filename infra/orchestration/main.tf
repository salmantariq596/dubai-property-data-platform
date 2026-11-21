# Orchestration: Data Factory content, deployed into the factory the platform stack created.
#
#   linked services  ls_lake (managed identity), ls_source (DLD open data, HTTP)
#   datasets         ds_source_file, ds_landing_file (binary: files are copied as delivered)
#   pipelines        adf/pipeline/*.json
#   trigger          tr_daily: pl_daily every day at 02:00 Dubai time (off until enabled)
#
# The pipelines are kept as JSON in ADF's own export format, so they can be
# read, reviewed and tested as files. Parameters that depend on the deployment
# (Databricks host and job id, source URLs, alert webhook) are set here.

terraform {
  required_version = ">= 1.6"

  # Filled from backend.hcl: same storage as the other stacks, key orchestration.tfstate.
  backend "azurerm" {}

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true
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
  adf_dir  = "${path.module}/../../adf/pipeline"

  pipelines = {
    for f in fileset(local.adf_dir, "*.json") :
    trimsuffix(f, ".json") => jsondecode(file("${local.adf_dir}/${f}")).properties
  }

  # Deployment-specific values replace the defaults in the JSON.
  parameter_values = {
    databricks_host   = local.platform.databricks_workspace_url
    databricks_job_id = var.databricks_job_id
    source_urls       = jsonencode(var.source_urls)
    alert_webhook_url = var.alert_webhook_url
    catalog           = var.catalog
  }

  # ADF rejects a pipeline that calls one it does not know yet: create callees first.
  pipeline_levels = {
    leaf = ["pl_alert", "pl_ingest_dataset"]
    mid  = ["pl_land_day", "pl_run_job"]
    top  = ["pl_daily", "pl_backfill"]
  }
}

data "azurerm_data_factory" "main" {
  name                = local.platform.data_factory_name
  resource_group_name = local.platform.resource_group
}

# --- connections -------------------------------------------------------------------------

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "lake" {
  name                 = "ls_lake"
  data_factory_id      = data.azurerm_data_factory.main.id
  url                  = "https://${local.platform.lake_storage_account}.dfs.core.windows.net"
  use_managed_identity = true # Storage Blob Data Contributor, granted in the platform stack
}

resource "azurerm_data_factory_linked_service_web" "source" {
  name                = "ls_source"
  data_factory_id     = data.azurerm_data_factory.main.id
  authentication_type = "Anonymous"
  url                 = var.source_base_url
}

# --- datasets: binary, so files land exactly as delivered ---------------------------------

resource "azurerm_data_factory_custom_dataset" "source_file" {
  name            = "ds_source_file"
  data_factory_id = data.azurerm_data_factory.main.id
  type            = "Binary"

  linked_service {
    name = azurerm_data_factory_linked_service_web.source.name
  }

  parameters = { relative_url = "" }

  type_properties_json = jsonencode({
    location = {
      type        = "HttpServerLocation"
      relativeUrl = { value = "@dataset().relative_url", type = "Expression" }
    }
  })
}

resource "azurerm_data_factory_custom_dataset" "landing_file" {
  name            = "ds_landing_file"
  data_factory_id = data.azurerm_data_factory.main.id
  type            = "Binary"

  linked_service {
    name = azurerm_data_factory_linked_service_data_lake_storage_gen2.lake.name
  }

  parameters = { dataset = "", load_date = "" }

  # raw/landing is the Unity Catalog volume dubai.raw.landing (unity-catalog stack).
  type_properties_json = jsonencode({
    location = {
      type       = "AzureBlobFSLocation"
      fileSystem = "raw"
      folderPath = {
        value = "@concat('landing/', dataset().dataset, '/load_date=', dataset().load_date)"
        type  = "Expression"
      }
      fileName = { value = "@concat(dataset().dataset, '.csv')", type = "Expression" }
    }
  })
}

# --- pipelines ------------------------------------------------------------------------------

resource "azurerm_data_factory_pipeline" "leaf" {
  for_each        = toset(local.pipeline_levels.leaf)
  name            = each.key
  data_factory_id = data.azurerm_data_factory.main.id
  description     = local.pipelines[each.key].description
  activities_json = jsonencode(local.pipelines[each.key].activities)
  parameters = {
    for k, v in local.pipelines[each.key].parameters : k => lookup(local.parameter_values, k, v.defaultValue)
  }
  variables = { for k, v in try(local.pipelines[each.key].variables, {}) : k => v.defaultValue }

  depends_on = [
    azurerm_data_factory_custom_dataset.source_file,
    azurerm_data_factory_custom_dataset.landing_file,
  ]
}

resource "azurerm_data_factory_pipeline" "mid" {
  for_each        = toset(local.pipeline_levels.mid)
  name            = each.key
  data_factory_id = data.azurerm_data_factory.main.id
  description     = local.pipelines[each.key].description
  activities_json = jsonencode(local.pipelines[each.key].activities)
  parameters = {
    for k, v in local.pipelines[each.key].parameters : k => lookup(local.parameter_values, k, v.defaultValue)
  }
  variables  = { for k, v in try(local.pipelines[each.key].variables, {}) : k => v.defaultValue }
  depends_on = [azurerm_data_factory_pipeline.leaf]
}

resource "azurerm_data_factory_pipeline" "top" {
  for_each        = toset(local.pipeline_levels.top)
  name            = each.key
  data_factory_id = data.azurerm_data_factory.main.id
  description     = local.pipelines[each.key].description
  activities_json = jsonencode(local.pipelines[each.key].activities)
  parameters = {
    for k, v in local.pipelines[each.key].parameters : k => lookup(local.parameter_values, k, v.defaultValue)
  }
  variables  = { for k, v in try(local.pipelines[each.key].variables, {}) : k => v.defaultValue }
  depends_on = [azurerm_data_factory_pipeline.mid]
}

# --- schedule -------------------------------------------------------------------------------

resource "azurerm_data_factory_trigger_schedule" "daily" {
  name            = "tr_daily"
  data_factory_id = data.azurerm_data_factory.main.id
  description     = "pl_daily every day at 02:00 Dubai time"
  frequency       = "Day"
  interval        = 1
  time_zone       = "Arabian Standard Time"
  activated       = var.enable_schedule

  schedule {
    hours   = [2]
    minutes = [0]
  }

  pipeline {
    name = azurerm_data_factory_pipeline.top["pl_daily"].name
    parameters = {
      load_date = "@formatDateTime(trigger().scheduledTime, 'yyyy-MM-dd')"
    }
  }
}
