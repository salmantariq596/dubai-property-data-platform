# Read by the unity-catalog stack through terraform_remote_state.

output "environment" {
  value = var.environment
}

output "resource_group" {
  value = azurerm_resource_group.main.name
}

output "lake_storage_account" {
  value = azurerm_storage_account.lake.name
}

output "lake_containers" {
  value = sort([for c in azurerm_storage_container.layer : c.name])
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.main.id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.main.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}
