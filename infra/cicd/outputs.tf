# Set these as GitHub repository variables (not secrets: none of them is secret).
# See docs/cicd.md.

output "AZURE_CLIENT_ID" {
  value = azuread_application.github.client_id
}

output "AZURE_TENANT_ID" {
  value = data.azuread_client_config.current.tenant_id
}

output "AZURE_SUBSCRIPTION_ID" {
  value = var.subscription_id
}

output "DATABRICKS_HOST" {
  value = "https://${local.platform.databricks_workspace_url}"
}
