output "catalog" {
  value = databricks_catalog.main.name
}

output "landing_volume_path" {
  description = "Where raw files go: <path>/<dataset>/load_date=YYYY-MM-DD/"
  value       = "/Volumes/${databricks_catalog.main.name}/raw/${databricks_volume.landing.name}"
}

output "external_locations" {
  value = { for k, v in databricks_external_location.layer : k => v.url }
}
