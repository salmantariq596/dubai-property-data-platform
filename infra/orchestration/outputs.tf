output "pipelines" {
  value = sort(concat(
    keys(azurerm_data_factory_pipeline.leaf),
    keys(azurerm_data_factory_pipeline.mid),
    keys(azurerm_data_factory_pipeline.top),
  ))
}

output "daily_trigger_active" {
  value = azurerm_data_factory_trigger_schedule.daily.activated
}
