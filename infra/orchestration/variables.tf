variable "subscription_id" {
  description = "Azure subscription (az account show --query id -o tsv)."
  type        = string
}

variable "state_resource_group" {
  description = "Resource group of the Terraform state storage (bootstrap output)."
  type        = string
  default     = "rg-dpdp-tfstate"
}

variable "state_storage_account" {
  description = "Storage account holding the Terraform state (bootstrap output)."
  type        = string
}

variable "state_container" {
  description = "Container holding the Terraform state."
  type        = string
  default     = "tfstate"
}

variable "databricks_job_id" {
  description = "Id of the dpdp-daily-pipeline job: databricks bundle summary -t azure."
  type        = string

  validation {
    condition     = can(regex("^[0-9]+$", var.databricks_job_id))
    error_message = "databricks_job_id is the numeric job id."
  }
}

variable "source_base_url" {
  description = "Base URL of the DLD open data downloads. Confirm on the portal and record it in docs/data_sources.md."
  type        = string
  default     = "https://www.dubailand.gov.ae/"
}

variable "source_urls" {
  description = <<-EOT
    Path of each dataset's file under source_base_url. "{load_date}" is replaced by the
    load date (yyyy-MM-dd), for sources that publish one file per day.
  EOT
  type        = map(string)
}

variable "alert_webhook_url" {
  description = "Teams or Slack incoming webhook for failure alerts. Empty: no webhook (the run still fails visibly)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "catalog" {
  description = "Unity Catalog catalog passed to the Databricks job."
  type        = string
  default     = "dubai"
}

variable "enable_schedule" {
  description = "Start the daily trigger. Off until a manual run of pl_daily has succeeded."
  type        = bool
  default     = false
}
