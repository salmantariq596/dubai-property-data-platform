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

variable "github_repository" {
  description = "owner/name of the GitHub repository allowed to deploy."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9-]+/[A-Za-z0-9._-]+$", var.github_repository))
    error_message = "github_repository must look like owner/name."
  }
}

variable "catalog" {
  description = "Unity Catalog catalog the deployed jobs write to."
  type        = string
  default     = "dubai"
}
