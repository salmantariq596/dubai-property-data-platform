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

variable "catalog" {
  description = "Unity Catalog catalog for the platform; the notebooks' catalog widget must match."
  type        = string
  default     = "dubai"
}
