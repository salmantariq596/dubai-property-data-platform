variable "subscription_id" {
  description = "Azure subscription to deploy into (az account show --query id -o tsv)."
  type        = string
}

variable "project" {
  description = "Short project code used in resource names (lowercase letters and digits)."
  type        = string
  default     = "dpdp"

  validation {
    condition     = can(regex("^[a-z0-9]{2,8}$", var.project))
    error_message = "project must be 2-8 lowercase letters or digits (it goes into storage account names)."
  }
}

variable "environment" {
  description = "Environment name, part of every resource name."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z0-9]{2,5}$", var.environment))
    error_message = "environment must be 2-5 lowercase letters or digits."
  }
}

variable "location" {
  description = "Azure region. UAE North is closest to the data and its users."
  type        = string
  default     = "uaenorth"
}
