# Copy to backend.hcl (git-ignored), with the values from: terraform -chdir=infra/bootstrap output backend_config
resource_group_name  = "rg-dpdp-tfstate"
storage_account_name = "stdpdptfxxxxxx"
container_name       = "tfstate"
key                  = "unity-catalog.tfstate"
use_azuread_auth     = true
