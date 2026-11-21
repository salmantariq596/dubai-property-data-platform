# Copy to terraform.tfvars (git-ignored) and fill in.
subscription_id       = "00000000-0000-0000-0000-000000000000"
state_storage_account = "stdpdptfxxxxxx"
databricks_job_id     = "123456789" # databricks bundle summary -t azure

# Confirm the real download paths on the DLD open data portal first (docs/data_sources.md).
source_base_url = "https://www.dubailand.gov.ae/"
source_urls = {
  transactions = "REPLACE-with-transactions-download-path.csv"
  rents        = "REPLACE-with-rents-download-path.csv"
}

# alert_webhook_url = "https://..."   # Teams or Slack incoming webhook
# enable_schedule   = true            # after a manual run of pl_daily has succeeded
