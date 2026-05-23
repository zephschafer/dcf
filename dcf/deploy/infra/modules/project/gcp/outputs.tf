output "sa_email" {
  description = "Email of the dcf-lake service account"
  value       = google_service_account.dcf_lake.email
}

output "warehouse_bucket" {
  description = "Name of the GCS data warehouse bucket"
  value       = google_storage_bucket.warehouse.name
}

output "dags_bucket" {
  description = "Name of the GCS DAGs bucket"
  value       = google_storage_bucket.dags.name
}

output "secret_name" {
  description = "Full resource name of the SA key Secret Manager secret version"
  value       = google_secret_manager_secret_version.sa_key.name
}

output "airflow_url" {
  description = "HTTPS URL of the Airflow Cloud Run service (empty when no collectors deployed)"
  value       = local.deploy_airflow ? google_cloud_run_v2_service.airflow[0].uri : ""
}

output "job_names" {
  description = "Map of collector name to Cloud Run job name"
  value       = { for k, v in google_cloud_run_v2_job.collector : k => v.name }
}
