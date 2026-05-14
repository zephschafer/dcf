output "job_name" {
  description = "Name of the provisioned Cloud Run job"
  value       = google_cloud_run_v2_job.pipeline.name
}

output "dag_blob_name" {
  description = "Full GCS blob path of the uploaded DAG file"
  value       = google_storage_bucket_object.dag.name
}
