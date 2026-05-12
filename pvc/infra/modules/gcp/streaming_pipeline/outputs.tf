output "job_name" {
  description = "Name of the provisioned Dataflow streaming job"
  value       = google_dataflow_flex_template_job.pipeline.name
}

output "job_id" {
  description = "ID of the provisioned Dataflow streaming job"
  value       = google_dataflow_flex_template_job.pipeline.job_id
}
