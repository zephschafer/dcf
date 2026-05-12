variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
}

variable "pipeline_name" {
  type        = string
  description = "pvc pipeline name (e.g. click_events)"
}

variable "template_gcs_path" {
  type        = string
  description = "GCS path to the Dataflow Flex Template spec JSON (gs://bucket/templates/pipeline.json)"
}

variable "subscription" {
  type        = string
  description = "Full Pub/Sub subscription resource path (projects/<project>/subscriptions/<name>)"
}

variable "warehouse_bucket" {
  type        = string
  description = "GCS bucket name for the warehouse output (no gs:// prefix)"
}

variable "window_seconds" {
  type        = number
  description = "Fixed-time window size in seconds for batching Parquet writes"
  default     = 60
}

variable "sa_email" {
  type        = string
  description = "Service account email for Dataflow workers"
}
