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
  description = "pvc pipeline name (e.g. github_repos)"
}

variable "image_uri" {
  type        = string
  description = "Container image URI for the Cloud Run job"
}

variable "sa_email" {
  type        = string
  description = "Service account email for the Cloud Run job"
}

variable "dag_bucket" {
  type        = string
  description = "GCS bucket name for the Composer DAGs (no gs:// prefix)"
}

variable "dag_blob_name" {
  type        = string
  description = "Full blob path for the DAG file within the bucket (e.g. dags/github_repos.py)"
}

variable "dag_content" {
  type        = string
  description = "Full Python content of the generated Airflow DAG file"
}
