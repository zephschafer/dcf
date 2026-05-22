variable "image_uri" {
  type        = string
  description = "Artifact Registry URI for the Airflow image"
}

variable "build_context" {
  type        = string
  description = "Absolute host path to the Airflow build context directory"
}

variable "content_hash" {
  type        = string
  description = "SHA256 of Airflow Dockerfile template — triggers Cloud Build rebuild"
}

variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
}

variable "sa_email" {
  type        = string
  description = "Service account email for Cloud Run Airflow service"
}

variable "warehouse_bucket" {
  type        = string
  description = "GCS data warehouse bucket (used for IAM grants)"
}

variable "dags_bucket" {
  type        = string
  description = "GCS bucket dedicated to Airflow DAGs (mounted at /opt/airflow/dags)"
}

variable "db_password" {
  type        = string
  sensitive   = true
  description = "PostgreSQL password for Cloud SQL Airflow database"
}

variable "admin_password" {
  type        = string
  sensitive   = true
  description = "Airflow webserver admin password"
}

variable "fernet_key" {
  type        = string
  sensitive   = true
  description = "Airflow fernet key for encrypting connection passwords"
}
