variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
}

variable "deploy_airflow" {
  type        = bool
  default     = false
  description = "Whether to deploy the Airflow stack (Cloud SQL + Cloud Run service). Set true when any collectors are active."
}

# ---- Airflow build (only required when deploy_airflow is true) ----

variable "airflow_image_uri" {
  type        = string
  default     = ""
  description = "Artifact Registry URI for the Airflow image"
}

variable "airflow_build_context" {
  type        = string
  default     = ""
  description = "Absolute host path to the Airflow build context directory"
}

variable "airflow_content_hash" {
  type        = string
  default     = ""
  description = "SHA256 of Airflow Dockerfile template — triggers Cloud Build rebuild"
}

# ---- Airflow credentials (only required when collectors is non-empty) ----

variable "db_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "PostgreSQL password for Cloud SQL Airflow database"
}

variable "admin_password" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Airflow webserver admin password"
}

variable "fernet_key" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Airflow fernet key for encrypting connection passwords"
}
