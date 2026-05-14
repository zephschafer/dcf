variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region (e.g. us-central1)"
  type        = string
}

variable "sa_email" {
  description = "Service account email that gets objectAdmin on the warehouse bucket"
  type        = string
}
