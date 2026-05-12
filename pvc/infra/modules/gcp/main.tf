terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "warehouse" {
  name                        = "pvc-warehouse-${var.project_id}"
  location                    = var.region
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_member" "warehouse_sa" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${var.sa_email}"
}

output "warehouse_bucket" {
  value = google_storage_bucket.warehouse.name
}
