terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_cloud_run_v2_job" "pipeline" {
  name     = "pvc-job-${replace(var.pipeline_name, "_", "-")}"
  location = var.region

  template {
    template {
      service_account = var.sa_email
      max_retries     = 0

      containers {
        image = var.image_uri

        env {
          name  = "PIPELINE_NAME"
          value = var.pipeline_name
        }

        resources {
          limits = {
            memory = "512Mi"
          }
        }
      }
    }
  }
}

resource "google_storage_bucket_object" "dag" {
  bucket  = var.dag_bucket
  name    = var.dag_blob_name
  content = var.dag_content
}
