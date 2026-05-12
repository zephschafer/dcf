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

resource "google_dataflow_flex_template_job" "pipeline" {
  name                    = "pvc-stream-${replace(var.pipeline_name, "_", "-")}"
  container_spec_gcs_path = var.template_gcs_path
  region                  = var.region
  on_delete               = "drain"
  service_account_email   = var.sa_email
  temp_gcs_location       = "gs://${var.warehouse_bucket}/dataflow-temp/${var.pipeline_name}"

  parameters = {
    pipeline_name    = var.pipeline_name
    subscription     = var.subscription
    warehouse_bucket = var.warehouse_bucket
    window_seconds   = tostring(var.window_seconds)
  }

  lifecycle {
    # Prevents destroy-recreate when non-structural params change on re-deploy
    ignore_changes = [parameters["num_workers"], parameters["max_workers"]]
  }
}
