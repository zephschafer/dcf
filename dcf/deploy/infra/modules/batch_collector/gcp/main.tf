terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "local_file" "dockerfile" {
  content  = templatefile("${path.module}/templates/batch_collector.Dockerfile.tftpl", {
    java_enabled = var.java_enabled
  })
  filename = "${var.build_context}/Dockerfile"
}

resource "null_resource" "build" {
  depends_on = [local_file.dockerfile]

  triggers = {
    content_hash = var.content_hash
  }

  provisioner "local-exec" {
    command = "gcloud builds submit --project ${var.project_id} --region ${var.region} --tag ${var.image_uri} --timeout 600s ${var.build_context}"
  }
}

resource "google_cloud_run_v2_job" "collector" {
  depends_on = [null_resource.build]

  name     = "dcf-job-${replace(var.collector_name, "_", "-")}"
  location = var.region

  template {
    template {
      service_account = var.sa_email
      max_retries     = 0

      containers {
        image = var.image_uri

        env {
          name  = "COLLECTOR_NAME"
          value = var.collector_name
        }

        env {
          name  = "DCF_PROJECT_DIR"
          value = "/app"
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
