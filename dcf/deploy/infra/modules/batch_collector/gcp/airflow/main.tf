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
  content  = templatefile("${path.module}/templates/airflow.Dockerfile.tftpl", {
    target = "gcp"
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

resource "google_sql_database_instance" "airflow_db" {
  name             = "dcf-airflow-db"
  database_version = "POSTGRES_15"
  region           = var.region

  deletion_protection = false

  settings {
    tier = "db-f1-micro"

    backup_configuration {
      enabled = false
    }
  }
}

resource "google_sql_database" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow_db.name
}

resource "google_sql_user" "airflow" {
  name     = "airflow"
  instance = google_sql_database_instance.airflow_db.name
  password = var.db_password
}

resource "google_project_iam_member" "cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${var.sa_email}"
}

locals {
  db_conn_name = google_sql_database_instance.airflow_db.connection_name
  db_url       = "postgresql+psycopg2://airflow:${var.db_password}@/airflow?host=/cloudsql/${local.db_conn_name}"
}

resource "google_cloud_run_v2_service" "airflow" {
  depends_on = [null_resource.build, google_sql_database_instance.airflow_db]

  name     = "dcf-airflow"
  location = var.region

  template {
    service_account = var.sa_email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    volumes {
      name = "dags"
      gcs {
        bucket    = var.warehouse_bucket
        read_only = true
      }
    }

    volumes {
      name = "cloudsql"
      cloud_sql_instance {
        instances = [local.db_conn_name]
      }
    }

    containers {
      image = var.image_uri

      ports {
        container_port = 8080
      }

      volume_mounts {
        name       = "dags"
        mount_path = "/opt/airflow/dags"
      }

      volume_mounts {
        name       = "cloudsql"
        mount_path = "/cloudsql"
      }

      env {
        name  = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        value = local.db_url
      }

      env {
        name  = "AIRFLOW__CORE__EXECUTOR"
        value = "LocalExecutor"
      }

      env {
        name  = "AIRFLOW__CORE__FERNET_KEY"
        value = var.fernet_key
      }

      env {
        name  = "AIRFLOW__WEBSERVER__SECRET_KEY"
        value = var.fernet_key
      }

      env {
        name  = "AIRFLOW__CORE__LOAD_EXAMPLES"
        value = "false"
      }

      env {
        name  = "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL"
        value = "30"
      }

      env {
        name  = "_AIRFLOW_WWW_USER_CREATE"
        value = "true"
      }

      env {
        name  = "_AIRFLOW_WWW_USER_USERNAME"
        value = "admin"
      }

      env {
        name  = "_AIRFLOW_WWW_USER_PASSWORD"
        value = var.admin_password
      }

      resources {
        limits = {
          memory = "2Gi"
          cpu    = "1"
        }
      }

      startup_probe {
        initial_delay_seconds = 30
        period_seconds        = 10
        failure_threshold     = 18
        timeout_seconds       = 5
        tcp_socket {
          port = 8080
        }
      }

      command = ["airflow", "standalone"]
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
    ]
  }
}
