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
    command = "n=0; until gcloud builds submit --project ${var.project_id} --region ${var.region} --tag ${var.image_uri} --timeout 600s ${var.build_context}; do n=$((n+1)); if [ $n -ge 6 ]; then exit 1; fi; echo \"Cloud Build not ready, retrying in 15s (attempt $n/6)...\"; sleep 15; done"
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

  # Airflow standalone creates many owned objects; DROP USER fails if we try to
  # delete the role explicitly. ABANDON removes it from state and lets the
  # Cloud SQL instance deletion cascade-drop it instead.
  deletion_policy = "ABANDON"
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

resource "google_storage_bucket_iam_member" "warehouse_writer" {
  bucket = var.warehouse_bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_project_iam_member" "run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${var.sa_email}"
}

resource "google_cloud_run_v2_service_iam_member" "airflow_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.airflow.name
  role     = "roles/run.invoker"
  member   = "allAuthenticatedUsers"
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
        bucket    = var.dags_bucket
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
        initial_delay_seconds = 10
        period_seconds        = 10
        failure_threshold     = 54
        timeout_seconds       = 5
        tcp_socket {
          port = 8080
        }
      }

      # Wait for Cloud SQL socket, init schema, create admin with the password
      # from env vars (the Docker entrypoint is bypassed by this custom command,
      # so _AIRFLOW_WWW_USER_* vars are not processed automatically).
      command = ["/bin/bash", "-c"]
      args    = ["for i in $(seq 1 60); do airflow db check 2>&1 && break; echo \"Waiting for DB (attempt $i)...\"; sleep 5; done; airflow db migrate; airflow users create --username admin --firstname Admin --lastname Admin --role Admin --email admin@airflow.local --password \"$_AIRFLOW_WWW_USER_PASSWORD\" 2>&1 || true; exec airflow standalone"]
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
    ]
  }
}
