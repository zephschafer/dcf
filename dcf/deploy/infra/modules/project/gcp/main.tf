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

locals {
  deploy_airflow = length(var.collectors) > 0
  sa_email       = google_service_account.dcf_lake.email
  db_conn_name   = local.deploy_airflow ? google_sql_database_instance.airflow_db[0].connection_name : ""
  db_url         = "postgresql+psycopg2://airflow:${var.db_password}@/airflow?host=/cloudsql/${local.db_conn_name}"
}

# ================================================================== #
# Lake: APIs                                                           #
# ================================================================== #

# Declare all required APIs so Terraform waits for full propagation
# before creating dependent resources. On established projects these
# are already enabled; Terraform records them in state idempotently.

resource "google_project_service" "iam" {
  project                    = var.project_id
  service                    = "iam.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "secretmanager" {
  project                    = var.project_id
  service                    = "secretmanager.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "storage" {
  project                    = var.project_id
  service                    = "storage.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "artifactregistry" {
  project                    = var.project_id
  service                    = "artifactregistry.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "run" {
  project                    = var.project_id
  service                    = "run.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "sqladmin" {
  project                    = var.project_id
  service                    = "sqladmin.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "cloudbuild" {
  project                    = var.project_id
  service                    = "cloudbuild.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

# ================================================================== #
# Lake: core resources                                                 #
# ================================================================== #

resource "google_service_account" "dcf_lake" {
  depends_on   = [google_project_service.iam]
  account_id   = "dcf-lake"
  display_name = "dcf Lake Service Account"
  project      = var.project_id
}

resource "google_service_account_key" "dcf_lake" {
  service_account_id = google_service_account.dcf_lake.name
}

resource "google_secret_manager_secret" "sa_key" {
  depends_on = [google_project_service.secretmanager]
  project    = var.project_id
  secret_id  = "dcf-lake-sa-key"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "sa_key" {
  secret      = google_secret_manager_secret.sa_key.id
  secret_data = base64decode(google_service_account_key.dcf_lake.private_key)
}

resource "google_storage_bucket" "warehouse" {
  depends_on                  = [google_project_service.storage]
  name                        = "dcf-warehouse-${var.project_id}"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "dags" {
  depends_on                  = [google_project_service.storage]
  name                        = "dcf-dags-${var.project_id}"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
}

resource "google_artifact_registry_repository" "dcf_runner" {
  depends_on    = [google_project_service.artifactregistry]
  project       = var.project_id
  location      = var.region
  repository_id = "dcf-runner"
  description   = "DCF collector container images"
  format        = "DOCKER"
}

# ================================================================== #
# Lake: IAM for the service account                                    #
# ================================================================== #

resource "google_project_iam_member" "cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_storage_bucket_iam_member" "warehouse_writer" {
  bucket = google_storage_bucket.warehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${local.sa_email}"
}

# ================================================================== #
# Collectors: Cloud Build + Cloud Run jobs                             #
# ================================================================== #

resource "local_file" "collector_dockerfile" {
  for_each = var.collectors

  content  = templatefile("${path.module}/templates/batch_collector.Dockerfile.tftpl", {
    java_enabled = each.value.java_enabled
  })
  filename = "${each.value.build_context}/Dockerfile"
}

resource "null_resource" "collector_build" {
  for_each   = var.collectors
  depends_on = [local_file.collector_dockerfile, google_artifact_registry_repository.dcf_runner]

  triggers = {
    content_hash = each.value.content_hash
    java_enabled = tostring(each.value.java_enabled)
  }

  provisioner "local-exec" {
    command = "n=0; until gcloud builds submit --project ${var.project_id} --region ${var.region} --tag ${each.value.image_uri} --timeout 600s ${each.value.build_context}; do n=$((n+1)); if [ $n -ge 6 ]; then exit 1; fi; echo \"Cloud Build not ready, retrying in 15s (attempt $n/6)...\"; sleep 15; done"
  }
}

resource "google_cloud_run_v2_job" "collector" {
  for_each   = var.collectors
  depends_on = [null_resource.collector_build]

  name     = "dcf-job-${replace(each.key, "_", "-")}"
  location = var.region

  template {
    template {
      service_account = local.sa_email
      max_retries     = 0

      containers {
        image = each.value.image_uri

        env {
          name  = "COLLECTOR_NAME"
          value = each.key
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

# ================================================================== #
# Airflow: conditional on collectors being non-empty                   #
# ================================================================== #

resource "local_file" "airflow_dockerfile" {
  count = local.deploy_airflow ? 1 : 0

  content  = templatefile("${path.module}/templates/airflow.Dockerfile.tftpl", {
    target = "gcp"
  })
  filename = "${var.airflow_build_context}/Dockerfile"
}

resource "null_resource" "airflow_build" {
  count      = local.deploy_airflow ? 1 : 0
  depends_on = [local_file.airflow_dockerfile, google_artifact_registry_repository.dcf_runner]

  triggers = {
    content_hash = var.airflow_content_hash
  }

  provisioner "local-exec" {
    command = "n=0; until gcloud builds submit --project ${var.project_id} --region ${var.region} --tag ${var.airflow_image_uri} --timeout 600s ${var.airflow_build_context}; do n=$((n+1)); if [ $n -ge 6 ]; then exit 1; fi; echo \"Cloud Build not ready, retrying in 15s (attempt $n/6)...\"; sleep 15; done"
  }
}

resource "google_sql_database_instance" "airflow_db" {
  count            = local.deploy_airflow ? 1 : 0
  depends_on       = [google_project_service.sqladmin]
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
  count    = local.deploy_airflow ? 1 : 0
  name     = "airflow"
  instance = google_sql_database_instance.airflow_db[0].name
}

resource "google_sql_user" "airflow" {
  count    = local.deploy_airflow ? 1 : 0
  name     = "airflow"
  instance = google_sql_database_instance.airflow_db[0].name
  password = var.db_password

  # Airflow standalone creates many owned objects; DROP USER fails if we try to
  # delete the role explicitly. ABANDON removes it from state and lets the
  # Cloud SQL instance deletion cascade-drop it instead.
  deletion_policy = "ABANDON"
}

resource "google_cloud_run_v2_service" "airflow" {
  count      = local.deploy_airflow ? 1 : 0
  depends_on = [null_resource.airflow_build, google_sql_database_instance.airflow_db, google_project_service.run]

  name     = "dcf-airflow"
  location = var.region

  template {
    service_account = local.sa_email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    volumes {
      name = "dags"
      gcs {
        bucket    = google_storage_bucket.dags.name
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
      image = var.airflow_image_uri

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

resource "google_cloud_run_v2_service_iam_member" "airflow_invoker" {
  count    = local.deploy_airflow ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.airflow[0].name
  role     = "roles/run.invoker"
  member   = "allAuthenticatedUsers"
}
