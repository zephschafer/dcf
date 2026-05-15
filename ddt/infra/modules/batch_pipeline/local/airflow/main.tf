terraform {
  required_version = ">= 1.0"
  required_providers {
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

resource "local_file" "dockerfile" {
  content  = templatefile("${path.module}/templates/airflow.Dockerfile.tftpl", {
    target = "local"
  })
  filename = "${var.build_context}/Dockerfile"
}

resource "null_resource" "build" {
  depends_on = [local_file.dockerfile]

  triggers = {
    content_hash = var.content_hash
  }

  provisioner "local-exec" {
    command = "docker build -t ${var.image_tag} ${var.build_context}"
  }
}

resource "local_file" "compose" {
  content = templatefile("${path.module}/templates/docker-compose.yml.tftpl", {
    image_tag      = var.image_tag
    dag_dir        = var.dag_dir
    docker_socket  = var.docker_socket
    db_password    = var.db_password
    admin_password = var.admin_password
    fernet_key     = var.fernet_key
    webserver_port = var.webserver_port
  })
  filename = var.compose_file_path
}

resource "null_resource" "up" {
  depends_on = [local_file.compose, null_resource.build]

  triggers = {
    content_hash = var.content_hash
    compose_hash = sha256(local_file.compose.content)
  }

  provisioner "local-exec" {
    command = "docker compose -f ${var.compose_file_path} up -d"
  }

  provisioner "local-exec" {
    when    = destroy
    command = "docker compose -f ${var.compose_file_path} down --volumes"
  }
}
