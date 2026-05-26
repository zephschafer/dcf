from __future__ import annotations

import subprocess
from pathlib import Path

_DCF_SOURCE = Path(__file__).parent.parent.parent  # repo root (where Dockerfile lives)

_COMPOSE_TEMPLATE = """\
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: dcf
      POSTGRES_USER: dcf
      POSTGRES_PASSWORD: dcf
    volumes:
      - dcf_postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "dcf"]
      interval: 5s
      timeout: 3s
      retries: 5

  app:
    build: {dcf_source}
    environment:
      DATABASE_URL: postgresql://dcf:dcf@db:5432/dcf
      DCF_PROJECT_DIR: /project
      GOOGLE_APPLICATION_CREDENTIALS: /root/.config/gcloud/application_default_credentials.json
    volumes:
      - {project_root}:/project
      - {gcloud_config}:/root/.config/gcloud:ro
    ports:
      - "8080:8080"
    depends_on:
      db:
        condition: service_healthy

volumes:
  dcf_postgres:
"""


def stop_app(project_root: Path) -> None:
    compose_path = project_root / ".dcf" / "docker-compose.yml"
    if not compose_path.exists():
        raise FileNotFoundError("No .dcf/docker-compose.yml found — has dcf deploy been run?")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_path), "down"],
        check=True,
    )


def launch_app(project_root: Path) -> None:
    gcloud_config = Path.home() / ".config" / "gcloud"
    dcf_dir = project_root / ".dcf"
    compose_path = dcf_dir / "docker-compose.yml"

    compose_path.write_text(
        _COMPOSE_TEMPLATE.format(
            dcf_source=_DCF_SOURCE,
            project_root=project_root,
            gcloud_config=gcloud_config,
        )
    )

    subprocess.run(
        ["docker", "compose", "-f", str(compose_path), "up", "-d", "--build"],
        check=True,
    )
