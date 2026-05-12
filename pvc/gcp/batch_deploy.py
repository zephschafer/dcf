"""Batch pipeline deployment: builds a container image, creates a Cloud Run job,
and uploads an Airflow DAG to Cloud Composer."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import yaml

_PVC_PKG_DIR = Path(__file__).parent.parent       # pvc/ package
_PVC_REPO_ROOT = _PVC_PKG_DIR.parent              # repo root (contains pyproject.toml)


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #

def deploy(
    pipeline_name: str,
    schedule: str,
    paused: bool,
    project_root: Path,
    gcp_config: dict,
) -> dict:
    """Provision a Cloud Run job + Composer DAG for a pipeline.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]
    sa_email = gcp_config["sa_email"]

    job_name = _job_name(pipeline_name)
    dag_id = pipeline_name
    image_uri = _image_uri(project_id, region, pipeline_name)

    print(f"  Building container image '{image_uri}'...")
    print("  (First build may take a few minutes)", flush=True)
    _build_image(project_root, project_id, region, pipeline_name,
                 image_uri, warehouse_bucket)

    print(f"  Creating Cloud Run job '{job_name}'...", flush=True)
    _create_or_update_cloud_run_job(
        job_name, image_uri, project_id, region, sa_email, pipeline_name,
    )

    print("  Locating Cloud Composer environment...", flush=True)
    composer_env_name, dag_bucket = _find_composer_env(project_id, region)
    print(f"  Using Composer environment: {composer_env_name}", flush=True)

    print(f"  Uploading DAG '{dag_id}' to Composer...", flush=True)
    _upload_dag(dag_id, job_name, schedule, paused, project_id, region, dag_bucket)

    return {
        "schedule": schedule,
        "dag_id": dag_id,
        "cloud_run_job": job_name,
        "composer_env": composer_env_name,
        "image_uri": image_uri,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(pipeline_name: str, deployment: dict, gcp_config: dict) -> None:
    """Remove the Cloud Run job and Composer DAG for a deployed pipeline."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    job_name = deployment["cloud_run_job"]
    dag_id = deployment["dag_id"]
    composer_env = deployment.get("composer_env", "")

    if composer_env:
        print(f"  Removing Composer DAG '{dag_id}'...")
        try:
            _, dag_bucket = _find_composer_env(project_id, region)
            _delete_dag_file(dag_id, dag_bucket)
        except Exception as e:
            print(f"  Warning: could not remove DAG file: {e}")

    print(f"  Deleting Cloud Run job '{job_name}'...")
    try:
        _delete_cloud_run_job(job_name, project_id, region)
    except Exception as e:
        print(f"  Warning: could not delete Cloud Run job: {e}")


# ------------------------------------------------------------------ #
# Container image                                                      #
# ------------------------------------------------------------------ #

def _image_uri(project_id: str, region: str, pipeline_name: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/pvc-runner/{pipeline_name}:latest"


def _build_image(
    project_root: Path,
    project_id: str,
    region: str,
    pipeline_name: str,
    image_uri: str,
    warehouse_bucket: str,
) -> None:
    """Build a Docker image using Cloud Build and push to Artifact Registry."""
    _ensure_artifact_registry_repo(project_id, region)

    with tempfile.TemporaryDirectory(prefix="pvc-build-") as tmp:
        tmp_path = Path(tmp)

        # Vendor pvc source into the build context
        shutil.copytree(_PVC_PKG_DIR, tmp_path / "pvc")
        shutil.copy2(_PVC_REPO_ROOT / "pyproject.toml", tmp_path / "pyproject.toml")

        # Copy pipeline and connector files from the user's project
        for subdir in ("pipelines", "connectors"):
            src = project_root / subdir
            if src.exists():
                shutil.copytree(src, tmp_path / subdir)
            else:
                (tmp_path / subdir).mkdir()

        # Generate a minimal project.yml — no secrets, GCP auth comes from SA
        minimal_config = {
            "catalog": "gcp",
            "gcp": {
                "project_id": project_id,
                "region": region,
                "warehouse_bucket": warehouse_bucket,
            },
        }
        (tmp_path / "project.yml").write_text(
            yaml.dump(minimal_config, default_flow_style=False)
        )

        (tmp_path / "Dockerfile").write_text(dedent("""\
            FROM python:3.12-slim
            WORKDIR /app
            COPY pyproject.toml .
            COPY pvc/ ./pvc/
            RUN pip install --no-cache-dir -e .
            COPY pipelines/ ./pipelines/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENV PIPELINE_NAME=""
            CMD ["sh", "-c", "pvc run $PIPELINE_NAME"]
        """))

        result = subprocess.run(
            [
                "gcloud", "builds", "submit",
                "--project", project_id,
                "--region", region,
                "--tag", image_uri,
                "--timeout", "600s",
                ".",
            ],
            cwd=tmp,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "Cloud Build failed. Ensure the API is enabled:\n"
                "  gcloud services enable cloudbuild.googleapis.com"
            )


def _ensure_artifact_registry_repo(project_id: str, region: str) -> None:
    check = subprocess.run(
        [
            "gcloud", "artifacts", "repositories", "describe", "pvc-runner",
            "--location", region, "--project", project_id,
        ],
        capture_output=True,
    )
    if check.returncode != 0:
        result = subprocess.run(
            [
                "gcloud", "artifacts", "repositories", "create", "pvc-runner",
                "--repository-format=docker",
                "--location", region,
                "--project", project_id,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to create Artifact Registry repository: {result.stderr}\n"
                "Ensure the API is enabled:\n"
                "  gcloud services enable artifactregistry.googleapis.com"
            )


# ------------------------------------------------------------------ #
# Cloud Run job                                                        #
# ------------------------------------------------------------------ #

def _job_name(pipeline_name: str) -> str:
    return f"pvc-job-{pipeline_name.replace('_', '-')}"


def _create_or_update_cloud_run_job(
    job_name: str,
    image_uri: str,
    project_id: str,
    region: str,
    sa_email: str,
    pipeline_name: str,
) -> None:
    check = subprocess.run(
        [
            "gcloud", "run", "jobs", "describe", job_name,
            "--region", region, "--project", project_id,
        ],
        capture_output=True,
    )
    verb = "update" if check.returncode == 0 else "create"

    cmd = [
        "gcloud", "run", "jobs", verb, job_name,
        "--image", image_uri,
        "--region", region,
        "--project", project_id,
        "--service-account", sa_email,
        "--set-env-vars", f"PIPELINE_NAME={pipeline_name}",
        "--max-retries", "0",
        "--memory", "512Mi",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to {verb} Cloud Run job '{job_name}': {result.stderr}\n"
            "Ensure the API is enabled:\n"
            "  gcloud services enable run.googleapis.com"
        )


def _delete_cloud_run_job(job_name: str, project_id: str, region: str) -> None:
    result = subprocess.run(
        [
            "gcloud", "run", "jobs", "delete", job_name,
            "--region", region, "--project", project_id, "--quiet",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)


# ------------------------------------------------------------------ #
# Cloud Composer DAG                                                   #
# ------------------------------------------------------------------ #

def _find_composer_env(project_id: str, region: str) -> tuple[str, str]:
    """Return (env_name, dag_gcs_prefix) for the first Composer environment found."""
    result = subprocess.run(
        [
            "gcloud", "composer", "environments", "list",
            "--locations", region, "--project", project_id,
            "--format", "json",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to list Composer environments: {result.stderr}\n"
            "Ensure the API is enabled:\n"
            "  gcloud services enable composer.googleapis.com\n"
            "And that a Composer environment exists in the project."
        )

    import json
    envs = json.loads(result.stdout)
    if not envs:
        raise RuntimeError(
            f"No Cloud Composer environments found in {project_id}/{region}.\n"
            "Create one first (takes 15–30 min):\n"
            f"  gcloud composer environments create pvc-composer \\\n"
            f"    --location {region} --project {project_id} \\\n"
            f"    --environment-size=small \\\n"
            f"    --service-account <your-sa-email>\n"
            "Then re-run: pvc deploy <pipeline-name>"
        )

    env = envs[0]
    env_name = env["name"].split("/")[-1]
    dag_bucket = env["config"]["dagGcsPrefix"]
    return env_name, dag_bucket


def _dag_content(dag_id: str, job_name: str, schedule: str, paused: bool,
                  project_id: str, region: str) -> str:
    paused_str = "True" if paused else "False"
    return dedent(f"""\
        # Generated by pvc — do not edit manually
        from datetime import datetime
        from airflow import DAG
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunJobOperator

        with DAG(
            dag_id="{dag_id}",
            schedule_interval="{schedule}",
            start_date=datetime(2024, 1, 1),
            catchup=False,
            is_paused_upon_creation={paused_str},
            tags=["pvc"],
        ) as dag:
            run_job = CloudRunJobOperator(
                task_id="run_{dag_id}",
                project_id="{project_id}",
                region="{region}",
                job_name="{job_name}",
            )
    """)


def _upload_dag(
    dag_id: str,
    job_name: str,
    schedule: str,
    paused: bool,
    project_id: str,
    region: str,
    dag_gcs_prefix: str,
) -> None:
    from google.cloud import storage

    dag_py = _dag_content(dag_id, job_name, schedule, paused, project_id, region)

    # dag_gcs_prefix is like "gs://bucket/dags" — strip the gs:// prefix
    prefix = dag_gcs_prefix.removeprefix("gs://")
    bucket_name, _, blob_prefix = prefix.partition("/")
    blob_name = f"{blob_prefix}/{dag_id}.py" if blob_prefix else f"dags/{dag_id}.py"

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).upload_from_string(dag_py, content_type="text/plain")


def _delete_dag_file(dag_id: str, dag_gcs_prefix: str) -> None:
    from google.cloud import storage

    prefix = dag_gcs_prefix.removeprefix("gs://")
    bucket_name, _, blob_prefix = prefix.partition("/")
    blob_name = f"{blob_prefix}/{dag_id}.py" if blob_prefix else f"dags/{dag_id}.py"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()
