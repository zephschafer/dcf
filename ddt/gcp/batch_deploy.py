"""Batch pipeline deployment: builds a container image via Cloud Build, then uses
Terraform to provision a Cloud Run job and upload the Airflow DAG to Composer."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import yaml

logger = logging.getLogger(__name__)

_DDT_PKG_DIR = Path(__file__).parent.parent          # ddt/ package
_DDT_REPO_ROOT = _DDT_PKG_DIR.parent                 # repo root (contains pyproject.toml)
_BATCH_MODULE_DIR = _DDT_PKG_DIR / "infra" / "modules" / "gcp" / "batch_pipeline"
_PIPELINE_TF_DIR = Path.home() / ".ddt" / "terraform" / "pipelines"
_TF_PLUGIN_CACHE = Path.home() / ".ddt" / "terraform" / ".plugin-cache"


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
    """Provision a Cloud Run job + Composer DAG for a pipeline via Terraform.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]
    sa_email = gcp_config["sa_email"]

    dag_id = pipeline_name
    image_uri = _image_uri(project_id, region, pipeline_name)

    print(f"  Building container image '{image_uri}'...")
    print("  (First build may take a few minutes)", flush=True)
    _build_image(project_root, project_id, region, pipeline_name,
                 image_uri, warehouse_bucket)

    print("  Locating Cloud Composer environment...", flush=True)
    composer_env_name, dag_gcs_prefix = _find_or_create_composer_env(
        project_id, region, sa_email
    )
    print(f"  Using Composer environment: {composer_env_name}", flush=True)

    dag_bucket, dag_blob_name = _parse_dag_path(dag_gcs_prefix, pipeline_name)
    dag_py = _dag_content(
        dag_id=dag_id,
        job_name=_expected_job_name(pipeline_name),
        schedule=schedule,
        paused=paused,
        project_id=project_id,
        region=region,
    )

    print("  Applying Terraform (Cloud Run job + DAG)...", flush=True)
    job_name = _terraform_apply_pipeline(
        pipeline_name=pipeline_name,
        image_uri=image_uri,
        sa_email=sa_email,
        dag_bucket=dag_bucket,
        dag_blob_name=dag_blob_name,
        dag_content=dag_py,
        project_id=project_id,
        region=region,
    )

    return {
        "schedule": schedule,
        "dag_id": dag_id,
        "cloud_run_job": job_name,
        "composer_env": composer_env_name,
        "image_uri": image_uri,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(pipeline_name: str, deployment: dict, gcp_config: dict) -> None:
    """Remove the Cloud Run job and Composer DAG via Terraform destroy."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]

    print(f"  Destroying Terraform resources for '{pipeline_name}'...", flush=True)
    _terraform_destroy_pipeline(pipeline_name, project_id, region)


# ------------------------------------------------------------------ #
# Container image                                                      #
# ------------------------------------------------------------------ #

def _image_uri(project_id: str, region: str, pipeline_name: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/ddt-runner/{pipeline_name}:latest"


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

    with tempfile.TemporaryDirectory(prefix="ddt-build-") as tmp:
        tmp_path = Path(tmp)

        shutil.copytree(_DDT_PKG_DIR, tmp_path / "ddt")
        shutil.copy2(_DDT_REPO_ROOT / "pyproject.toml", tmp_path / "pyproject.toml")

        for subdir in ("pipelines", "connectors"):
            src = project_root / subdir
            if src.exists():
                shutil.copytree(src, tmp_path / subdir)
            else:
                (tmp_path / subdir).mkdir()

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
            COPY ddt/ ./ddt/
            RUN pip install --no-cache-dir -e .
            COPY pipelines/ ./pipelines/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENV PIPELINE_NAME=""
            CMD ["sh", "-c", "ddt run $PIPELINE_NAME"]
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
            "gcloud", "artifacts", "repositories", "describe", "ddt-runner",
            "--location", region, "--project", project_id,
        ],
        capture_output=True,
    )
    if check.returncode != 0:
        result = subprocess.run(
            [
                "gcloud", "artifacts", "repositories", "create", "ddt-runner",
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
# Terraform: per-pipeline resources                                    #
# ------------------------------------------------------------------ #

def _expected_job_name(pipeline_name: str) -> str:
    return f"ddt-job-{pipeline_name.replace('_', '-')}"


def _tf_work_dir(pipeline_name: str) -> Path:
    return _PIPELINE_TF_DIR / pipeline_name


def _tf_env() -> dict:
    return {
        **os.environ,
        "TF_INPUT": "0",
        "TF_PLUGIN_CACHE_DIR": str(_TF_PLUGIN_CACHE),
    }


def _terraform_apply_pipeline(
    pipeline_name: str,
    image_uri: str,
    sa_email: str,
    dag_bucket: str,
    dag_blob_name: str,
    dag_content: str,
    project_id: str,
    region: str,
) -> str:
    """Provision Cloud Run job + DAG file via Terraform. Returns the job name."""
    work_dir = _tf_work_dir(pipeline_name)
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    for tf_file in _BATCH_MODULE_DIR.glob("*.tf"):
        shutil.copy2(tf_file, work_dir / tf_file.name)

    tfvars = {
        "project_id": project_id,
        "region": region,
        "pipeline_name": pipeline_name,
        "image_uri": image_uri,
        "sa_email": sa_email,
        "dag_bucket": dag_bucket,
        "dag_blob_name": dag_blob_name,
        "dag_content": dag_content,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()

    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)

    _import_existing_cloud_run_job(pipeline_name, project_id, region, work_dir, env)

    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    outputs = json.loads(
        subprocess.run(
            ["terraform", "output", "-json"],
            cwd=str(work_dir), env=env, capture_output=True, text=True,
        ).stdout
    )
    return outputs["job_name"]["value"]


def _terraform_destroy_pipeline(
    pipeline_name: str,
    project_id: str,
    region: str,
) -> None:
    """Destroy Cloud Run job + DAG file via Terraform, then remove the state dir."""
    work_dir = _tf_work_dir(pipeline_name)
    if not work_dir.exists():
        raise RuntimeError(
            f"No Terraform state found for pipeline '{pipeline_name}' "
            f"at {work_dir}.\n"
            "If you deployed from a different machine, delete the Cloud Run job and "
            "DAG file manually:\n"
            f"  gcloud run jobs delete ddt-job-{pipeline_name.replace('_', '-')} "
            f"--region {region} --project {project_id} --quiet"
        )

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)

    shutil.rmtree(work_dir)


def _import_existing_cloud_run_job(
    pipeline_name: str,
    project_id: str,
    region: str,
    work_dir: Path,
    env: dict,
) -> None:
    """Import an existing Cloud Run job into Terraform state to avoid 409 on apply."""
    job_name = _expected_job_name(pipeline_name)
    check = subprocess.run(
        ["gcloud", "run", "jobs", "describe", job_name,
         "--region", region, "--project", project_id],
        capture_output=True,
    )
    if check.returncode != 0:
        return  # job doesn't exist yet

    resource_id = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
    result = subprocess.run(
        ["terraform", "import", "google_cloud_run_v2_job.pipeline", resource_id],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Imported existing Cloud Run job '%s' into Terraform state", job_name)
    elif "already managed by Terraform" in result.stdout + result.stderr:
        logger.info("Cloud Run job '%s' already in Terraform state", job_name)
    else:
        logger.warning("terraform import returned non-zero: %s", result.stderr[-500:])


def _tf_run(cmd: list[str], work_dir: Path, env: dict) -> None:
    result = subprocess.run(
        cmd, cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error(
            "Terraform command failed: %s\nSTDOUT: %s\nSTDERR: %s",
            " ".join(cmd), result.stdout, result.stderr,
        )
        raise RuntimeError(
            f"terraform {cmd[1]} failed (exit {result.returncode}): {result.stderr[-2000:]}"
        )
    logger.info("terraform %s OK", cmd[1])


# ------------------------------------------------------------------ #
# Cloud Composer environment                                           #
# ------------------------------------------------------------------ #

def _parse_dag_path(dag_gcs_prefix: str, pipeline_name: str) -> tuple[str, str]:
    """Split 'gs://bucket/dags' into (bucket_name, 'dags/<pipeline>.py')."""
    prefix = dag_gcs_prefix.removeprefix("gs://")
    bucket_name, _, blob_prefix = prefix.partition("/")
    blob_name = f"{blob_prefix}/{pipeline_name}.py" if blob_prefix else f"dags/{pipeline_name}.py"
    return bucket_name, blob_name


def _find_or_create_composer_env(
    project_id: str, region: str, sa_email: str
) -> tuple[str, str]:
    """Return (env_name, dag_gcs_prefix) for a RUNNING Composer environment.

    If none exists, creates 'ddt-composer' and polls until RUNNING.
    """
    import time

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
            "  gcloud services enable composer.googleapis.com"
        )

    envs = json.loads(result.stdout)
    if envs:
        env = envs[0]
        env_name = env["name"].split("/")[-1]
        dag_bucket = env["config"]["dagGcsPrefix"]
        return env_name, dag_bucket

    env_name = "ddt-composer"
    print(
        f"\n  No Cloud Composer environment found in {project_id}/{region}.\n"
        f"  Provisioning '{env_name}' (this takes 20–30 minutes)...",
        flush=True,
    )
    create_result = subprocess.run(
        [
            "gcloud", "composer", "environments", "create", env_name,
            "--location", region,
            "--project", project_id,
            "--environment-size", "small",
            "--service-account", sa_email,
            "--async",
        ],
        capture_output=True, text=True,
    )
    if create_result.returncode != 0:
        raise RuntimeError(
            f"Failed to create Composer environment: {create_result.stderr}\n"
            "Ensure the API is enabled and the service account has roles/composer.worker:\n"
            "  gcloud services enable composer.googleapis.com\n"
            f"  gcloud projects add-iam-policy-binding {project_id} \\\n"
            f"    --member=serviceAccount:{sa_email} --role=roles/composer.worker"
        )

    poll_interval = 30
    timeout_secs = 40 * 60
    elapsed = 0
    dots = 0
    while elapsed < timeout_secs:
        time.sleep(poll_interval)
        elapsed += poll_interval
        dots += 1
        print(f"  Still provisioning{'.' * (dots % 4 + 1)} ({elapsed // 60}m elapsed)", flush=True)

        state_result = subprocess.run(
            [
                "gcloud", "composer", "environments", "describe", env_name,
                "--location", region, "--project", project_id,
                "--format", "json",
            ],
            capture_output=True, text=True,
        )
        if state_result.returncode != 0:
            continue

        env_data = json.loads(state_result.stdout)
        state = env_data.get("state", "")
        if state == "RUNNING":
            dag_bucket = env_data["config"]["dagGcsPrefix"]
            print(f"  Composer environment '{env_name}' is ready.", flush=True)
            return env_name, dag_bucket
        if state == "ERROR":
            raise RuntimeError(
                f"Composer environment '{env_name}' entered ERROR state.\n"
                "Check the GCP console for details:\n"
                f"  https://console.cloud.google.com/composer/environments?project={project_id}"
            )

    raise RuntimeError(
        f"Composer environment '{env_name}' did not reach RUNNING state within 40 minutes.\n"
        f"  https://console.cloud.google.com/composer/environments?project={project_id}"
    )


# ------------------------------------------------------------------ #
# DAG content                                                          #
# ------------------------------------------------------------------ #

def _dag_content(dag_id: str, job_name: str, schedule: str, paused: bool,
                  project_id: str, region: str) -> str:
    paused_str = "True" if paused else "False"
    return dedent(f"""\
        # Generated by ddt — do not edit manually
        from datetime import datetime
        from airflow import DAG
        from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

        with DAG(
            dag_id="{dag_id}",
            schedule_interval="{schedule}",
            start_date=datetime(2024, 1, 1),
            catchup=False,
            is_paused_upon_creation={paused_str},
            tags=["ddt"],
        ) as dag:
            run_job = CloudRunExecuteJobOperator(
                task_id="run_{dag_id}",
                project_id="{project_id}",
                region="{region}",
                job_name="{job_name}",
            )
    """)
