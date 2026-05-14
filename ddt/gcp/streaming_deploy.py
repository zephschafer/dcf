"""Streaming pipeline deployment: builds a container image via Cloud Build,
uploads a Dataflow Flex Template spec to GCS, then provisions the Dataflow
streaming job via Terraform."""

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

_DDT_PKG_DIR = Path(__file__).parent.parent
_DDT_REPO_ROOT = _DDT_PKG_DIR.parent
_STREAMING_MODULE_DIR = _DDT_PKG_DIR / "infra" / "modules" / "gcp" / "streaming_pipeline"
_PIPELINE_TF_DIR = Path.home() / ".ddt" / "terraform" / "pipelines"
_TF_PLUGIN_CACHE = Path.home() / ".ddt" / "terraform" / ".plugin-cache"


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #

def deploy(
    pipeline_name: str,
    subscription: str,
    window_seconds: int,
    project_root: Path,
    gcp_config: dict,
) -> dict:
    """Provision a Dataflow streaming job for a pipeline via Terraform.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]
    sa_email = gcp_config["sa_email"]

    image_uri = _image_uri(project_id, region, pipeline_name)

    print(f"  Building container image '{image_uri}'...")
    print("  (First build may take a few minutes — includes apache-beam[gcp])", flush=True)
    _build_image(project_root, project_id, region, pipeline_name, image_uri, warehouse_bucket)

    template_gcs_path = f"gs://{warehouse_bucket}/dataflow-templates/{pipeline_name}.json"
    print(f"  Uploading Flex Template spec to {template_gcs_path}...", flush=True)
    _upload_flex_template_spec(image_uri, pipeline_name, template_gcs_path, sa_email,
                               warehouse_bucket, project_id, region)

    print("  Applying Terraform (Dataflow streaming job)...", flush=True)
    job_name, job_id = _terraform_apply_streaming(
        pipeline_name=pipeline_name,
        template_gcs_path=template_gcs_path,
        subscription=subscription,
        warehouse_bucket=warehouse_bucket,
        window_seconds=window_seconds,
        sa_email=sa_email,
        project_id=project_id,
        region=region,
    )

    print(f"  Waiting for Dataflow job to reach RUNNING state...", flush=True)
    _wait_for_running(job_id, project_id, region)

    return {
        "type": "streaming",
        "subscription": subscription,
        "window_seconds": window_seconds,
        "dataflow_job_name": job_name,
        "dataflow_job_id": job_id,
        "image_uri": image_uri,
        "template_gcs_path": template_gcs_path,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(pipeline_name: str, deployment: dict, gcp_config: dict) -> None:
    """Drain and destroy the Dataflow streaming job via Terraform."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]

    print(f"  Destroying Terraform resources for '{pipeline_name}' (draining job)...", flush=True)
    print("  Note: draining a streaming job may take 1–5 minutes.", flush=True)
    _terraform_destroy_streaming(pipeline_name, project_id, region)


# ------------------------------------------------------------------ #
# Container image                                                      #
# ------------------------------------------------------------------ #

def _image_uri(project_id: str, region: str, pipeline_name: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/ddt-runner/{pipeline_name}-stream:latest"


def _build_image(
    project_root: Path,
    project_id: str,
    region: str,
    pipeline_name: str,
    image_uri: str,
    warehouse_bucket: str,
) -> None:
    _ensure_artifact_registry_repo(project_id, region)

    with tempfile.TemporaryDirectory(prefix="ddt-stream-build-") as tmp:
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
            FROM gcr.io/dataflow-templates-base/python312-template-launcher-base
            WORKDIR /template
            COPY pyproject.toml .
            COPY ddt/ ./ddt/
            RUN pip install --no-cache-dir -e . 'apache-beam[gcp]'
            COPY pipelines/ ./pipelines/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/ddt/gcp/beam_runner.py"
        """))

        result = subprocess.run(
            [
                "gcloud", "builds", "submit",
                "--project", project_id,
                "--region", region,
                "--tag", image_uri,
                "--timeout", "900s",
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
# Flex Template spec                                                   #
# ------------------------------------------------------------------ #

def _upload_flex_template_spec(
    image_uri: str,
    pipeline_name: str,
    template_gcs_path: str,
    sa_email: str,
    warehouse_bucket: str,
    project_id: str,
    region: str,
) -> None:
    spec = {
        "image": image_uri,
        "sdk_info": {"language": "PYTHON"},
        "metadata": {
            "name": f"ddt-stream-{pipeline_name}",
            "description": f"ddt streaming pipeline: {pipeline_name}",
            "parameters": [
                {
                    "name": "pipeline_name",
                    "label": "Pipeline Name",
                    "helpText": "ddt pipeline name (must match a file in pipelines/)",
                },
                {
                    "name": "subscription",
                    "label": "Pub/Sub Subscription",
                    "helpText": "Full subscription resource path",
                },
                {
                    "name": "warehouse_bucket",
                    "label": "Warehouse Bucket",
                    "helpText": "GCS bucket name (no gs:// prefix)",
                },
                {
                    "name": "window_seconds",
                    "label": "Window Size (seconds)",
                    "helpText": "Fixed-time window size for Parquet writes",
                },
            ],
        },
        "defaultEnvironment": {
            "serviceAccountEmail": sa_email,
            "tempLocation": f"gs://{warehouse_bucket}/dataflow-temp/{pipeline_name}",
            "workerRegion": region,
        },
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", prefix="ddt-template-", delete=False
    ) as f:
        json.dump(spec, f, indent=2)
        tmp_path = f.name

    try:
        result = subprocess.run(
            ["gsutil", "cp", tmp_path, template_gcs_path],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to upload Flex Template spec: {result.stderr}")
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ------------------------------------------------------------------ #
# Terraform                                                            #
# ------------------------------------------------------------------ #

def _tf_work_dir(pipeline_name: str) -> Path:
    return _PIPELINE_TF_DIR / pipeline_name


def _tf_env() -> dict:
    return {
        **os.environ,
        "TF_INPUT": "0",
        "TF_PLUGIN_CACHE_DIR": str(_TF_PLUGIN_CACHE),
    }


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


def _terraform_apply_streaming(
    pipeline_name: str,
    template_gcs_path: str,
    subscription: str,
    warehouse_bucket: str,
    window_seconds: int,
    sa_email: str,
    project_id: str,
    region: str,
) -> tuple[str, str]:
    """Provision the Dataflow streaming job via Terraform. Returns (job_name, job_id)."""
    work_dir = _tf_work_dir(pipeline_name)
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    for tf_file in _STREAMING_MODULE_DIR.glob("*.tf"):
        shutil.copy2(tf_file, work_dir / tf_file.name)

    tfvars = {
        "project_id": project_id,
        "region": region,
        "pipeline_name": pipeline_name,
        "template_gcs_path": template_gcs_path,
        "subscription": subscription,
        "warehouse_bucket": warehouse_bucket,
        "window_seconds": window_seconds,
        "sa_email": sa_email,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    outputs = json.loads(
        subprocess.run(
            ["terraform", "output", "-json"],
            cwd=str(work_dir), env=env, capture_output=True, text=True,
        ).stdout
    )
    return outputs["job_name"]["value"], outputs["job_id"]["value"]


def _terraform_destroy_streaming(pipeline_name: str, project_id: str, region: str) -> None:
    """Drain + destroy the Dataflow job via Terraform, then remove the state dir.

    The Terraform resource has on_delete = "drain", so terraform destroy
    automatically drains (not cancels) before removing the resource.
    """
    work_dir = _tf_work_dir(pipeline_name)
    if not work_dir.exists():
        raise RuntimeError(
            f"No Terraform state found for pipeline '{pipeline_name}' at {work_dir}.\n"
            "If you deployed from a different machine, drain the job manually:\n"
            f"  gcloud dataflow jobs drain <job_id> --region {region} --project {project_id}"
        )

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


# ------------------------------------------------------------------ #
# Dataflow job state polling                                           #
# ------------------------------------------------------------------ #

def _wait_for_running(job_id: str, project_id: str, region: str, timeout_secs: int = 300) -> None:
    import time

    poll_interval = 15
    elapsed = 0
    while elapsed < timeout_secs:
        time.sleep(poll_interval)
        elapsed += poll_interval

        result = subprocess.run(
            [
                "gcloud", "dataflow", "jobs", "describe", job_id,
                "--region", region, "--project", project_id,
                "--format", "value(currentState)",
            ],
            capture_output=True, text=True,
        )
        state = result.stdout.strip()
        if state == "JOB_STATE_RUNNING":
            return
        if state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED"):
            raise RuntimeError(
                f"Dataflow job {job_id} reached unexpected state '{state}' during startup.\n"
                f"Check the Dataflow console for details."
            )
        print(f"  Job state: {state} ({elapsed}s elapsed)...", flush=True)

    raise RuntimeError(
        f"Dataflow job {job_id} did not reach JOB_STATE_RUNNING within {timeout_secs}s."
    )
