"""Batch pipeline deployment: builds a container image via Cloud Build, then uses
Terraform to provision a Cloud Run job. DAG is written directly to GCS for the
custom Airflow stack (no Cloud Composer)."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DDT_PKG_DIR = Path(__file__).parent.parent          # ddt/ package
_DDT_REPO_ROOT = _DDT_PKG_DIR.parent
_BATCH_MODULE_DIR = _DDT_PKG_DIR / "infra" / "modules" / "batch_pipeline"
_BUILD_DIR = Path.home() / ".ddt" / "build"
_PIPELINE_TF_DIR = Path.home() / ".ddt" / "terraform" / "pipelines"
_AIRFLOW_TF_DIR = Path.home() / ".ddt" / "terraform" / "airflow" / "gcp"
_TF_PLUGIN_CACHE = Path.home() / ".ddt" / "terraform" / ".plugin-cache"


def _write_pyproject_toml(dest: Path) -> None:
    repo_pyproject = _DDT_REPO_ROOT / "pyproject.toml"
    if repo_pyproject.exists():
        shutil.copy2(repo_pyproject, dest / "pyproject.toml")
        return

    import importlib.metadata

    meta = importlib.metadata.metadata("ddt")
    version = meta["Version"]
    reqs = importlib.metadata.requires("ddt") or []
    direct_deps = [r for r in reqs if "extra ==" not in r]
    deps_str = "\n".join(f'    "{r}",' for r in direct_deps)
    (dest / "pyproject.toml").write_text(
        f'[project]\n'
        f'name = "ddt"\n'
        f'version = "{version}"\n'
        f'requires-python = ">=3.12"\n'
        f'dependencies = [\n{deps_str}\n]\n\n'
        f'[project.scripts]\n'
        f'ddt = "ddt.cli:app"\n\n'
        f'[tool.setuptools.packages.find]\n'
        f'include = ["ddt*"]\n'
    )


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
    """Provision a Cloud Run job for a pipeline via Terraform, write DAG to GCS,
    and provision the GCP Airflow stack (Cloud Run + Cloud SQL) if needed.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]
    sa_email = gcp_config["sa_email"]

    image_uri = _image_uri(project_id, region, pipeline_name)

    print(f"  Syncing build context for '{pipeline_name}'...", flush=True)
    build_context = _sync_build_context(project_root, pipeline_name, gcp_config)
    content_hash = _content_hash(build_context)

    print(f"  Ensuring Artifact Registry repository exists...", flush=True)
    _ensure_artifact_registry_repo(project_id, region)

    print(f"  Applying Terraform (Cloud Run job + Cloud Build)...", flush=True)
    print(f"  (First build may take a few minutes)", flush=True)
    job_name = _terraform_apply_pipeline(
        pipeline_name=pipeline_name,
        image_uri=image_uri,
        sa_email=sa_email,
        build_context=build_context,
        content_hash=content_hash,
        project_id=project_id,
        region=region,
    )

    print(f"  Writing DAG to GCS...", flush=True)
    dag_content = _gcp_dag_content(
        pipeline_name=pipeline_name,
        schedule=schedule,
        paused=paused,
        project_id=project_id,
        region=region,
        job_name=job_name,
    )
    _write_dag_gcs(dag_content, pipeline_name, warehouse_bucket)

    print(f"  Provisioning GCP Airflow stack...", flush=True)
    credentials = _generate_airflow_credentials(project_root)
    airflow_outputs = _tf_apply_airflow_gcp(
        build_context=_airflow_build_context(),
        image_uri=_airflow_image_uri(project_id, region),
        content_hash=_airflow_content_hash(),
        gcp_config=gcp_config,
        credentials=credentials,
    )

    airflow_url = airflow_outputs.get("webserver_url", {}).get("value", "")
    if airflow_url:
        print(f"  Airflow UI: {airflow_url}", flush=True)

    return {
        "schedule": schedule,
        "dag_id": pipeline_name,
        "cloud_run_job": job_name,
        "airflow_url": airflow_url,
        "image_uri": image_uri,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(pipeline_name: str, deployment: dict, gcp_config: dict) -> None:
    """Remove the Cloud Run job via Terraform destroy and delete the DAG from GCS."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]

    print(f"  Destroying Terraform resources for '{pipeline_name}'...", flush=True)
    _terraform_destroy_pipeline(pipeline_name, project_id, region)

    print(f"  Deleting DAG from GCS...", flush=True)
    _delete_dag_gcs(pipeline_name, warehouse_bucket)

    if not _gcs_dag_files_exist(warehouse_bucket):
        print(f"  No remaining DAGs — tearing down Airflow stack...", flush=True)
        _tf_destroy_airflow_gcp()


# ------------------------------------------------------------------ #
# Build context                                                        #
# ------------------------------------------------------------------ #

def _image_uri(project_id: str, region: str, pipeline_name: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/ddt-runner/{pipeline_name}:latest"


def _sync_build_context(
    project_root: Path, pipeline_name: str, gcp_config: dict
) -> Path:
    """Create a stable build context dir at ~/.ddt/build/gcp/<name>/."""
    build_context = _BUILD_DIR / "gcp" / pipeline_name
    shutil.rmtree(build_context, ignore_errors=True)
    build_context.mkdir(parents=True)

    shutil.copytree(_DDT_PKG_DIR, build_context / "ddt")
    _write_pyproject_toml(build_context)

    for subdir in ("pipelines", "connectors"):
        src = project_root / subdir
        dst = build_context / subdir
        if src.exists():
            shutil.copytree(src, dst)
        else:
            dst.mkdir()

    minimal_config = {
        "catalog": "gcp",
        "gcp": {
            "project_id": gcp_config["project_id"],
            "region": gcp_config["region"],
            "warehouse_bucket": gcp_config["warehouse_bucket"],
        },
    }
    (build_context / "project.yml").write_text(
        yaml.dump(minimal_config, default_flow_style=False)
    )

    return build_context


def _content_hash(build_context: Path) -> str:
    """SHA256 of all files in build_context, excluding Dockerfile (written by Terraform)."""
    h = hashlib.sha256()
    for path in sorted(build_context.rglob("*")):
        if path.is_file() and path.name != "Dockerfile":
            h.update(path.read_bytes())
    return h.hexdigest()


# ------------------------------------------------------------------ #
# Terraform: per-pipeline resources                                    #
# ------------------------------------------------------------------ #

def _expected_job_name(pipeline_name: str) -> str:
    return f"ddt-job-{pipeline_name.replace('_', '-')}"


def _tf_work_dir(pipeline_name: str) -> Path:
    return _PIPELINE_TF_DIR / pipeline_name / "gcp"


def _copy_module_to_work_dir(module_dir: Path, work_dir: Path) -> None:
    """Copy a leaf Terraform module's .tf files + shared templates into work_dir."""
    for item in module_dir.iterdir():
        if item.name in (".terraform", ".terraform.lock.hcl"):
            continue
        if item.is_file() and item.suffix == ".tf":
            shutil.copy2(item, work_dir / item.name)
    templates_src = _DDT_PKG_DIR / "infra" / "templates"
    templates_dst = work_dir / "templates"
    if templates_dst.exists():
        shutil.rmtree(templates_dst)
    shutil.copytree(templates_src, templates_dst)


def _tf_env() -> dict:
    return {
        **os.environ,
        "TF_INPUT": "0",
        "TF_PLUGIN_CACHE_DIR": str(_TF_PLUGIN_CACHE),
    }


def _tf_run(cmd: list[str], work_dir: Path, env: dict) -> None:
    result = subprocess.run(cmd, cwd=str(work_dir), env=env, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(
            "Terraform command failed: %s\nSTDOUT: %s\nSTDERR: %s",
            " ".join(cmd), result.stdout, result.stderr,
        )
        raise RuntimeError(
            f"terraform {cmd[1]} failed (exit {result.returncode}): {result.stderr[-2000:]}"
        )
    logger.info("terraform %s OK", cmd[1])


def _terraform_apply_pipeline(
    pipeline_name: str,
    image_uri: str,
    sa_email: str,
    build_context: Path,
    content_hash: str,
    project_id: str,
    region: str,
) -> str:
    """Provision Cloud Run job via Terraform + Cloud Build. Returns the job name."""
    work_dir = _tf_work_dir(pipeline_name)
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_MODULE_DIR / "gcp", work_dir)

    tfvars = {
        "project_id": project_id,
        "region": region,
        "pipeline_name": pipeline_name,
        "image_uri": image_uri,
        "sa_email": sa_email,
        "build_context": str(build_context),
        "content_hash": content_hash,
        "java_enabled": False,
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
    pipeline_name: str, project_id: str, region: str,
) -> None:
    """Destroy Cloud Run job via Terraform, then remove the state dir."""
    work_dir = _tf_work_dir(pipeline_name)
    if not work_dir.exists():
        raise RuntimeError(
            f"No Terraform state found for pipeline '{pipeline_name}' at {work_dir}.\n"
            "If you deployed from a different machine, delete the Cloud Run job manually:\n"
            f"  gcloud run jobs delete ddt-job-{pipeline_name.replace('_', '-')} "
            f"--region {region} --project {project_id} --quiet"
        )

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


def _import_existing_cloud_run_job(
    pipeline_name: str, project_id: str, region: str, work_dir: Path, env: dict,
) -> None:
    """Import an existing Cloud Run job into Terraform state to avoid 409 on apply."""
    job_name = _expected_job_name(pipeline_name)
    check = subprocess.run(
        ["gcloud", "run", "jobs", "describe", job_name,
         "--region", region, "--project", project_id],
        capture_output=True,
    )
    if check.returncode != 0:
        return

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


# ------------------------------------------------------------------ #
# GCS DAG management                                                   #
# ------------------------------------------------------------------ #

def _dag_gcs_path(pipeline_name: str) -> str:
    return f"airflow/dags/{pipeline_name}.py"


def _gcp_dag_content(
    pipeline_name: str, schedule: str, paused: bool,
    project_id: str, region: str, job_name: str,
) -> str:
    paused_str = "True" if paused else "False"
    return f"""\
# Generated by ddt — do not edit manually
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

with DAG(
    dag_id="{pipeline_name}",
    schedule_interval="{schedule}",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation={paused_str},
    tags=["ddt"],
) as dag:
    run_job = CloudRunExecuteJobOperator(
        task_id="run_{pipeline_name}",
        project_id="{project_id}",
        region="{region}",
        job_name="{job_name}",
    )
"""


def _write_dag_gcs(dag_content: str, pipeline_name: str, warehouse_bucket: str) -> None:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(warehouse_bucket)
    blob = bucket.blob(_dag_gcs_path(pipeline_name))
    blob.upload_from_string(dag_content, content_type="text/plain")
    logger.info("Uploaded DAG to gs://%s/%s", warehouse_bucket, _dag_gcs_path(pipeline_name))


def _delete_dag_gcs(pipeline_name: str, warehouse_bucket: str) -> None:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(warehouse_bucket)
    blob = bucket.blob(_dag_gcs_path(pipeline_name))
    if blob.exists():
        blob.delete()
        logger.info("Deleted DAG gs://%s/%s", warehouse_bucket, _dag_gcs_path(pipeline_name))


def _gcs_dag_files_exist(warehouse_bucket: str) -> bool:
    """Return True if any DAG files remain in gs://<bucket>/airflow/dags/."""
    from google.cloud import storage
    client = storage.Client()
    blobs = list(client.list_blobs(warehouse_bucket, prefix="airflow/dags/", max_results=1))
    return len(blobs) > 0


# ------------------------------------------------------------------ #
# Artifact Registry                                                    #
# ------------------------------------------------------------------ #

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
# GCP Airflow stack                                                    #
# ------------------------------------------------------------------ #

def _airflow_image_uri(project_id: str, region: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/ddt-runner/ddt-airflow:latest"


def _airflow_build_context() -> Path:
    build_context = _BUILD_DIR / "airflow-gcp"
    build_context.mkdir(parents=True, exist_ok=True)
    return build_context


def _airflow_content_hash() -> str:
    template = _DDT_PKG_DIR / "infra" / "modules" / "templates" / "airflow.Dockerfile.tftpl"
    return hashlib.sha256(template.read_bytes()).hexdigest()


def _generate_airflow_credentials(project_root: Path) -> dict:
    """Read/generate Airflow credentials from project.yml."""
    cfg_path = project_root / "project.yml"
    cfg: dict = yaml.safe_load(cfg_path.read_text()) or {} if cfg_path.exists() else {}

    admin_password = cfg.get("airflow_admin_password")
    if not admin_password:
        raise RuntimeError(
            "airflow_admin_password is missing from project.yml.\n"
            "Add it before running ddt deploy:\n\n"
            "  airflow_admin_password: \"your-password-here\"\n"
        )

    changed = False

    fernet_key = cfg.get("airflow_fernet_key")
    if not fernet_key:
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key().decode()
        cfg["airflow_fernet_key"] = fernet_key
        changed = True

    db_password = cfg.get("airflow_db_password")
    if not db_password:
        db_password = secrets.token_urlsafe(16)
        cfg["airflow_db_password"] = db_password
        changed = True

    if changed:
        cfg_path.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False))

    return {
        "db_password": db_password,
        "admin_password": admin_password,
        "fernet_key": fernet_key,
    }


def _tf_apply_airflow_gcp(
    build_context: Path,
    image_uri: str,
    content_hash: str,
    gcp_config: dict,
    credentials: dict,
) -> dict:
    work_dir = _AIRFLOW_TF_DIR
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_MODULE_DIR / "gcp" / "airflow", work_dir)

    tfvars = {
        "image_uri": image_uri,
        "build_context": str(build_context),
        "content_hash": content_hash,
        "project_id": gcp_config["project_id"],
        "region": gcp_config["region"],
        "sa_email": gcp_config["sa_email"],
        "warehouse_bucket": gcp_config["warehouse_bucket"],
        "db_password": credentials["db_password"],
        "admin_password": credentials["admin_password"],
        "fernet_key": credentials["fernet_key"],
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    raw = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    ).stdout
    return json.loads(raw) if raw.strip() else {}


def _tf_destroy_airflow_gcp() -> None:
    work_dir = _AIRFLOW_TF_DIR
    if not work_dir.exists():
        return

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)
