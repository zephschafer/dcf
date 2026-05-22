"""Batch collector deployment: builds a container image via Cloud Build, then uses
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
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DCF_DEPLOY_DIR   = Path(__file__).parent.parent                            # dcf/deploy/
_DCF_PKG_DIR      = _DCF_DEPLOY_DIR.parent                                  # dcf/ Python package
_DCF_REPO_ROOT    = _DCF_PKG_DIR.parent                                     # project root
_BATCH_MODULE_DIR = _DCF_DEPLOY_DIR / "infra" / "modules" / "batch_collector"
_BUILD_DIR = Path.home() / ".dcf" / "build"
_TF_PLUGIN_CACHE = Path.home() / ".dcf" / ".plugin-cache"


def _tf_state_dir(project_root: Path) -> Path:
    """Return the Terraform state directory for this project.

    Defaults to <project_root>/.dcf/terraform; can be overridden with
    `terraform_state_dir` in .dcf/state.yml.
    """
    try:
        from ...state import load_state
        custom = load_state().get("terraform_state_dir")
        if custom:
            return Path(custom).expanduser()
    except RuntimeError:
        pass
    return project_root / ".dcf" / "terraform"


def _write_pyproject_toml(dest: Path) -> None:
    repo_pyproject = _DCF_REPO_ROOT / "pyproject.toml"
    if repo_pyproject.exists():
        shutil.copy2(repo_pyproject, dest / "pyproject.toml")
        return

    import importlib.metadata

    meta = importlib.metadata.metadata("dcf-core")
    version = meta["Version"]
    reqs = importlib.metadata.requires("dcf-core") or []
    direct_deps = [r for r in reqs if "extra ==" not in r]
    deps_str = "\n".join(f'    "{r}",' for r in direct_deps)
    (dest / "pyproject.toml").write_text(
        f'[project]\n'
        f'name = "dcf"\n'
        f'version = "{version}"\n'
        f'requires-python = ">=3.12"\n'
        f'dependencies = [\n{deps_str}\n]\n\n'
        f'[project.scripts]\n'
        f'dcf = "dcf.cli:app"\n\n'
        f'[tool.setuptools.packages.find]\n'
        f'include = ["dcf*"]\n'
    )


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #

def deploy(
    collector_name: str,
    schedule: str,
    paused: bool,
    project_root: Path,
    gcp_config: dict,
) -> dict:
    """Provision a Cloud Run job for a collector via Terraform, write DAG to GCS,
    and provision the GCP Airflow stack (Cloud Run + Cloud SQL) if needed.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]
    sa_email = gcp_config["sa_email"]

    image_uri = _image_uri(project_id, region, collector_name)

    print(f"  Syncing build context for '{collector_name}'...", flush=True)
    build_context = _sync_build_context(project_root, collector_name, gcp_config)
    content_hash = _content_hash(build_context)

    from .bootstrap import enable_required_apis
    from .gcloud import get_credentials as _get_credentials
    enable_required_apis(project_id, _get_credentials())

    print(f"  Ensuring Artifact Registry repository exists...", flush=True)
    _ensure_artifact_registry_repo(project_id, region)

    print(f"  Applying Terraform (Cloud Run job + Cloud Build)...", flush=True)
    print(f"  (First build may take a few minutes)", flush=True)
    job_name = _terraform_apply_collector(
        collector_name=collector_name,
        image_uri=image_uri,
        sa_email=sa_email,
        build_context=build_context,
        content_hash=content_hash,
        project_id=project_id,
        region=region,
        project_root=project_root,
    )

    print(f"  Writing DAG to GCS...", flush=True)
    dag_content = _gcp_dag_content(
        collector_name=collector_name,
        schedule=schedule,
        paused=paused,
        project_id=project_id,
        region=region,
        job_name=job_name,
    )
    _write_dag_gcs(dag_content, collector_name, warehouse_bucket)

    print(f"  Provisioning GCP Airflow stack...", flush=True)
    credentials = _generate_airflow_credentials(project_root)
    airflow_outputs = _tf_apply_airflow_gcp(
        build_context=_airflow_build_context(),
        image_uri=_airflow_image_uri(project_id, region),
        content_hash=_airflow_content_hash(),
        gcp_config=gcp_config,
        credentials=credentials,
        project_root=project_root,
    )

    airflow_url = airflow_outputs.get("webserver_url", {}).get("value", "")
    if airflow_url:
        print(f"  Airflow UI: {airflow_url}", flush=True)

    return {
        "schedule": schedule,
        "dag_id": collector_name,
        "cloud_run_job": job_name,
        "airflow_url": airflow_url,
        "image_uri": image_uri,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(collector_name: str, deployment: dict, gcp_config: dict, project_root: Path) -> None:
    """Remove the Cloud Run job via Terraform destroy and delete the DAG from GCS."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    warehouse_bucket = gcp_config["warehouse_bucket"]

    print(f"  Destroying Terraform resources for '{collector_name}'...", flush=True)
    _terraform_destroy_collector(collector_name, project_id, region, project_root)

    print(f"  Deleting DAG from GCS...", flush=True)
    _delete_dag_gcs(collector_name, warehouse_bucket)

    if not _gcs_dag_files_exist(warehouse_bucket):
        print(f"  No remaining DAGs — tearing down Airflow stack...", flush=True)
        _tf_destroy_airflow_gcp(project_root)


# ------------------------------------------------------------------ #
# Build context                                                        #
# ------------------------------------------------------------------ #

def _image_uri(project_id: str, region: str, collector_name: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/dcf-runner/{collector_name}:latest"


def _sync_build_context(
    project_root: Path, collector_name: str, gcp_config: dict
) -> Path:
    """Create a stable build context dir at ~/.dcf/build/gcp/<name>/."""
    build_context = _BUILD_DIR / "gcp" / collector_name
    shutil.rmtree(build_context, ignore_errors=True)
    build_context.mkdir(parents=True)

    shutil.copytree(_DCF_PKG_DIR, build_context / "dcf")
    _write_pyproject_toml(build_context)

    for subdir in ("collectors", "connectors"):
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
# Terraform: per-collector resources                                   #
# ------------------------------------------------------------------ #

def _expected_job_name(collector_name: str) -> str:
    return f"dcf-job-{collector_name.replace('_', '-')}"


def _tf_work_dir(collector_name: str, project_root: Path) -> Path:
    return _tf_state_dir(project_root) / "collectors" / collector_name / "gcp"


def _copy_module_to_work_dir(module_dir: Path, work_dir: Path) -> None:
    """Copy a leaf Terraform module's .tf files + shared templates into work_dir."""
    for item in module_dir.iterdir():
        if item.name in (".terraform", ".terraform.lock.hcl"):
            continue
        if item.is_file() and item.suffix == ".tf":
            shutil.copy2(item, work_dir / item.name)
    templates_src = _DCF_DEPLOY_DIR / "infra" / "templates"
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


def _terraform_apply_collector(
    collector_name: str,
    image_uri: str,
    sa_email: str,
    build_context: Path,
    content_hash: str,
    project_id: str,
    region: str,
    project_root: Path,
) -> str:
    """Provision Cloud Run job via Terraform + Cloud Build. Returns the job name."""
    work_dir = _tf_work_dir(collector_name, project_root)
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_MODULE_DIR / "gcp", work_dir)

    tfvars = {
        "project_id": project_id,
        "region": region,
        "collector_name": collector_name,
        "image_uri": image_uri,
        "sa_email": sa_email,
        "build_context": str(build_context),
        "content_hash": content_hash,
        "java_enabled": False,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _import_existing_cloud_run_job(collector_name, project_id, region, work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    outputs = json.loads(
        subprocess.run(
            ["terraform", "output", "-json"],
            cwd=str(work_dir), env=env, capture_output=True, text=True,
        ).stdout
    )
    return outputs["job_name"]["value"]


def _terraform_destroy_collector(
    collector_name: str, project_id: str, region: str, project_root: Path,
) -> None:
    """Destroy Cloud Run job via Terraform, then remove the state dir."""
    work_dir = _tf_work_dir(collector_name, project_root)
    if not work_dir.exists():
        raise RuntimeError(
            f"No Terraform state found for collector '{collector_name}' at {work_dir}.\n"
            "If you deployed from a different machine, delete the Cloud Run job manually:\n"
            f"  gcloud run jobs delete dcf-job-{collector_name.replace('_', '-')} "
            f"--region {region} --project {project_id} --quiet"
        )

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


def _import_existing_cloud_run_job(
    collector_name: str, project_id: str, region: str, work_dir: Path, env: dict,
) -> None:
    """Import an existing Cloud Run job into Terraform state to avoid 409 on apply."""
    job_name = _expected_job_name(collector_name)
    check = subprocess.run(
        ["gcloud", "run", "jobs", "describe", job_name,
         "--region", region, "--project", project_id],
        capture_output=True,
    )
    if check.returncode != 0:
        return

    resource_id = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
    result = subprocess.run(
        ["terraform", "import", "google_cloud_run_v2_job.collector", resource_id],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Imported existing Cloud Run job '%s' into Terraform state", job_name)
    elif "already managed by Terraform" in result.stdout + result.stderr:
        logger.info("Cloud Run job '%s' already in Terraform state", job_name)
    else:
        logger.warning("terraform import returned non-zero: %s", result.stderr[-500:])


def _import_existing_airflow_resources(
    project_id: str, region: str, work_dir: Path, env: dict,
) -> None:
    """Import any already-existing Airflow resources to avoid 409 on apply."""
    _tf_import_if_exists(
        work_dir, env,
        "google_sql_database_instance.airflow_db",
        f"{project_id}/dcf-airflow-db",
        ["gcloud", "sql", "instances", "describe", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_sql_database.airflow",
        f"{project_id}/dcf-airflow-db/airflow",
        ["gcloud", "sql", "databases", "describe", "airflow",
         "--instance", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_cloud_run_v2_service.airflow",
        f"projects/{project_id}/locations/{region}/services/dcf-airflow",
        ["gcloud", "run", "services", "describe", "dcf-airflow",
         "--region", region, "--project", project_id],
    )


def _tf_import_if_exists(
    work_dir: Path, env: dict, resource_addr: str, resource_id: str, check_cmd: list,
) -> None:
    if subprocess.run(check_cmd, capture_output=True).returncode != 0:
        return
    result = subprocess.run(
        ["terraform", "import", resource_addr, resource_id],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Imported %s into Terraform state", resource_addr)
    elif "already managed by Terraform" in result.stdout + result.stderr:
        logger.info("%s already in Terraform state", resource_addr)
    else:
        logger.warning("terraform import %s returned non-zero: %s", resource_addr, result.stderr[-300:])


# ------------------------------------------------------------------ #
# GCS DAG management                                                   #
# ------------------------------------------------------------------ #

def _dag_gcs_path(collector_name: str) -> str:
    return f"airflow/dags/{collector_name}.py"


def _gcp_dag_content(
    collector_name: str, schedule: str, paused: bool,
    project_id: str, region: str, job_name: str,
) -> str:
    paused_str = "True" if paused else "False"
    return f"""\
# Generated by dcf — do not edit manually
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

with DAG(
    dag_id="{collector_name}",
    schedule_interval="{schedule}",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation={paused_str},
    tags=["dcf"],
) as dag:
    run_job = CloudRunExecuteJobOperator(
        task_id="run_{collector_name}",
        project_id="{project_id}",
        region="{region}",
        job_name="{job_name}",
    )
"""


def _write_dag_gcs(dag_content: str, collector_name: str, warehouse_bucket: str) -> None:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(warehouse_bucket)
    blob = bucket.blob(_dag_gcs_path(collector_name))
    blob.upload_from_string(dag_content, content_type="text/plain")
    logger.info("Uploaded DAG to gs://%s/%s", warehouse_bucket, _dag_gcs_path(collector_name))


def _delete_dag_gcs(collector_name: str, warehouse_bucket: str) -> None:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(warehouse_bucket)
    blob = bucket.blob(_dag_gcs_path(collector_name))
    if blob.exists():
        blob.delete()
        logger.info("Deleted DAG gs://%s/%s", warehouse_bucket, _dag_gcs_path(collector_name))


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
            "gcloud", "artifacts", "repositories", "describe", "dcf-runner",
            "--location", region, "--project", project_id,
        ],
        capture_output=True,
    )
    if check.returncode == 0:
        return

    # Retry up to ~60s: Artifact Registry API may not be fully propagated
    # immediately after `gcloud services enable` returns.
    last_err = ""
    for attempt in range(12):
        result = subprocess.run(
            [
                "gcloud", "artifacts", "repositories", "create", "dcf-runner",
                "--repository-format=docker",
                "--location", region,
                "--project", project_id,
            ],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            return
        last_err = result.stderr
        if "PERMISSION_DENIED" in last_err and attempt < 11:
            logger.info("Artifact Registry API not yet ready, retrying in 5s (attempt %d/12)...", attempt + 1)
            time.sleep(5)
            continue
        raise RuntimeError(
            f"Failed to create Artifact Registry repository: {last_err}\n"
            "Ensure the API is enabled:\n"
            "  gcloud services enable artifactregistry.googleapis.com"
        )


# ------------------------------------------------------------------ #
# GCP Airflow stack                                                    #
# ------------------------------------------------------------------ #

def _airflow_image_uri(project_id: str, region: str) -> str:
    return f"{region}-docker.pkg.dev/{project_id}/dcf-runner/dcf-airflow:latest"


def _airflow_build_context() -> Path:
    build_context = _BUILD_DIR / "airflow-gcp"
    build_context.mkdir(parents=True, exist_ok=True)
    return build_context


def _airflow_content_hash() -> str:
    template = _DCF_DEPLOY_DIR / "infra" / "templates" / "airflow.Dockerfile.tftpl"
    return hashlib.sha256(template.read_bytes()).hexdigest()


def _generate_airflow_credentials(project_root: Path) -> dict:
    """Read/generate Airflow credentials from .env."""
    from ...state import load_env, save_env
    env = load_env()

    admin_password = env.get("AIRFLOW_ADMIN_PASSWORD")
    if not admin_password:
        import getpass
        admin_password = getpass.getpass("Enter Airflow admin password: ").strip()
        if not admin_password:
            raise RuntimeError("Airflow admin password cannot be empty.")
        save_env("AIRFLOW_ADMIN_PASSWORD", admin_password)
        logger.info("Saved AIRFLOW_ADMIN_PASSWORD to .env")

    fernet_key = env.get("AIRFLOW_FERNET_KEY")
    if not fernet_key:
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key().decode()
        save_env("AIRFLOW_FERNET_KEY", fernet_key)

    db_password = env.get("AIRFLOW_DB_PASSWORD")
    if not db_password:
        db_password = secrets.token_urlsafe(16)
        save_env("AIRFLOW_DB_PASSWORD", db_password)

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
    project_root: Path,
) -> dict:
    work_dir = _tf_state_dir(project_root) / "airflow" / "gcp"
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
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _import_existing_airflow_resources(project_id, region, work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    raw = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    ).stdout
    return json.loads(raw) if raw.strip() else {}


def _tf_destroy_airflow_gcp(project_root: Path) -> None:
    work_dir = _tf_state_dir(project_root) / "airflow" / "gcp"
    if not work_dir.exists():
        return

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)
