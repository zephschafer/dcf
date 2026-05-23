"""Batch collector deployment: builds container images via Cloud Build, then uses
a single Terraform apply to provision all Cloud Run jobs + Airflow in one state."""

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

_DCF_DEPLOY_DIR     = Path(__file__).parent.parent                             # dcf/deploy/
_DCF_PKG_DIR        = _DCF_DEPLOY_DIR.parent                                  # dcf/ Python package
_DCF_REPO_ROOT      = _DCF_PKG_DIR.parent                                     # project root
_PROJECT_MODULE_DIR = _DCF_DEPLOY_DIR / "infra" / "modules" / "project"
_BUILD_DIR          = Path.home() / ".dcf" / "build"
_TF_PLUGIN_CACHE    = Path.home() / ".dcf" / ".plugin-cache"


def _tf_state_dir(project_root: Path) -> Path:
    """Return the Terraform state directory for this project."""
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
    """Provision all GCP resources via a single Terraform apply, write DAG to GCS.

    Returns the deployment state dict to write into project.yml.
    """
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    dags_bucket = gcp_config.get("dags_bucket") or gcp_config["warehouse_bucket"]

    image_uri = _image_uri(project_id, region, collector_name)

    print(f"  Syncing build context for '{collector_name}'...", flush=True)
    build_context = _sync_build_context(project_root, collector_name, gcp_config)
    content_hash = _content_hash(build_context)

    collectors = _collectors_map(gcp_config, project_root, override={
        collector_name: {
            "image_uri": image_uri,
            "build_context": str(build_context),
            "content_hash": content_hash,
            "java_enabled": False,
        }
    })

    credentials = _generate_airflow_credentials(project_root)

    print(f"  Applying Terraform (all project resources)...", flush=True)
    print(f"  (First deploy may take several minutes)", flush=True)
    outputs = _tf_apply_project(project_id, region, collectors, credentials, project_root)

    job_name = outputs["job_names"][collector_name]
    airflow_url = outputs.get("airflow_url", "")
    if airflow_url:
        print(f"  Airflow UI: {airflow_url}", flush=True)

    print(f"  Writing DAG to GCS...", flush=True)
    dag_content = _gcp_dag_content(
        collector_name=collector_name,
        schedule=schedule,
        paused=paused,
        project_id=project_id,
        region=region,
        job_name=job_name,
    )
    _write_dag_gcs(dag_content, collector_name, dags_bucket)

    return {
        "schedule": schedule,
        "dag_id": collector_name,
        "cloud_run_job": job_name,
        "airflow_url": airflow_url,
        "airflow_dags_bucket": dags_bucket,
        "image_uri": image_uri,
        "content_hash": content_hash,
        "java_enabled": False,
        "build_context": str(build_context),
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def undeploy(collector_name: str, deployment: dict, gcp_config: dict, project_root: Path) -> None:
    """Remove a collector by re-applying Terraform without it, then delete the DAG from GCS."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    dags_bucket = gcp_config.get("dags_bucket") or gcp_config["warehouse_bucket"]

    collectors = _collectors_map(gcp_config, project_root)
    collectors.pop(collector_name, None)

    print(f"  Applying Terraform (removing '{collector_name}')...", flush=True)
    if collectors:
        credentials = _generate_airflow_credentials(project_root)
        _tf_apply_project(project_id, region, collectors, credentials, project_root)
    else:
        _tf_apply_project(project_id, region, {}, {}, project_root)

    print(f"  Deleting DAG from GCS...", flush=True)
    _delete_dag_gcs(collector_name, dags_bucket)


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

    dcf_dir = build_context / ".dcf"
    dcf_dir.mkdir(exist_ok=True)
    state = {
        "catalog": "gcp",
        "gcp": {
            "project_id": gcp_config["project_id"],
            "region": gcp_config["region"],
            "warehouse_bucket": gcp_config["warehouse_bucket"],
        },
    }
    (dcf_dir / "state.yml").write_text(
        yaml.dump(state, default_flow_style=False, sort_keys=False)
    )

    return build_context


def _content_hash(build_context: Path) -> str:
    """SHA256 of all files in build_context, excluding Dockerfile (written by Terraform)."""
    h = hashlib.sha256()
    for path in sorted(build_context.rglob("*")):
        if path.is_file() and path.name != "Dockerfile":
            h.update(path.read_bytes())
    return h.hexdigest()


def _collectors_map(
    gcp_config: dict,
    project_root: Path,
    override: dict | None = None,
) -> dict:
    """Build the full collectors map from stored deployments, plus optional overrides."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    collectors: dict = {}

    for name, dep in gcp_config.get("deployments", {}).items():
        if dep.get("type", "batch") != "batch":
            continue
        build_ctx = dep.get("build_context") or str(_BUILD_DIR / "gcp" / name)
        Path(build_ctx).mkdir(parents=True, exist_ok=True)
        collectors[name] = {
            "image_uri": dep.get("image_uri") or _image_uri(project_id, region, name),
            "build_context": build_ctx,
            "content_hash": dep.get("content_hash", ""),
            "java_enabled": dep.get("java_enabled", False),
        }

    if override:
        collectors.update(override)

    return collectors


# ------------------------------------------------------------------ #
# Terraform: single project state                                      #
# ------------------------------------------------------------------ #

def _expected_job_name(collector_name: str) -> str:
    return f"dcf-job-{collector_name.replace('_', '-')}"


def _copy_module_to_work_dir(module_dir: Path, work_dir: Path) -> None:
    """Copy a Terraform module's .tf files + shared templates into work_dir."""
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
    result = subprocess.run(cmd, cwd=str(work_dir), env=env)
    if result.returncode != 0:
        raise RuntimeError(f"terraform {cmd[1]} failed (exit {result.returncode})")
    logger.info("terraform %s OK", cmd[1])


def _tf_apply_project(
    project_id: str,
    region: str,
    collectors: dict,
    airflow_creds: dict,
    project_root: Path,
) -> dict:
    """Provision all project resources via a single Terraform apply.

    collectors: map of collector_name -> {image_uri, build_context, content_hash, java_enabled}
    airflow_creds: {db_password, admin_password, fernet_key} (ignored when collectors is empty)
    Returns flattened terraform output dict.
    """
    work_dir = _tf_state_dir(project_root) / "gcp"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_PROJECT_MODULE_DIR / "gcp", work_dir)

    deploy_airflow = bool(collectors)
    tfvars: dict = {
        "project_id": project_id,
        "region": region,
        "collectors": collectors,
        "airflow_image_uri": _airflow_image_uri(project_id, region) if deploy_airflow else "",
        "airflow_build_context": str(_airflow_build_context()) if deploy_airflow else "",
        "airflow_content_hash": _airflow_content_hash() if deploy_airflow else "",
        "db_password": airflow_creds.get("db_password", ""),
        "admin_password": airflow_creds.get("admin_password", ""),
        "fernet_key": airflow_creds.get("fernet_key", ""),
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _import_existing_project_resources(project_id, region, collectors, work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    raw = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    ).stdout
    outputs = json.loads(raw) if raw.strip() else {}
    return {k: v["value"] for k, v in outputs.items()}


def _import_existing_project_resources(
    project_id: str,
    region: str,
    collectors: dict,
    work_dir: Path,
    env: dict,
) -> None:
    """Import any GCP resources that already exist into Terraform state to avoid 409 errors."""
    sa_email = f"dcf-lake@{project_id}.iam.gserviceaccount.com"
    _tf_import_if_exists(
        work_dir, env,
        "google_service_account.dcf_lake",
        f"projects/{project_id}/serviceAccounts/{sa_email}",
        ["gcloud", "iam", "service-accounts", "describe", sa_email, "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_secret_manager_secret.sa_key",
        f"projects/{project_id}/secrets/dcf-lake-sa-key",
        ["gcloud", "secrets", "describe", "dcf-lake-sa-key", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_storage_bucket.warehouse",
        f"dcf-warehouse-{project_id}",
        ["gcloud", "storage", "buckets", "describe",
         f"gs://dcf-warehouse-{project_id}", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_storage_bucket.dags",
        f"dcf-dags-{project_id}",
        ["gcloud", "storage", "buckets", "describe",
         f"gs://dcf-dags-{project_id}", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_artifact_registry_repository.dcf_runner",
        f"projects/{project_id}/locations/{region}/repositories/dcf-runner",
        ["gcloud", "artifacts", "repositories", "describe", "dcf-runner",
         "--location", region, "--project", project_id],
    )
    for name in collectors:
        job_name = _expected_job_name(name)
        _tf_import_if_exists(
            work_dir, env,
            f'google_cloud_run_v2_job.collector["{name}"]',
            f"projects/{project_id}/locations/{region}/jobs/{job_name}",
            ["gcloud", "run", "jobs", "describe", job_name,
             "--region", region, "--project", project_id],
        )
    _tf_import_if_exists(
        work_dir, env,
        "google_sql_database_instance.airflow_db[0]",
        f"{project_id}/dcf-airflow-db",
        ["gcloud", "sql", "instances", "describe", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_sql_database.airflow[0]",
        f"{project_id}/dcf-airflow-db/airflow",
        ["gcloud", "sql", "databases", "describe", "airflow",
         "--instance", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_sql_user.airflow[0]",
        f"{project_id}/dcf-airflow-db/airflow",
        ["gcloud", "sql", "instances", "describe", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_exists(
        work_dir, env,
        "google_cloud_run_v2_service.airflow[0]",
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
    return f"{collector_name}.py"


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


# ------------------------------------------------------------------ #
# Airflow                                                              #
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
