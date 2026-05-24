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
    """Compile + Terraform apply for a single collector.

    Returns the deployment state dict to write into state.yml.
    """
    from .compile import compile_project

    project_id = gcp_config["project_id"]
    region = gcp_config["region"]

    print(f"  Compiling '{collector_name}'...", flush=True)
    plan = compile_project(project_root, gcp_config, filter_names=[collector_name])

    credentials = _generate_airflow_credentials(project_root)

    print(f"  Applying Terraform...", flush=True)
    print(f"  (First deploy may take several minutes)", flush=True)
    outputs = _tf_apply_project(project_id, region, True, credentials, project_root,
                                new_collectors=plan.new)

    airflow_url = outputs.get("airflow_url", "")
    if airflow_url:
        print(f"  Airflow UI: {airflow_url}", flush=True)

    job_name = _expected_job_name(collector_name)
    content_hash = plan.hashes.get(collector_name, gcp_config.get("deployments", {}).get(collector_name, {}).get("content_hash", ""))

    from ...config.models import SqlSource, CloudSqlConnection
    from ...config.loader import load_collector
    _collector = load_collector(project_root / "collectors" / f"{collector_name}.yml")
    cloud_sql_instances: list[str] = []
    if (isinstance(_collector.source, SqlSource) and
            isinstance(_collector.source.connection, CloudSqlConnection)):
        cloud_sql_instances = [_collector.source.connection.instance]

    return {
        "schedule": schedule,
        "dag_id": collector_name,
        "cloud_run_job": job_name,
        "airflow_url": airflow_url,
        "image_uri": _image_uri(project_id, region, collector_name),
        "content_hash": content_hash,
        "java_enabled": False,
        "build_context": str(_BUILD_DIR / "gcp" / collector_name),
        "cloud_sql_instances": cloud_sql_instances,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def deploy_all(project_root: Path, gcp_config: dict) -> dict[str, dict]:
    """Full-project compile + single Terraform apply for all collectors.

    Returns a dict of collector_name → state dict for every new/changed collector.
    Unchanged collectors are skipped (already up to date in state).
    """
    from .compile import compile_project

    project_id = gcp_config["project_id"]
    region = gcp_config["region"]

    plan = compile_project(project_root, gcp_config, filter_names=None)
    deploy_airflow = len(plan.all_collector_names) > 0
    credentials = _generate_airflow_credentials(project_root) if deploy_airflow else {}

    print(f"  Applying Terraform...", flush=True)
    print(f"  (First deploy may take several minutes)", flush=True)
    outputs = _tf_apply_project(project_id, region, deploy_airflow, credentials, project_root,
                                new_collectors=plan.new)

    airflow_url = outputs.get("airflow_url", "")
    if airflow_url:
        print(f"  Airflow UI: {airflow_url}", flush=True)

    results: dict[str, dict] = {}
    for name in plan.new + plan.changed:
        collector = plan.collectors[name]

        from ...config.models import SqlSource, CloudSqlConnection
        cloud_sql_instances: list[str] = []
        if (isinstance(collector.source, SqlSource) and
                isinstance(collector.source.connection, CloudSqlConnection)):
            cloud_sql_instances = [collector.source.connection.instance]

        results[name] = {
            "schedule": collector.deployment.schedule,
            "dag_id": name,
            "cloud_run_job": _expected_job_name(name),
            "airflow_url": airflow_url,
            "image_uri": _image_uri(project_id, region, name),
            "content_hash": plan.hashes[name],
            "java_enabled": False,
            "build_context": str(_BUILD_DIR / "gcp" / name),
            "cloud_sql_instances": cloud_sql_instances,
            "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        }

    return results


def undeploy(collector_name: str, deployment: dict, gcp_config: dict, project_root: Path) -> None:
    """Remove a collector by deleting its .tf.json and re-applying Terraform."""
    project_id = gcp_config["project_id"]
    region = gcp_config["region"]

    tf_dir = _tf_state_dir(project_root) / "gcp"
    tf_json_path = tf_dir / f"collector_{collector_name}.tf.json"
    if tf_json_path.exists():
        tf_json_path.unlink()

    remaining = list(tf_dir.glob("collector_*.tf.json"))
    deploy_airflow = len(remaining) > 0
    credentials = _generate_airflow_credentials(project_root) if deploy_airflow else {}

    print(f"  Applying Terraform (removing '{collector_name}')...", flush=True)
    _tf_apply_project(project_id, region, deploy_airflow, credentials, project_root,
                      new_collectors=[])


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
    deploy_airflow: bool,
    airflow_creds: dict,
    project_root: Path,
    new_collectors: list[str] | None = None,
) -> dict:
    """Provision all project resources via a single Terraform apply.

    Per-collector resources are defined by .tf.json files already written to work_dir
    by the compile step. Only platform resources (SA, buckets, Airflow) are in main.tf.
    Returns flattened terraform output dict.
    """
    work_dir = _tf_state_dir(project_root) / "gcp"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_PROJECT_MODULE_DIR / "gcp", work_dir)

    tfvars: dict = {
        "project_id": project_id,
        "region": region,
        "deploy_airflow": deploy_airflow,
        "airflow_image_uri": _airflow_image_uri(project_id, region) if deploy_airflow else "",
        "airflow_build_context": str(_airflow_build_context()) if deploy_airflow else "",
        "airflow_content_hash": _airflow_content_hash() if deploy_airflow else "",
        "db_password": airflow_creds.get("db_password", ""),
        "admin_password": airflow_creds.get("admin_password", ""),
        "fernet_key": airflow_creds.get("fernet_key", ""),
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    if not (work_dir / ".terraform").exists():
        _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _import_existing_project_resources(project_id, region, new_collectors or [], work_dir, env)
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
    new_collectors: list[str],
    work_dir: Path,
    env: dict,
) -> None:
    """Import pre-existing GCP resources into Terraform state to avoid 409 conflicts.

    Optimizations vs the old approach:
    - Runs `terraform state list` once; skips gcloud checks for resources already in state.
    - Migrates old for_each addresses (collector["name"]) to per-file addresses (collector_name).
    - Only checks Cloud Run job imports for new_collectors, not all collectors.
    """
    import re as _re

    state_result = subprocess.run(
        ["terraform", "state", "list"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    existing = set(state_result.stdout.splitlines())

    # Migrate old for_each addresses to per-file addresses (one-time, idempotent)
    foreach_re = _re.compile(
        r'^(google_cloud_run_v2_job|local_file|null_resource)\.collector\["(.+)"\]$'
    )
    for addr in list(existing):
        m = foreach_re.match(addr)
        if not m:
            continue
        resource_type, collector_name = m.group(1), m.group(2)
        new_addr = f"{resource_type}.collector_{collector_name}"
        result = subprocess.run(
            ["terraform", "state", "mv", addr, new_addr],
            cwd=str(work_dir), env=env, capture_output=True, text=True,
        )
        if result.returncode == 0:
            existing.discard(addr)
            existing.add(new_addr)
            logger.info("Migrated state: %s → %s", addr, new_addr)
        else:
            logger.warning("state mv failed for %s: %s", addr, result.stderr[-200:])

    # Platform resources: skip gcloud check entirely if already in state
    sa_email = f"dcf-lake@{project_id}.iam.gserviceaccount.com"
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_service_account.dcf_lake",
        f"projects/{project_id}/serviceAccounts/{sa_email}",
        ["gcloud", "iam", "service-accounts", "describe", sa_email, "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_secret_manager_secret.sa_key",
        f"projects/{project_id}/secrets/dcf-lake-sa-key",
        ["gcloud", "secrets", "describe", "dcf-lake-sa-key", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_storage_bucket.warehouse",
        f"dcf-warehouse-{project_id}",
        ["gcloud", "storage", "buckets", "describe",
         f"gs://dcf-warehouse-{project_id}", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_storage_bucket.dags",
        f"dcf-dags-{project_id}",
        ["gcloud", "storage", "buckets", "describe",
         f"gs://dcf-dags-{project_id}", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_artifact_registry_repository.dcf_runner",
        f"projects/{project_id}/locations/{region}/repositories/dcf-runner",
        ["gcloud", "artifacts", "repositories", "describe", "dcf-runner",
         "--location", region, "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_sql_database_instance.airflow_db[0]",
        f"{project_id}/dcf-airflow-db",
        ["gcloud", "sql", "instances", "describe", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_sql_database.airflow[0]",
        f"{project_id}/dcf-airflow-db/airflow",
        ["gcloud", "sql", "databases", "describe", "airflow",
         "--instance", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_sql_user.airflow[0]",
        f"{project_id}/dcf-airflow-db/airflow",
        ["gcloud", "sql", "instances", "describe", "dcf-airflow-db", "--project", project_id],
    )
    _tf_import_if_not_in_state(
        work_dir, env, existing,
        "google_cloud_run_v2_service.airflow[0]",
        f"projects/{project_id}/locations/{region}/services/dcf-airflow",
        ["gcloud", "run", "services", "describe", "dcf-airflow",
         "--region", region, "--project", project_id],
    )

    # Per-collector: only check new collectors (not all collectors)
    for name in new_collectors:
        job_name = _expected_job_name(name)
        _tf_import_if_not_in_state(
            work_dir, env, existing,
            f"google_cloud_run_v2_job.collector_{name}",
            f"projects/{project_id}/locations/{region}/jobs/{job_name}",
            ["gcloud", "run", "jobs", "describe", job_name,
             "--region", region, "--project", project_id],
        )


def _tf_import_if_not_in_state(
    work_dir: Path,
    env: dict,
    existing_addresses: set[str],
    resource_addr: str,
    resource_id: str,
    check_cmd: list,
) -> None:
    """Import resource only if it is not already in Terraform state.

    Skips the gcloud API call entirely when the resource is already tracked,
    eliminating the main source of per-deploy latency.
    """
    if resource_addr in existing_addresses:
        logger.debug("%s already in Terraform state, skipping import", resource_addr)
        return
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
