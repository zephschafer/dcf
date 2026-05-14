import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import storage

logger = logging.getLogger(__name__)

_MODULE_DIR = Path(__file__).parent.parent / "infra" / "modules" / "gcp"
_WORK_DIR   = Path.home() / ".ddt" / "terraform"


def provision(
    project_id: str,
    region: str,
    sa_email: str,
    tf_state_bucket: str,
) -> str:
    """
    Run terraform init + apply.
    Auth is handled automatically via ADC (gcloud application-default credentials).
    Returns the warehouse_bucket name from terraform output.
    Raises RuntimeError on non-zero exit.
    """
    _WORK_DIR.mkdir(parents=True, exist_ok=True)

    for tf_file in _MODULE_DIR.glob("*.tf"):
        shutil.copy2(tf_file, _WORK_DIR / tf_file.name)

    env = {
        **os.environ,
        "TF_INPUT":            "0",
        "TF_PLUGIN_CACHE_DIR": str(_WORK_DIR / ".plugin-cache"),
    }
    (_WORK_DIR / ".plugin-cache").mkdir(exist_ok=True)

    _run(
        [
            "terraform", "init", "-reconfigure",
            f"-backend-config=bucket={tf_state_bucket}",
            "-backend-config=prefix=terraform/state",
        ],
        _WORK_DIR, env,
    )

    # Import any already-existing resources so apply doesn't fail with 409
    _import_existing_resources(project_id, _WORK_DIR, env)

    _run(
        [
            "terraform", "apply", "-auto-approve",
            f"-var=project_id={project_id}",
            f"-var=region={region}",
            f"-var=sa_email={sa_email}",
        ],
        _WORK_DIR, env,
    )

    return _read_output(_WORK_DIR, env, "warehouse_bucket")


def destroy(
    project_id: str,
    region: str,
    sa_email: str,
    tf_state_bucket: str,
) -> None:
    """Run terraform destroy to remove all provisioned GCP resources."""
    _WORK_DIR.mkdir(parents=True, exist_ok=True)

    for tf_file in _MODULE_DIR.glob("*.tf"):
        shutil.copy2(tf_file, _WORK_DIR / tf_file.name)

    env = {
        **os.environ,
        "TF_INPUT":            "0",
        "TF_PLUGIN_CACHE_DIR": str(_WORK_DIR / ".plugin-cache"),
    }
    (_WORK_DIR / ".plugin-cache").mkdir(exist_ok=True)

    _run(
        [
            "terraform", "init", "-reconfigure",
            f"-backend-config=bucket={tf_state_bucket}",
            "-backend-config=prefix=terraform/state",
        ],
        _WORK_DIR, env,
    )

    _run(
        [
            "terraform", "destroy", "-auto-approve",
            f"-var=project_id={project_id}",
            f"-var=region={region}",
            f"-var=sa_email={sa_email}",
        ],
        _WORK_DIR, env,
    )


def _import_existing_resources(project_id: str, work_dir: Path, env: dict) -> None:
    """Import already-existing GCP resources into Terraform state to avoid 409 on apply."""
    warehouse_bucket = f"ddt-warehouse-{project_id}"
    client = storage.Client(project=project_id)
    try:
        client.get_bucket(warehouse_bucket)
    except (NotFound, Forbidden):
        return  # bucket doesn't exist yet — apply will create it

    # Bucket exists; import it so terraform apply doesn't try to create it again
    result = subprocess.run(
        ["terraform", "import", "google_storage_bucket.warehouse", warehouse_bucket],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode == 0:
        logger.info("Imported existing warehouse bucket '%s' into Terraform state", warehouse_bucket)
    elif "already managed by Terraform" in result.stdout + result.stderr:
        logger.info("Warehouse bucket '%s' already in Terraform state", warehouse_bucket)
    else:
        # Import failed for an unexpected reason — log and continue; apply may still succeed
        logger.warning("terraform import returned non-zero: %s", result.stderr[-500:])


def _run(cmd: list[str], cwd: Path, env: dict) -> None:
    result = subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(
            "Terraform command failed: %s\nSTDOUT: %s\nSTDERR: %s",
            " ".join(cmd), result.stdout, result.stderr,
        )
        raise RuntimeError(
            f"terraform {cmd[1]} failed (exit {result.returncode}): {result.stderr[-2000:]}"
        )
    logger.info("terraform %s OK", cmd[1])


def _read_output(work_dir: Path, env: dict, key: str) -> str:
    result = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"terraform output failed: {result.stderr}")
    outputs = json.loads(result.stdout)
    if key not in outputs:
        raise RuntimeError(f"'{key}' not in terraform output. Got: {list(outputs)}")
    return outputs[key]["value"]
