from __future__ import annotations

import re
from pathlib import Path

import yaml

_KNOWN_DCF_KEYS = frozenset({
    "catalog", "gcp", "deployments", "terraform_state_dir",
    "airflow_admin_password", "airflow_fernet_key", "airflow_db_password",
})
_AIRFLOW_ENV_MAP = {
    "airflow_admin_password": "AIRFLOW_ADMIN_PASSWORD",
    "airflow_fernet_key": "AIRFLOW_FERNET_KEY",
    "airflow_db_password": "AIRFLOW_DB_PASSWORD",
}


def _project_root() -> Path:
    from .project import find_project_root
    return find_project_root()


def _state_path() -> Path:
    return _project_root() / ".dcf" / "state.yml"


def _env_path() -> Path:
    return _project_root() / ".env"


# ------------------------------------------------------------------ #
# State (.dcf/state.yml)                                               #
# ------------------------------------------------------------------ #

def load_state() -> dict:
    _migrate_if_needed()
    path = _state_path()
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def save_state(state: dict) -> None:
    path = _state_path()
    path.parent.mkdir(exist_ok=True)
    path.write_text(yaml.dump(state, default_flow_style=False, sort_keys=False))


def get_catalog() -> str:
    return load_state().get("catalog", "local")


def get_active_profile_name() -> str:
    return load_state().get("active_profile", "default")


# ------------------------------------------------------------------ #
# Env (.env)                                                           #
# ------------------------------------------------------------------ #

def load_env() -> dict:
    """Parse .env as {KEY: value}. Keys are preserved as written (typically uppercase)."""
    path = _env_path()
    if not path.exists():
        return {}
    result = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        result[key.strip()] = value.strip()
    return result


def save_env(key: str, value: str) -> None:
    """Upsert KEY=value in .env."""
    path = _env_path()
    content = path.read_text() if path.exists() else ""
    lines = content.splitlines()
    pattern = re.compile(rf"^{re.escape(key)}\s*=")
    new_line = f"{key}={value}"
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = new_line
            path.write_text("\n".join(lines) + "\n")
            return
    lines.append(new_line)
    path.write_text("\n".join(lines) + "\n")


# ------------------------------------------------------------------ #
# Migration from old project.yml                                       #
# ------------------------------------------------------------------ #

def _migrate_if_needed() -> None:
    try:
        root = _project_root()
    except RuntimeError:
        return

    old_path = root / "project.yml"
    if not old_path.exists():
        return

    data = yaml.safe_load(old_path.read_text()) or {}
    if not data:
        old_path.rename(root / "project.yml.bak")
        return

    state: dict = {}

    if "catalog" in data:
        state["catalog"] = data["catalog"]
    if "terraform_state_dir" in data:
        state["terraform_state_dir"] = data["terraform_state_dir"]

    # Split deployments: local records (no dag_id/dataflow_job_name) → state
    #                    GCP records (dag_id or dataflow_job_name) → profile
    deployments = data.get("deployments", {})
    local_deps, gcp_deps = {}, {}
    for name, dep in deployments.items():
        if "dag_id" in dep or "dataflow_job_name" in dep:
            gcp_deps[name] = dep
        else:
            local_deps[name] = dep
    if local_deps:
        state["deployments"] = local_deps

    # GCP provisioning state → active profile in profiles.yml
    gcp = data.get("gcp", {})
    if gcp or gcp_deps:
        from .profiles import load_profile, save_profile
        profile_name = "default"
        try:
            profile = load_profile(profile_name)
        except (FileNotFoundError, KeyError):
            profile = {}

        if gcp:
            updates = {
                k: gcp[k] for k in (
                    "project_id", "region", "setup_status", "setup_error",
                    "sa_email", "secret_name", "tf_state_bucket", "warehouse_bucket",
                ) if k in gcp
            }
            profile.update(updates)
        if gcp_deps:
            profile["deployments"] = gcp_deps
        save_profile(profile_name, profile)

        if gcp.get("setup_status") == "complete":
            state["active_profile"] = profile_name

    # Airflow credentials → .env
    for yml_key, env_key in _AIRFLOW_ENV_MAP.items():
        if data.get(yml_key):
            save_env(env_key, data[yml_key])

    # User env var keys (anything not in _KNOWN_DCF_KEYS) → .env
    for key, value in data.items():
        if key not in _KNOWN_DCF_KEYS and isinstance(value, str):
            save_env(key.upper(), value)

    if state:
        save_state(state)

    old_path.rename(root / "project.yml.bak")
    import typer
    typer.echo(
        "[dcf] Migrated project.yml → .dcf/state.yml + profiles.yml + .env\n"
        "      (project.yml renamed to project.yml.bak — safe to delete)"
    )
