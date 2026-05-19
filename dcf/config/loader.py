from __future__ import annotations

import os
from pathlib import Path

import yaml

from .models import Collector


def _project_config() -> dict:
    """Load project.yml from the project root, returning an empty dict if absent."""
    from ..project import find_project_root
    try:
        cfg_path = find_project_root() / "project.yml"
    except RuntimeError:
        return {}
    if cfg_path.exists():
        return yaml.safe_load(cfg_path.read_text()) or {}
    return {}


def _resolve_env(value: str, project_cfg: dict) -> str:
    """Replace {{ env.VAR }} placeholders.

    Resolution order:
      1. OS environment variable
      2. project.yml key (VAR lowercased, e.g. PORTLANDMAPS_API_KEY → portlandmaps_api_key)
    """
    import re
    def replacer(match):
        var = match.group(1).strip()
        resolved = os.environ.get(var)
        if resolved is None:
            resolved = project_cfg.get(var.lower())
        if not resolved:
            raise EnvironmentError(
                f"'{var}' is not set — add it as an environment variable "
                f"or set '{var.lower()}' in project.yml"
            )
        return resolved
    return re.sub(r"\{\{\s*env\.(\w+)\s*\}\}", replacer, value)


def _resolve_env_in(obj, project_cfg: dict):
    if isinstance(obj, dict):
        return {k: _resolve_env_in(v, project_cfg) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_in(v, project_cfg) for v in obj]
    if isinstance(obj, str):
        return _resolve_env(obj, project_cfg)
    return obj


def load_collector(path: Path, resolve_env: bool = True) -> Collector:
    raw = yaml.safe_load(path.read_text())
    if resolve_env:
        raw = _resolve_env_in(raw, _project_config())
    else:
        raw = _strip_env_placeholders(raw)
    return Collector.from_dict(raw)


def _strip_env_placeholders(obj):
    """Replace {{ env.VAR }} with a placeholder string for structural validation."""
    import re
    if isinstance(obj, dict):
        return {k: _strip_env_placeholders(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_strip_env_placeholders(v) for v in obj]
    if isinstance(obj, str):
        return re.sub(r"\{\{\s*env\.\w+\s*\}\}", "<env>", obj)
    return obj


def load_all_collectors(collectors_dir: Path, resolve_env: bool = True) -> list[Collector]:
    return [load_collector(p, resolve_env=resolve_env) for p in sorted(collectors_dir.rglob("*.yml"))]
