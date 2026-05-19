from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

import yaml

from .models import Collector


def _project_config() -> dict:
    """Load .env from the project root as a lowercase-keyed dict for env var resolution."""
    try:
        from ..state import load_env
        return {k.lower(): v for k, v in load_env().items()}
    except RuntimeError:
        return {}


def _resolve_env(
    value: str,
    project_cfg: dict,
    on_missing: Callable[[str], str] | None = None,
) -> str:
    """Replace {{ env.VAR }} placeholders.

    Resolution order:
      1. OS environment variable
      2. .env file key (VAR lowercased, e.g. PORTLANDMAPS_API_KEY → portlandmaps_api_key)
      3. on_missing callback (if provided), which may prompt the user
    """
    import re
    def replacer(match):
        var = match.group(1).strip()
        resolved = os.environ.get(var)
        if resolved is None:
            resolved = project_cfg.get(var.lower())
        if not resolved:
            if on_missing is not None:
                resolved = on_missing(var)
                project_cfg[var.lower()] = resolved
                return resolved
            raise EnvironmentError(
                f"'{var}' is not set — add it as an environment variable "
                f"or set '{var.lower()}' in .env"
            )
        return resolved
    return re.sub(r"\{\{\s*env\.(\w+)\s*\}\}", replacer, value)


def _resolve_env_in(
    obj,
    project_cfg: dict,
    on_missing: Callable[[str], str] | None = None,
):
    if isinstance(obj, dict):
        return {k: _resolve_env_in(v, project_cfg, on_missing) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_in(v, project_cfg, on_missing) for v in obj]
    if isinstance(obj, str):
        return _resolve_env(obj, project_cfg, on_missing)
    return obj


def load_collector(
    path: Path,
    resolve_env: bool = True,
    on_missing: Callable[[str], str] | None = None,
) -> Collector:
    raw = yaml.safe_load(path.read_text())
    if resolve_env:
        raw = _resolve_env_in(raw, _project_config(), on_missing)
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


def load_all_collectors(
    collectors_dir: Path,
    resolve_env: bool = True,
    on_missing: Callable[[str], str] | None = None,
) -> list[Collector]:
    return [
        load_collector(p, resolve_env=resolve_env, on_missing=on_missing)
        for p in sorted(collectors_dir.rglob("*.yml"))
    ]
