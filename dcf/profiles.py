from __future__ import annotations

from pathlib import Path

import yaml


def _profiles_path() -> Path:
    from .project import find_project_root
    return find_project_root() / "profiles.yml"


def load_profile(name: str = "default") -> dict:
    """Load a named profile from profiles.yml. Raises FileNotFoundError if absent."""
    path = _profiles_path()
    data = yaml.safe_load(path.read_text()) or {}
    if name not in data:
        available = list(data.keys())
        raise KeyError(
            f"Profile '{name}' not found in profiles.yml."
            + (f" Available: {available}" if available else " File is empty.")
        )
    return data[name]
