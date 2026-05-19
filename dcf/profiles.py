from __future__ import annotations

from pathlib import Path

import yaml


def _profiles_path() -> Path:
    from .project import find_project_root
    return find_project_root() / "profiles.yml"


def save_profile(name: str, profile: dict) -> None:
    """Write a named profile back to profiles.yml, preserving other profiles."""
    path = _profiles_path()
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    data = data or {}
    data[name] = profile
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def load_profile(name: str = "default") -> dict:
    """Load a named profile from profiles.yml. Raises FileNotFoundError if absent."""
    path = _profiles_path()
    data = yaml.safe_load(path.read_text()) or {}
    if not data:
        raise FileNotFoundError("profiles.yml is empty")
    if name not in data:
        available = list(data.keys())
        raise KeyError(f"Profile '{name}' not found in profiles.yml. Available: {available}")
    return data[name]
