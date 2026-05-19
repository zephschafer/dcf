from __future__ import annotations

import os
from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    """Return the dcf project root directory.

    Resolution order:
      1. DCF_PROJECT_DIR environment variable (absolute path)
      2. Walk up from `start` (default: cwd) looking for profiles.yml
    """
    if env := os.environ.get("DCF_PROJECT_DIR"):
        return Path(env).resolve()
    start = (start or Path.cwd()).resolve()
    for p in [start, *start.parents]:
        if (p / "profiles.yml").exists():
            return p
    raise RuntimeError(
        "No profiles.yml found in current directory or any parent. "
        "Run 'dcf init' to create one, or set DCF_PROJECT_DIR."
    )
