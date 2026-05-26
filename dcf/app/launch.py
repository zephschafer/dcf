from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path


def launch_app(project_root: Path) -> None:
    dcf_dir = project_root / ".dcf"
    dcf_dir.mkdir(exist_ok=True)

    log_file = open(dcf_dir / "app.log", "w")
    proc = subprocess.Popen(
        ["uv", "run", "uvicorn", "dcf.app.server:app", "--host", "0.0.0.0", "--port", "8080"],
        cwd=str(project_root),
        env={**os.environ, "DCF_PROJECT_DIR": str(project_root)},
        stdout=log_file,
        stderr=log_file,
    )
    (dcf_dir / "app.pid").write_text(str(proc.pid))


def stop_app(project_root: Path) -> None:
    pid_file = project_root / ".dcf" / "app.pid"
    if not pid_file.exists():
        raise FileNotFoundError("No .dcf/app.pid found — has dcf deploy been run?")

    pid = int(pid_file.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # already stopped
    pid_file.unlink()
