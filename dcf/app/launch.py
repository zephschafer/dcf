from __future__ import annotations

import os
import signal
import socket
import subprocess
from pathlib import Path

_PORT = 8080


def _port_in_use() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", _PORT)) == 0


def launch_app(project_root: Path) -> None:
    if _port_in_use():
        raise RuntimeError(
            f"Port {_PORT} is already in use. "
            "Stop the existing process (dcf undeploy, or docker compose down) before starting the app."
        )

    dcf_dir = project_root / ".dcf"
    dcf_dir.mkdir(exist_ok=True)

    log_file = open(dcf_dir / "app.log", "w")
    proc = subprocess.Popen(
        ["uv", "run", "uvicorn", "dcf.app.server:app", "--host", "0.0.0.0", "--port", str(_PORT)],
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
