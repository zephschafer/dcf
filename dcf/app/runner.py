from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

RUN_TIMEOUT = int(os.environ.get("DCF_RUN_TIMEOUT", "600"))

_STEP_MSGS = {
    "connected": ("ok",   "connected to source"),
    "iterating": ("info", "fetching records"),
    "writing":   ("info", "writing to warehouse"),
}


def get_step_labels_for_type(source_type: str) -> list[str]:
    if source_type == "http":
        return ["connected", "iterating", "writing", "complete"]
    return ["connected", "writing", "complete"]


def build_steps(labels: list[str], current_phase: str | None, overall_status: str) -> list[dict]:
    if overall_status == "done":
        return [{"label": l, "status": "done"} for l in labels]

    if overall_status == "error":
        idx = labels.index(current_phase) if current_phase and current_phase in labels else 0
        return [
            {"label": l, "status": "done" if i < idx else ("error" if i == idx else "pending")}
            for i, l in enumerate(labels)
        ]

    if not current_phase or current_phase not in labels:
        return [{"label": l, "status": "pending"} for l in labels]
    idx = labels.index(current_phase)
    return [
        {"label": l, "status": "done" if i < idx else ("running" if i == idx else "pending")}
        for i, l in enumerate(labels)
    ]


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _mark_error(run_id: int, db_path: str, message: str) -> None:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        log_entry = json.dumps({"ts": _now_ts(), "cls": "err", "msg": message})
        conn.execute(
            """UPDATE collector_runs
               SET status = 'error', finished_at = datetime('now'), error_message = ?,
                   log = json_insert(COALESCE(log, '[]'), '$[#]', json(?))
               WHERE id = ? AND status = 'running'""",
            (message, log_entry, run_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _sync_run(run_id: int, collector_name: str, project_dir: str, db_path: str) -> None:
    current_phase: str | None = None
    labels: list[str] = []
    conn = None
    started = datetime.now(timezone.utc)

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row

        def update_steps(phase: str) -> None:
            nonlocal current_phase
            current_phase = phase
            steps = build_steps(labels, phase, "running")
            cls, msg = _STEP_MSGS.get(phase, ("info", phase))
            log_entry = json.dumps({"ts": _now_ts(), "cls": cls, "msg": msg})
            conn.execute(
                """UPDATE collector_runs
                   SET steps = ?,
                       log = json_insert(COALESCE(log, '[]'), '$[#]', json(?))
                   WHERE id = ? AND status = 'running'""",
                (json.dumps(steps), log_entry, run_id),
            )
            conn.commit()

        from dcf.config.loader import load_collector
        from dcf.config.models import HttpSource
        from dcf.engine.runner import run_collector
        from dcf.state import get_catalog

        collector_path = Path(project_dir) / "collectors" / f"{collector_name}.yml"
        collector = load_collector(collector_path)

        source_type = "http" if isinstance(collector.source, HttpSource) else collector.source.type
        labels = get_step_labels_for_type(source_type)

        catalog = get_catalog()
        run_collector(collector, catalog=catalog, on_step=update_steps)

        elapsed = int((datetime.now(timezone.utc) - started).total_seconds())
        steps = build_steps(labels, None, "done")
        final_log = json.dumps({"ts": _now_ts(), "cls": "ok", "msg": f"run complete in {elapsed}s"})
        conn.execute(
            """UPDATE collector_runs
               SET status = 'done', steps = ?, finished_at = datetime('now'),
                   log = json_insert(COALESCE(log, '[]'), '$[#]', json(?))
               WHERE id = ? AND status = 'running'""",
            (json.dumps(steps), final_log, run_id),
        )
        conn.commit()

    except Exception as e:
        if conn is not None:
            try:
                steps = build_steps(labels, current_phase, "error")
                error_log = json.dumps({"ts": _now_ts(), "cls": "err", "msg": str(e)})
                conn.execute(
                    """UPDATE collector_runs
                       SET status = 'error', steps = ?, finished_at = datetime('now'),
                           error_message = ?,
                           log = json_insert(COALESCE(log, '[]'), '$[#]', json(?))
                       WHERE id = ? AND status = 'running'""",
                    (json.dumps(steps), str(e), error_log, run_id),
                )
                conn.commit()
            except Exception:
                pass
        else:
            _mark_error(run_id, db_path, str(e))

    finally:
        if conn is not None:
            conn.close()


async def run_in_background(run_id: int, collector_name: str, project_dir: str, db_path: str) -> None:
    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(None, _sync_run, run_id, collector_name, project_dir, db_path)
    try:
        await asyncio.wait_for(asyncio.shield(future), timeout=RUN_TIMEOUT)
    except asyncio.TimeoutError:
        _mark_error(run_id, db_path, f"timed out after {RUN_TIMEOUT}s")
