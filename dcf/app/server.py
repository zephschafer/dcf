from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import yaml
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

from .db import init_db, insert_run, get_latest_runs
from .runner import get_step_labels_for_type, build_steps, run_in_background

PROJECT_DIR = Path(os.environ.get("DCF_PROJECT_DIR", "."))
DB_URL = os.environ.get("DATABASE_URL", "postgresql://dcf:dcf@localhost:5432/dcf")
STATIC_DIR = Path(__file__).parent / "static"

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(DB_URL)
    async with _pool.acquire() as conn:
        await init_db(conn)
    yield
    await _pool.close()


app = FastAPI(lifespan=lifespan)


def _format_elapsed(started_at: datetime | None, finished_at: datetime | None, status: str) -> str | None:
    if started_at is None:
        return None
    now = datetime.now(timezone.utc)
    if status == "running":
        delta = now - started_at
        s = int(delta.total_seconds())
        if s < 60:
            return f"{s}s"
        return f"{s // 60}m {s % 60}s"
    ref = finished_at or started_at
    delta = now - ref
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s ago"
    m = s // 60
    if m < 60:
        return f"{m}m ago"
    h = m // 60
    if h < 24:
        return f"{h}h ago"
    return f"{h // 24}d ago"


def _read_collector_meta(path: Path) -> dict | None:
    try:
        raw = yaml.safe_load(path.read_text())
        return {
            "name": raw.get("name", path.stem),
            "namespace": raw.get("namespace"),
            "source_type": raw.get("source", {}).get("type", "http"),
        }
    except Exception:
        return None


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/collectors")
async def list_collectors():
    collectors_dir = PROJECT_DIR / "collectors"
    yamls = (
        {p.stem: p for p in sorted(collectors_dir.glob("*.yml"))}
        if collectors_dir.exists()
        else {}
    )

    async with _pool.acquire() as conn:
        runs = await get_latest_runs(conn)

    result = []
    for name, path in yamls.items():
        meta = _read_collector_meta(path)
        if meta is None:
            continue

        namespace = meta["namespace"]
        source_type = meta["source_type"]
        labels = get_step_labels_for_type(source_type)

        run = runs.get(name)
        if run:
            steps = run["steps"] if run["steps"] is not None else build_steps(labels, None, "running")
            result.append({
                "name": name,
                "namespace": namespace or "",
                "status": run["status"],
                "steps": steps,
                "lastRan": _format_elapsed(run["started_at"], run["finished_at"], run["status"]),
            })
        else:
            result.append({
                "name": name,
                "namespace": namespace or "",
                "status": "idle",
                "steps": [{"label": l, "status": "pending"} for l in labels],
                "lastRan": None,
            })

    return result


@app.get("/api/collectors/{collector_name}/detail")
async def collector_detail(collector_name: str):
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT status, log, error_message FROM collector_runs
               WHERE collector_name = $1
               ORDER BY started_at DESC LIMIT 1""",
            collector_name,
        )

    log = None
    if row:
        raw = row["log"]
        if raw is not None:
            log = json.loads(raw) if isinstance(raw, str) else raw
        if row["status"] == "running" and not log:
            log = [{"ts": "—", "cls": "info", "msg": "run in progress..."}]

    yaml_path = PROJECT_DIR / "collectors" / f"{collector_name}.yml"
    yaml_content = yaml_path.read_text() if yaml_path.exists() else ""

    return {"log": log, "yaml": yaml_content}


@app.delete("/api/run/{collector_name}")
async def cancel_run(collector_name: str):
    async with _pool.acquire() as conn:
        result = await conn.execute(
            """UPDATE collector_runs
               SET status = 'error', error_message = 'cancelled', finished_at = now()
               WHERE collector_name = $1 AND status = 'running'""",
            collector_name,
        )
    if result == "UPDATE 0":
        return JSONResponse(status_code=404, content={"error": "no active run"})
    return {"cancelled": True}


@app.post("/api/run")
async def trigger_run(body: dict):
    name = body.get("collector", "").strip()
    if not name:
        return JSONResponse(status_code=400, content={"error": "collector name required"})

    collector_path = PROJECT_DIR / "collectors" / f"{name}.yml"
    if not collector_path.exists():
        return JSONResponse(status_code=404, content={"error": f"collector '{name}' not found"})

    meta = _read_collector_meta(collector_path)
    if meta is None:
        return JSONResponse(status_code=422, content={"error": f"could not parse '{name}.yml'"})

    async with _pool.acquire() as conn:
        runs = await get_latest_runs(conn)

    if runs.get(name, {}).get("status") == "running":
        return JSONResponse(status_code=409, content={"error": "already running"})

    async with _pool.acquire() as conn:
        run_id = await insert_run(conn, name, meta["namespace"])

    asyncio.create_task(
        run_in_background(run_id, name, str(PROJECT_DIR), DB_URL)
    )
    return {"run_id": run_id}
