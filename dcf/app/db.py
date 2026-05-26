from __future__ import annotations

import json
from datetime import datetime, timezone


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


_INIT_SQL = """
CREATE TABLE IF NOT EXISTS collector_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_name TEXT NOT NULL,
    namespace      TEXT,
    status         TEXT NOT NULL,
    steps          TEXT,
    log            TEXT,
    started_at     TEXT DEFAULT (datetime('now')),
    finished_at    TEXT,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_name_started
    ON collector_runs (collector_name, started_at DESC);
"""


async def init_db(conn) -> None:
    await conn.executescript(_INIT_SQL)
    await conn.execute("PRAGMA journal_mode=WAL")
    # migrate tables that predate the log column
    try:
        await conn.execute("ALTER TABLE collector_runs ADD COLUMN log TEXT")
        await conn.commit()
    except Exception:
        pass  # column already exists
    # stale running rows from a previous crash become errors
    await conn.execute("""
        UPDATE collector_runs
        SET status = 'error', error_message = 'interrupted (app restart)',
            finished_at = datetime('now')
        WHERE status = 'running'
    """)
    await conn.commit()


async def insert_run(conn, collector_name: str, namespace: str | None) -> int:
    cursor = await conn.execute(
        "INSERT INTO collector_runs (collector_name, namespace, status) VALUES (?, ?, 'running')",
        (collector_name, namespace),
    )
    await conn.commit()
    return cursor.lastrowid


async def get_latest_runs(conn) -> dict[str, dict]:
    async with conn.execute("""
        SELECT id, collector_name, namespace, status, steps, log,
               started_at, finished_at, error_message
        FROM collector_runs
        WHERE (collector_name, started_at) IN (
            SELECT collector_name, MAX(started_at)
            FROM collector_runs
            GROUP BY collector_name
        )
        ORDER BY collector_name
    """) as cursor:
        rows = await cursor.fetchall()

    result = {}
    for row in rows:
        d = dict(row)
        if isinstance(d.get("steps"), str):
            try:
                d["steps"] = json.loads(d["steps"])
            except Exception:
                d["steps"] = None
        d["started_at"] = _parse_dt(d.get("started_at"))
        d["finished_at"] = _parse_dt(d.get("finished_at"))
        result[row["collector_name"]] = d
    return result
