from __future__ import annotations

import json

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS collector_runs (
    id             SERIAL PRIMARY KEY,
    collector_name TEXT NOT NULL,
    namespace      TEXT,
    status         TEXT NOT NULL,
    steps          JSONB,
    log            JSONB,
    started_at     TIMESTAMPTZ DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    error_message  TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_name_started
    ON collector_runs (collector_name, started_at DESC);
"""


async def init_db(conn) -> None:
    await conn.execute(_INIT_SQL)
    # migrate existing tables that predate the log column
    await conn.execute(
        "ALTER TABLE collector_runs ADD COLUMN IF NOT EXISTS log JSONB"
    )
    # Stale "running" rows from a previous crash/restart become error
    await conn.execute("""
        UPDATE collector_runs
        SET status = 'error', error_message = 'interrupted (app restart)', finished_at = now()
        WHERE status = 'running'
    """)


async def insert_run(conn, collector_name: str, namespace: str | None) -> int:
    row = await conn.fetchrow(
        "INSERT INTO collector_runs (collector_name, namespace, status) VALUES ($1, $2, 'running') RETURNING id",
        collector_name,
        namespace,
    )
    return row["id"]


async def get_latest_runs(conn) -> dict[str, dict]:
    rows = await conn.fetch("""
        SELECT DISTINCT ON (collector_name)
            id, collector_name, namespace, status, steps, started_at, finished_at, error_message
        FROM collector_runs
        ORDER BY collector_name, started_at DESC
    """)
    result = {}
    for row in rows:
        d = dict(row)
        if isinstance(d.get("steps"), str):
            d["steps"] = json.loads(d["steps"])
        result[row["collector_name"]] = d
    return result
