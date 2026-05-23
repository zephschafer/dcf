from __future__ import annotations

import io
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ..config.models import HttpSource, PythonSource, Param


def _resolve_static_params(params: list[Param]) -> dict[str, Any]:
    return {p.name: p.value for p in params if p.value is not None}


def _rate_limit_sleep(rate_limit) -> None:
    if rate_limit is None:
        return
    sleep_secs = (rate_limit.per_minutes * 60) / rate_limit.requests
    time.sleep(sleep_secs)


def _get_nested(record: dict, path: str) -> Any:
    """Resolve a dot-notation path into a nested dict."""
    parts = path.split(".")
    val = record
    for part in parts:
        if not isinstance(val, dict):
            return None
        val = val.get(part)
    return val


def _parse_response(response: requests.Response, source: HttpSource) -> list[dict]:
    fmt = source.response.format

    if fmt == "csv":
        try:
            df = pd.read_csv(io.StringIO(response.text))
        except pd.errors.EmptyDataError:
            return []
        return df.to_dict(orient="records")

    if fmt == "json":
        data = response.json()
        if source.response.records_path:
            for key in source.response.records_path.split("."):
                if not isinstance(data, dict):
                    raise ValueError(
                        f"records_path '{source.response.records_path}' could not be followed: "
                        f"expected a JSON object at key '{key}' but found {type(data).__name__}. "
                        f"If the response is a top-level array, omit records_path entirely."
                    )
                data = data.get(key, [])
        if isinstance(data, list):
            return data
        return [data]

    raise ValueError(f"Unsupported response format: '{fmt}'")


def _fetch_http(source: HttpSource, dynamic_params: dict[str, Any]) -> list[dict]:
    params = _resolve_static_params(source.params)
    params.update(dynamic_params)

    if source.auth and source.auth.type == "query_param":
        params[source.auth.key] = source.auth.value

    headers = {}
    if source.auth and source.auth.type == "header":
        headers[source.auth.key] = source.auth.value
    if source.auth and source.auth.type == "bearer":
        headers["Authorization"] = f"Bearer {source.auth.value}"

    _rate_limit_sleep(source.rate_limit)

    response = requests.request(
        method=source.method,
        url=source.url,
        params=params,
        headers=headers,
        timeout=60,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError:
        status = response.status_code
        hint = {
            401: "Check that your API token or key is correct and has not expired.",
            403: "Your credentials may lack the required permissions for this endpoint.",
            404: "The URL may be wrong, or this resource does not exist.",
            429: "Rate limit exceeded. Add a rate_limit block to your collector YAML to slow down requests.",
        }.get(status, "")
        msg = f"HTTP {status} from {source.url}"
        if hint:
            msg += f" — {hint}"
        raise requests.HTTPError(msg, response=response)
    return _parse_response(response, source)


def _fetch_python(source: PythonSource, dynamic_params: dict[str, Any]) -> list[dict]:
    from ..project import find_project_root
    project_root = str(find_project_root())
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    import importlib
    mod = importlib.import_module(source.module)
    fn = getattr(mod, source.function)
    return fn(dynamic_params)


def fetch_records(source, dynamic_params: dict[str, Any]) -> list[dict]:
    if isinstance(source, PythonSource):
        return _fetch_python(source, dynamic_params)
    return _fetch_http(source, dynamic_params)


# ------------------------------------------------------------------ #
# SQL source                                                           #
# ------------------------------------------------------------------ #

def _normalize_sql_value(v: Any) -> Any:
    from decimal import Decimal
    import uuid as _uuid
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, _uuid.UUID):
        return str(v)
    if isinstance(v, (bytes, memoryview)):
        return None
    return v


def _get_sql_connection(connection):
    import psycopg2
    if connection.type == "postgres":
        return psycopg2.connect(
            host=connection.host,
            port=connection.port,
            database=connection.database,
            user=connection.user,
            password=connection.password,
        )
    # cloud_sql: Cloud Run mounts the socket at /cloudsql/{connection_name}
    # For local dev, run cloud-sql-proxy to expose the same socket path
    return psycopg2.connect(
        host=f"/cloudsql/{connection.instance}",
        database=connection.database,
        user=connection.user,
        password=connection.password,
    )


def fetch_sql_table(source, table) -> list[dict]:
    conn = _get_sql_connection(source.connection)
    try:
        with conn.cursor() as cur:
            cols = ", ".join(table.columns) if table.columns else "*"
            cur.execute(f"SELECT {cols} FROM {table.table}")
            names = [d[0] for d in cur.description]
            return [
                {k: _normalize_sql_value(v) for k, v in zip(names, row)}
                for row in cur.fetchall()
            ]
    finally:
        conn.close()
