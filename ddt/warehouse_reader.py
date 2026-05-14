"""
Fast warehouse querying via DuckDB.

For catalog: local  — reads Parquet files from warehouse/{namespace}/{table}/data/*.parquet
For catalog: gcp    — downloads Parquet blobs from GCS via google-cloud-storage,
                      registers them as Arrow tables in DuckDB, then rewrites
                      namespace.table references to the registered names.

list_tables()       returns BOTH GCS and local-only tables when catalog: gcp,
                    with a `location` field ("gcs" | "local") on each row.

Returns at most 500 rows per query.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

_MAX_ROWS = 500

# SQL statement types that must NOT be wrapped in SELECT … LIMIT.
_WRITE_PREFIXES = {"copy", "create", "insert", "drop", "delete", "update", "alter"}


def _project_config() -> dict:
    import yaml
    from .project import find_project_root
    cfg_file = find_project_root() / "project.yml"
    return yaml.safe_load(cfg_file.read_text()) if cfg_file.exists() else {}


def _catalog() -> str:
    return _project_config().get("catalog", "local")


def _warehouse() -> Path:
    from .project import find_project_root
    return find_project_root() / "warehouse"


def _gcs_bucket() -> str:
    return _project_config().get("gcp", {}).get("warehouse_bucket", "")


def _iter_gcs_tables(bucket_name: str) -> list[tuple[str, str]]:
    """List all namespace/table pairs that have data in the GCS warehouse bucket."""
    from google.cloud import storage as gcs
    client = gcs.Client()
    blobs = client.list_blobs(bucket_name)
    seen: set[tuple[str, str]] = set()
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) >= 4 and parts[2] == "data" and parts[3].endswith(".parquet"):
            seen.add((parts[0], parts[1]))
    return sorted(seen)


def _load_gcs_table(bucket_name: str, namespace: str, table: str):
    """Download all Parquet blobs for a GCS table and return a single PyArrow table."""
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import storage as gcs

    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"{namespace}/{table}/data/"
    blobs = [b for b in bucket.list_blobs(prefix=prefix) if b.name.endswith(".parquet")]
    if not blobs:
        return None
    tables = [pq.read_table(io.BytesIO(b.download_as_bytes())) for b in blobs]
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _gcs_table_key(namespace: str, table: str) -> str:
    """DuckDB-safe registered name for a GCS table."""
    return f"_gcs_{namespace}_{table}"


def _is_write_statement(sql: str) -> bool:
    """Return True if sql is a write/DDL statement that must not be wrapped in SELECT … LIMIT."""
    first_word = sql.strip().split()[0].lower() if sql.strip() else ""
    return first_word in _WRITE_PREFIXES


def _iter_local_tables() -> list[tuple[str, str, Path]]:
    """Yield (namespace, table, data_dir) for every local warehouse table with parquet data."""
    warehouse = _warehouse()
    if not warehouse.exists():
        return []
    results = []
    for ns_dir in sorted(warehouse.iterdir()):
        if not ns_dir.is_dir():
            continue
        for table_dir in sorted(ns_dir.iterdir()):
            if not table_dir.is_dir():
                continue
            data_dir = table_dir / "data"
            if data_dir.exists() and list(data_dir.glob("*.parquet")):
                results.append((ns_dir.name, table_dir.name, data_dir))
    return results


def _resolve_table_refs(sql: str, conn, catalog: str) -> str:
    """
    Rewrite namespace.table references in sql to DuckDB-readable form.

    GCS tables (catalog=gcp) → registered as Arrow tables in conn (priority).
    Local tables → rewritten to read_parquet(glob).  In GCP mode this acts as
    a fallback so that local-only tables work transparently without an error (F-021).
    """
    import re

    resolved = sql
    gcs_pairs: set[tuple[str, str]] = set()

    if catalog == "gcp":
        bucket = _gcs_bucket()
        if bucket:
            for namespace, table in _iter_gcs_tables(bucket):
                pattern = rf"\b{re.escape(namespace)}\.{re.escape(table)}\b"
                if re.search(pattern, resolved):
                    arrow_table = _load_gcs_table(bucket, namespace, table)
                    if arrow_table is not None:
                        key = _gcs_table_key(namespace, table)
                        conn.register(key, arrow_table)
                        resolved = re.sub(pattern, key, resolved)
                gcs_pairs.add((namespace, table))

    # Resolve local tables — for local catalog, or as GCP fallback for local-only tables
    for namespace, table, data_dir in _iter_local_tables():
        if (namespace, table) in gcs_pairs:
            continue
        pattern = rf"\b{re.escape(namespace)}\.{re.escape(table)}\b"
        glob = str(data_dir / "*.parquet")
        resolved = re.sub(pattern, f"read_parquet('{glob}')", resolved)

    return resolved


def list_tables() -> list[dict[str, Any]]:
    """
    Return all tables in the warehouse with column schemas and row counts.

    When catalog=gcp, returns BOTH GCS tables (location='gcs') and local-only
    tables that have not been synced to GCS (location='local').
    """
    import duckdb

    catalog = _catalog()
    results: list[dict[str, Any]] = []

    if catalog == "gcp":
        bucket = _gcs_bucket()
        gcs_pairs: set[tuple[str, str]] = set()

        if bucket:
            conn = duckdb.connect()
            for namespace, table in _iter_gcs_tables(bucket):
                arrow_table = _load_gcs_table(bucket, namespace, table)
                if arrow_table is None:
                    continue
                key = _gcs_table_key(namespace, table)
                try:
                    conn.register(key, arrow_table)
                    row_count = conn.execute(f"SELECT COUNT(*) FROM {key}").fetchone()[0]
                    cols = conn.execute(f"DESCRIBE SELECT * FROM {key} LIMIT 0").fetchall()
                    columns = [{"name": c[0], "type": c[1]} for c in cols]
                except Exception as e:
                    row_count = -1
                    columns = [{"error": str(e)}]
                results.append({
                    "namespace": namespace,
                    "table": table,
                    "full_name": f"{namespace}.{table}",
                    "row_count": row_count,
                    "columns": columns,
                    "location": "gcs",
                })
                gcs_pairs.add((namespace, table))
            conn.close()

        # Also list local-only tables not yet in GCS (F-018)
        for namespace, table, data_dir in _iter_local_tables():
            if (namespace, table) in gcs_pairs:
                continue
            glob = str(data_dir / "*.parquet")
            try:
                conn2 = duckdb.connect()
                info = conn2.execute(f"SELECT COUNT(*) as n FROM read_parquet('{glob}')").fetchone()
                row_count = info[0] if info else 0
                cols = conn2.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{glob}') LIMIT 0"
                ).fetchall()
                columns = [{"name": c[0], "type": c[1]} for c in cols]
                conn2.close()
            except Exception as e:
                row_count = -1
                columns = [{"error": str(e)}]
            results.append({
                "namespace": namespace,
                "table": table,
                "full_name": f"{namespace}.{table}",
                "row_count": row_count,
                "columns": columns,
                "location": "local",
            })

        return results

    # local catalog
    for namespace, table, data_dir in _iter_local_tables():
        glob = str(data_dir / "*.parquet")
        try:
            conn = duckdb.connect()
            info = conn.execute(f"SELECT COUNT(*) as n FROM read_parquet('{glob}')").fetchone()
            row_count = info[0] if info else 0
            cols = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{glob}') LIMIT 0"
            ).fetchall()
            columns = [{"name": c[0], "type": c[1]} for c in cols]
            conn.close()
        except Exception as e:
            row_count = -1
            columns = [{"error": str(e)}]
        results.append({
            "namespace": namespace,
            "table": table,
            "full_name": f"{namespace}.{table}",
            "row_count": row_count,
            "columns": columns,
            "location": "local",
        })

    return results


def query(sql: str) -> list[dict[str, Any]]:
    """
    Run a SQL query against the warehouse.

    Table references use the form  namespace.table  — e.g.
        SELECT neighborhood, AVG(CAST(price AS DOUBLE)) as avg_price
        FROM craigslist_apts.craigslist_apts
        GROUP BY 1
        ORDER BY 2 DESC

    Write statements (COPY, CREATE, INSERT, etc.) are executed as-is without
    being wrapped in SELECT … LIMIT.  SELECT queries are automatically capped
    at 500 rows unless the caller includes a LIMIT clause.

    Returns at most 500 rows for SELECT queries.
    """
    import duckdb

    catalog = _catalog()
    conn = duckdb.connect()
    resolved = _resolve_table_refs(sql, conn, catalog)

    # F-019: skip auto-LIMIT for write/DDL statements
    if not _is_write_statement(resolved) and "limit" not in resolved.lower():
        resolved = f"SELECT * FROM ({resolved}) _q LIMIT {_MAX_ROWS}"

    try:
        rows = conn.execute(resolved).fetchall()
    except Exception:
        conn.close()
        raise

    cols = [d[0] for d in conn.description]
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


def materialize_model(sql: str, namespace: str, table: str) -> dict[str, Any]:
    """
    Run sql and write the result as a new warehouse table at namespace/table.

    Writes locally to warehouse/<namespace>/<table>/data/part-001.parquet.
    If catalog=gcp, also uploads the Parquet to the GCS warehouse bucket so
    the model is immediately queryable via query_warehouse() and visible in
    list_warehouse_tables().

    Returns a dict with ok, namespace, table, row_count, and location.
    """
    import duckdb
    import pyarrow.parquet as pq

    catalog = _catalog()
    conn = duckdb.connect()
    resolved = _resolve_table_refs(sql, conn, catalog)

    arrow_result = conn.execute(resolved).arrow()
    if hasattr(arrow_result, "read_all"):
        arrow_result = arrow_result.read_all()  # RecordBatchReader → Table
    row_count = arrow_result.num_rows
    conn.close()

    out_dir = _warehouse() / namespace / table / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-001.parquet"
    pq.write_table(arrow_result, out_path)

    location = str(out_path)

    if catalog == "gcp":
        bucket_name = _gcs_bucket()
        if bucket_name:
            from google.cloud import storage as gcs_storage
            client = gcs_storage.Client()
            gcs_bucket = client.bucket(bucket_name)
            blob_name = f"{namespace}/{table}/data/part-001.parquet"
            blob = gcs_bucket.blob(blob_name)
            blob.upload_from_filename(str(out_path))
            location = f"gs://{bucket_name}/{blob_name}"

    return {
        "ok": True,
        "namespace": namespace,
        "table": table,
        "row_count": row_count,
        "location": location,
    }
