"""
Warehouse querying via DuckDB + PyIceberg.

Tables are stored as Apache Iceberg tables, discovered via a SqlCatalog
backed by .dcf/catalog.db (SQLite). Spark/Hadoop Iceberg tables written
by staging+merge are also readable via StaticTable.from_metadata().

list_tables()       returns all tables with schemas and row counts.
query(sql)          executes SQL with namespace.table refs auto-resolved.
materialize_model() runs SQL and writes the result as an Iceberg table.

Returns at most 500 rows per SELECT query.
"""
from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings(
    "ignore",
    message="Your application has authenticated using end user credentials",
    category=UserWarning,
)

_MAX_ROWS = 500
_WRITE_PREFIXES = {"copy", "create", "insert", "drop", "delete", "update", "alter"}


def _catalog_type() -> str:
    try:
        from .state import get_catalog
        return get_catalog()
    except RuntimeError:
        return "local"


def _warehouse() -> Path:
    from .project import find_project_root
    return find_project_root() / "warehouse"


def _get_catalog(catalog_type: str | None = None):
    from .catalog import get_catalog
    return get_catalog(catalog_type)


def _is_write_statement(sql: str) -> bool:
    first_word = sql.strip().split()[0].lower() if sql.strip() else ""
    return first_word in _WRITE_PREFIXES


# ------------------------------------------------------------------ #
# Table discovery                                                       #
# ------------------------------------------------------------------ #

def _iter_iceberg_tables(catalog_type: str) -> list[tuple[str, str]]:
    """
    Return (namespace, table) pairs for all Iceberg tables.

    Includes both PyIceberg SqlCatalog-registered tables and Spark Hadoop
    Iceberg tables (staging+merge output) found on the local filesystem.
    For GCS catalog, only SqlCatalog-registered tables are listed.
    """
    results: set[tuple[str, str]] = set()
    cat = _get_catalog(catalog_type)

    # 1. SqlCatalog-registered tables (PyIceberg-written, covers both local + gcs)
    try:
        for ns_tuple in cat.list_namespaces():
            ns = ns_tuple[0] if ns_tuple else str(ns_tuple)
            for tbl_id in cat.list_tables(ns):
                tbl_name = tbl_id[-1]
                results.add((ns, tbl_name))
    except Exception:
        pass

    # 2. Spark Hadoop Iceberg tables (local only — staging+merge)
    if catalog_type == "local":
        warehouse = _warehouse()
        if warehouse.exists():
            for ns_dir in sorted(warehouse.iterdir()):
                if not ns_dir.is_dir():
                    continue
                for tbl_dir in sorted(ns_dir.iterdir()):
                    if not tbl_dir.is_dir():
                        continue
                    meta_dir = tbl_dir / "metadata"
                    if meta_dir.exists() and list(meta_dir.glob("*.metadata.json")):
                        results.add((ns_dir.name, tbl_dir.name))

    return sorted(results)


def _load_iceberg_table(catalog, identifier: tuple[str, str]):
    """
    Load an Iceberg table by identifier.

    Tries SqlCatalog first (PyIceberg-written tables), then falls back to
    StaticTable.from_metadata for Spark Hadoop Iceberg tables on disk.
    Returns None if no table is found.
    """
    from pyiceberg.exceptions import NoSuchTableError

    try:
        return catalog.load_table(identifier)
    except (NoSuchTableError, Exception):
        pass

    return _load_hadoop_iceberg_table(identifier)


def _load_hadoop_iceberg_table(identifier: tuple[str, str]):
    """Load a Spark Hadoop Iceberg table by scanning its metadata/ directory."""
    from pyiceberg.table import StaticTable

    namespace, table_name = identifier
    meta_dir = _warehouse() / namespace / table_name / "metadata"
    if not meta_dir.exists():
        return None

    # Prefer version-hint.text (Hadoop catalog canonical pointer)
    version_hint = meta_dir / "version-hint.text"
    if version_hint.exists():
        try:
            v = version_hint.read_text().strip()
            candidate = meta_dir / f"v{v}.metadata.json"
            if candidate.exists():
                return StaticTable.from_metadata(candidate.as_uri())
        except Exception:
            pass

    # Fall back to the alphabetically-last metadata file
    files = sorted(meta_dir.glob("*.metadata.json"))
    if not files:
        return None
    try:
        return StaticTable.from_metadata(files[-1].as_uri())
    except Exception:
        return None


# ------------------------------------------------------------------ #
# Query helpers                                                         #
# ------------------------------------------------------------------ #

def _resolve_table_refs(sql: str, conn, catalog_type: str) -> str:
    """
    Rewrite namespace.table references in sql to DuckDB-registered Arrow tables.

    For each word.word pattern found in sql, attempts to load it as an Iceberg
    table. If found, registers the Arrow scan result in conn and rewrites the
    reference. Unknown patterns are left as-is for DuckDB to resolve natively.
    """
    cat = _get_catalog(catalog_type)
    resolved = sql

    candidates = set(re.findall(r'\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b', resolved))
    for namespace, table in candidates:
        tbl = _load_iceberg_table(cat, (namespace, table))
        if tbl is None:
            continue
        try:
            arrow = tbl.scan().to_arrow()
        except Exception:
            continue
        key = f"_iceberg_{namespace}_{table}"
        conn.register(key, arrow)
        resolved = re.sub(
            rf"\b{re.escape(namespace)}\.{re.escape(table)}\b",
            f"{key} AS {table}",
            resolved,
        )

    return resolved


# ------------------------------------------------------------------ #
# Public API                                                            #
# ------------------------------------------------------------------ #

def list_tables() -> list[dict[str, Any]]:
    """Return all warehouse tables with column schemas and row counts."""
    import duckdb

    catalog_type = _catalog_type()
    cat = _get_catalog(catalog_type)
    results: list[dict[str, Any]] = []

    for namespace, table_name in _iter_iceberg_tables(catalog_type):
        identifier = (namespace, table_name)
        tbl = _load_iceberg_table(cat, identifier)
        if tbl is None:
            continue

        try:
            arrow = tbl.scan().to_arrow()
            conn = duckdb.connect()
            conn.register("_tbl", arrow)
            row_count = conn.execute("SELECT COUNT(*) FROM _tbl").fetchone()[0]
            cols = conn.execute("DESCRIBE SELECT * FROM _tbl LIMIT 0").fetchall()
            columns = [{"name": c[0], "type": c[1]} for c in cols]
            conn.close()
        except Exception as e:
            row_count = -1
            columns = [{"error": str(e)}]

        # Determine location based on where the table data lives
        location = "gcs" if catalog_type == "gcp" else "local"
        try:
            loc_uri = tbl.location()
            if loc_uri and loc_uri.startswith("gs://"):
                location = "gcs"
        except Exception:
            pass

        results.append({
            "namespace": namespace,
            "table": table_name,
            "full_name": f"{namespace}.{table_name}",
            "row_count": row_count,
            "columns": columns,
            "location": location,
        })

    return results


def query(sql: str) -> list[dict[str, Any]]:
    """
    Run a SQL query against the warehouse.

    Table references use the form  namespace.table  — e.g.
        SELECT * FROM stackoverflow.so_questions

    SELECT queries are automatically capped at 500 rows unless the caller
    includes a LIMIT clause. Write statements (COPY, CREATE, INSERT, etc.)
    are executed as-is.
    """
    import duckdb

    catalog_type = _catalog_type()
    conn = duckdb.connect()
    resolved = _resolve_table_refs(sql, conn, catalog_type)

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
    Run sql and write the result as a new Iceberg warehouse table.

    Returns a dict with ok, namespace, table, row_count, and location.
    """
    import duckdb
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids as pyarrow_to_schema

    catalog_type = _catalog_type()
    conn = duckdb.connect()
    resolved = _resolve_table_refs(sql, conn, catalog_type)

    arrow_result = conn.execute(resolved).arrow()
    if hasattr(arrow_result, "read_all"):
        arrow_result = arrow_result.read_all()
    row_count = arrow_result.num_rows
    conn.close()

    cat = _get_catalog(catalog_type)
    identifier = (namespace, table)

    if not cat.namespace_exists((namespace,)):
        cat.create_namespace((namespace,))

    try:
        tbl = cat.load_table(identifier)
        tbl.overwrite(arrow_result)
    except NoSuchTableError:
        schema = pyarrow_to_schema(arrow_result.schema)
        tbl = cat.create_table(identifier, schema=schema)
        tbl.append(arrow_result)

    return {
        "ok": True,
        "namespace": namespace,
        "table": table,
        "row_count": row_count,
        "location": tbl.location(),
    }
