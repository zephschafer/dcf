"""
pvc MCP server.

Exposes pvc as tools Claude can call directly during a conversation.
Start with: pvc mcp serve

Tools
-----
list_pipelines        — discover what pipelines exist
get_pipeline          — read a pipeline YAML
validate_pipeline     — check YAML is valid without running
run_pipeline          — execute a pipeline (use limit for testing)
list_warehouse_tables — see what's in the warehouse (GCS + local)
query_warehouse       — run SQL against warehouse data (DuckDB, instant)
materialize_model     — run SQL and persist the result as a new warehouse table
write_pipeline        — create or update a pipeline YAML
write_connector       — create or update a Python connector module
"""
from __future__ import annotations

import io
import sys
import traceback
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("pvc")

def _pipelines_dir() -> Path:
    from .project import find_project_root
    return find_project_root() / "pipelines"


def _connectors_dir() -> Path:
    from .project import find_project_root
    return find_project_root() / "connectors"


# ------------------------------------------------------------------ #
# Discovery                                                            #
# ------------------------------------------------------------------ #

@mcp.tool()
def list_pipelines() -> list[dict[str, Any]]:
    """List all pipeline definitions with their key metadata."""
    from .config.loader import load_pipeline

    results = []
    pipelines_dir = _pipelines_dir()
    for path in sorted(pipelines_dir.glob("*.yml")):
        try:
            p = load_pipeline(path, resolve_env=False)
            results.append({
                "name": p.name,
                "description": p.description or "",
                "source_type": p.source.type,
                "build_strategy": p.build.strategy,
                "primary_key": p.build.primary_key,
                "iterate_axes": [
                    {"type": ax.type, "param": getattr(ax, "param", None),
                     "params": getattr(ax, "params", None)}
                    for ax in p.source.iterate
                ],
                "columns": [c.name for c in p.schema_.columns],
            })
        except Exception as e:
            results.append({"name": path.stem, "error": str(e)})
    return results


@mcp.tool()
def get_pipeline(name: str) -> str:
    """Return the raw YAML content of a pipeline definition."""
    pipelines_dir = _pipelines_dir()
    path = pipelines_dir / f"{name}.yml"
    if not path.exists():
        return f"Pipeline '{name}' not found. Available: {[p.stem for p in pipelines_dir.glob('*.yml')]}"
    return path.read_text()


# ------------------------------------------------------------------ #
# Validation & execution                                               #
# ------------------------------------------------------------------ #

@mcp.tool()
def validate_pipeline(name: str) -> dict[str, Any]:
    """Parse and validate a pipeline YAML without running it."""
    from .config.loader import load_pipeline

    path = _pipelines_dir() / f"{name}.yml"
    if not path.exists():
        return {"ok": False, "error": f"Pipeline '{name}' not found"}
    try:
        p = load_pipeline(path, resolve_env=False)
        return {
            "ok": True,
            "name": p.name,
            "params": len(p.source.params),
            "iterate_axes": len(p.source.iterate),
            "columns": len(p.schema_.columns),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def run_pipeline(
    name: str,
    limit: int | None = None,
    params: dict[str, Any] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> str:
    """
    Run a pipeline and return its output.

    For testing, use limit=1 to run only the first iteration.
    Use params to override static param values (e.g. {"max_records": 5}).
    Use start/end (YYYY-MM-DD) to override date range pipelines.
    """
    from .config.loader import load_pipeline
    from .engine.runner import run_pipeline as _run
    from .config.models import DateRangeIterate

    path = _pipelines_dir() / f"{name}.yml"
    if not path.exists():
        return f"Pipeline '{name}' not found"

    try:
        pipeline = load_pipeline(path)
        if start or end:
            for spec in pipeline.source.iterate:
                if isinstance(spec, DateRangeIterate):
                    if start:
                        spec.start = start
                    if end:
                        spec.end = end

        buf = io.StringIO()
        with redirect_stdout(buf):
            _run(pipeline, limit=limit, param_overrides=params or {})
        return buf.getvalue()
    except Exception:
        return traceback.format_exc()


# ------------------------------------------------------------------ #
# Warehouse exploration                                                #
# ------------------------------------------------------------------ #

@mcp.tool()
def list_warehouse_tables() -> list[dict[str, Any]]:
    """
    List all tables in the warehouse with row counts and column schemas.
    Uses DuckDB — no Spark startup required.

    Each entry includes a `location` field:
      "gcs"   — table is in the GCS bucket (queryable by namespace.table)
      "local" — table exists only in the local warehouse (use read_parquet() or
                 materialize_model() to promote it to GCS)
    """
    from .warehouse_reader import list_tables
    return list_tables()


@mcp.tool()
def query_warehouse(sql: str) -> list[dict[str, Any]]:
    """
    Run a SQL query against the local warehouse. Returns up to 500 rows.
    Uses DuckDB — instant results, no Spark startup required.

    Reference tables as  namespace.table  — e.g.:
        SELECT neighborhood, AVG(CAST(price AS DOUBLE)) as avg_price
        FROM craigslist_apts.craigslist_apts
        GROUP BY 1
        ORDER BY 2 DESC

    Table names match the directory structure under warehouse/.
    Use list_warehouse_tables() to discover available tables.
    """
    from .warehouse_reader import query
    try:
        return query(sql)
    except Exception as e:
        return [{"error": str(e)}]


@mcp.tool()
def materialize_model(sql: str, namespace: str, table: str) -> dict[str, Any]:
    """
    Run a SQL query and persist the result as a new warehouse table.

    This is the pvc equivalent of `dbt run` for a single model: it executes
    the SQL, writes the result to warehouse/<namespace>/<table>/data/part-001.parquet,
    and (when catalog=gcp) uploads it to the GCS bucket so it is immediately
    visible in list_warehouse_tables() and queryable via query_warehouse().

    Example — create a repo activity summary model:
        materialize_model(
            sql="SELECT language, COUNT(*) AS repo_count FROM github.github_repos GROUP BY 1",
            namespace="github",
            table="repo_language_summary",
        )

    The sql argument follows the same namespace.table rewriting rules as
    query_warehouse(): reference tables as namespace.table and pvc resolves them.
    """
    from .warehouse_reader import materialize_model as _materialize
    try:
        return _materialize(sql, namespace, table)
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ------------------------------------------------------------------ #
# Writing (enables the full vibe-coding loop)                          #
# ------------------------------------------------------------------ #

@mcp.tool()
def write_pipeline(name: str, yaml_content: str) -> dict[str, Any]:
    """
    Write a pipeline YAML file to pipelines/{name}.yml.
    Creates or overwrites. Validates the YAML before writing.
    """
    from .config.loader import load_pipeline
    import tempfile

    pipelines_dir = _pipelines_dir()
    path = pipelines_dir / f"{name}.yml"

    # Validate before writing
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        tmp = Path(f.name)
    try:
        load_pipeline(tmp, resolve_env=False)
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return {"ok": False, "error": f"Validation failed: {e}"}
    finally:
        tmp.unlink(missing_ok=True)

    pipelines_dir.mkdir(exist_ok=True)
    path.write_text(yaml_content)
    return {"ok": True, "path": str(path)}


@mcp.tool()
def write_connector(name: str, python_content: str) -> dict[str, Any]:
    """
    Write a Python connector module to connectors/{name}.py.
    Creates or overwrites. Used for type:python pipeline sources.
    The module must contain a function matching the pipeline's 'function' field.
    """
    connectors_dir = _connectors_dir()
    connectors_dir.mkdir(exist_ok=True)
    path = connectors_dir / f"{name}.py"
    path.write_text(python_content)
    return {"ok": True, "path": str(path)}


def serve() -> None:
    """Start the MCP server (stdio transport for Claude Desktop)."""
    mcp.run(transport="stdio")
