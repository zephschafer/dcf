from __future__ import annotations

from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("dcf")


def _project_root() -> Path:
    from .project import find_project_root
    return find_project_root()


@mcp.tool()
def list_collectors() -> list[dict[str, Any]]:
    """List all collectors in the project's collectors/ directory."""
    from .config.loader import load_collector
    from pydantic import ValidationError

    try:
        root = _project_root()
    except RuntimeError as e:
        return [{"ok": False, "error": str(e)}]

    collectors_dir = root / "collectors"
    if not collectors_dir.exists():
        return []

    results = []
    for path in sorted(collectors_dir.rglob("*.yml")):
        try:
            c = load_collector(path, resolve_env=False)
            results.append({
                "name": c.name,
                "source_type": c.source.type,
                "namespace": c.namespace or c.name,
                "file": path.name,
            })
        except (ValidationError, Exception) as e:
            results.append({
                "name": path.stem,
                "source_type": "unknown",
                "namespace": None,
                "file": path.name,
                "parse_error": str(e),
            })
    return results


@mcp.tool()
def read_collector(name: str) -> dict[str, Any]:
    """Return the raw YAML content of a named collector."""
    try:
        root = _project_root()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}

    path = root / "collectors" / f"{name}.yml"
    if not path.exists():
        return {"ok": False, "error": f"Collector '{name}' not found at collectors/{name}.yml"}

    return {"ok": True, "name": name, "yaml": path.read_text()}


@mcp.tool()
def write_collector(name: str, yaml_content: str) -> dict[str, Any]:
    """Write or overwrite collectors/{name}.yml with the given YAML content."""
    try:
        root = _project_root()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}

    collectors_dir = root / "collectors"
    collectors_dir.mkdir(exist_ok=True)
    path = collectors_dir / f"{name}.yml"
    path.write_text(yaml_content)
    return {"ok": True, "path": str(path)}


@mcp.tool()
def validate_collector(name: str) -> dict[str, Any]:
    """Parse and validate a collector YAML. Returns ok or a structured list of errors."""
    from .config.loader import load_collector
    from pydantic import ValidationError

    try:
        root = _project_root()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}

    path = root / "collectors" / f"{name}.yml"
    if not path.exists():
        return {"ok": False, "error": f"Collector '{name}' not found"}

    try:
        c = load_collector(path, resolve_env=False)
        return {
            "ok": True,
            "name": c.name,
            "source_type": c.source.type,
            "namespace": c.namespace or c.name,
        }
    except ValidationError as e:
        errors = [
            {"loc": ".".join(str(x) for x in err["loc"]), "msg": err["msg"]}
            for err in e.errors()
        ]
        return {"ok": False, "errors": errors}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def query_warehouse(sql: str) -> dict[str, Any]:
    """Run a SQL query against the dcf warehouse. Reference tables as namespace.table_name."""
    from .warehouse_reader import query as run_query

    try:
        rows = run_query(sql)
        return {"ok": True, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def get_status() -> dict[str, Any]:
    """Return warehouse catalog type, location, and all tables with row counts."""
    from .state import get_catalog, load_state
    from .warehouse_reader import list_tables

    try:
        root = _project_root()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}

    try:
        catalog = get_catalog()
        state = load_state()
        gcp = state.get("gcp") or {}

        if catalog == "gcp":
            warehouse_location = f"gs://{gcp.get('warehouse_bucket', '(unknown)')}"
        else:
            warehouse_location = str(root / "warehouse")

        tables = list_tables()
        return {
            "ok": True,
            "catalog": catalog,
            "warehouse_location": warehouse_location,
            "tables": tables,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
def deploy(profile: str = "default") -> dict[str, Any]:
    """Provision the GCS warehouse bucket and start the local dcf app."""
    from .profiles import load_profile
    from .state import load_state, save_state
    from .deploy.gcp.deploy import deploy as gcp_deploy
    from .app.launch import launch_app

    try:
        root = _project_root()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}

    try:
        prof = load_profile(profile)
    except FileNotFoundError:
        return {"ok": False, "error": "profiles.yml not found. Run: dcf init"}
    except KeyError:
        return {"ok": False, "error": f"Profile '{profile}' not found in profiles.yml"}

    if prof.get("type") != "gcp":
        return {
            "ok": False,
            "error": (
                f"Profile '{profile}' has type '{prof.get('type')}'. "
                "Local catalog requires no deployment."
            ),
        }

    try:
        gcp_state = gcp_deploy(prof)
    except Exception as e:
        try:
            from google.auth.exceptions import DefaultCredentialsError
            if isinstance(e, DefaultCredentialsError):
                return {
                    "ok": False,
                    "error": (
                        "No Google credentials found. "
                        "Run: gcloud auth application-default login"
                    ),
                }
        except ImportError:
            pass
        return {"ok": False, "error": str(e)}

    st = load_state()
    st["catalog"] = "gcp"
    st["active_profile"] = profile
    st["gcp"] = gcp_state
    save_state(st)

    app_status = "started"
    try:
        launch_app(root)
    except Exception as e:
        app_status = f"warning: {e}"

    return {
        "ok": True,
        "project_id": gcp_state["project_id"],
        "region": gcp_state["region"],
        "warehouse_bucket": gcp_state["warehouse_bucket"],
        "setup_status": gcp_state["setup_status"],
        "app_url": "http://localhost:8080",
        "app_status": app_status,
    }


def serve() -> None:
    """Start the MCP server on stdio. Called by `dcf mcp serve`."""
    mcp.run(transport="stdio")
