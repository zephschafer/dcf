from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(help="dcf", no_args_is_help=True)
mcp_app = typer.Typer(help="MCP server for AI-driven collector development", no_args_is_help=True)
app.add_typer(mcp_app, name="mcp")

def _project_root() -> Path:
    from .project import find_project_root
    return find_project_root()


def _collectors_dir() -> Path:
    return _project_root() / "collectors"


def _get_catalog() -> str:
    from .state import get_catalog
    return get_catalog()


def _active_profile_name() -> str:
    from .state import get_active_profile_name
    return get_active_profile_name()


def _load_profile() -> dict:
    from .profiles import load_profile
    try:
        return load_profile(_active_profile_name())
    except (FileNotFoundError, KeyError):
        return {}


def _save_profile(profile: dict) -> None:
    from .profiles import save_profile
    save_profile(_active_profile_name(), profile)


def _load_state() -> dict:
    from .state import load_state
    return load_state()


def _save_state(state: dict) -> None:
    from .state import save_state
    save_state(state)


def _load_gcp_state() -> dict:
    return _load_state().get("gcp") or {}


def _save_gcp_state(gcp: dict) -> None:
    st = _load_state()
    st["gcp"] = gcp
    _save_state(st)


def _prompt_for_missing_var(var: str) -> str:
    import sys
    if not sys.stdin.isatty():
        raise EnvironmentError(
            f"'{var}' is not set — add it as an environment variable "
            f"or set it in .env"
        )
    value = typer.prompt(f"Enter value for {var}", hide_input=True)
    from .state import save_env
    save_env(var, value)
    typer.echo(f"Saved '{var}' to .env.")
    return value


# ------------------------------------------------------------------ #
# init                                                                 #
# ------------------------------------------------------------------ #

_PYPROJECT_TEMPLATE = """\
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dcf-core",
]

[tool.uv]
package = false
"""

_GITIGNORE_CONTENT = """\
warehouse/
.dcf/
.env
.venv/
__pycache__/
"""

_PROFILES_YML_TEMPLATE = """\
default:
  type: gcp
  project_id: my-gcp-project
  region: us-central1
"""

_EXAMPLE_COLLECTOR = """\
name: so_questions
namespace: stackoverflow

source:
  type: http
  url: https://api.stackexchange.com/2.3/questions
  response:
    records_path: items
  params:
    - name: site
      type: string
      value: stackoverflow
    - name: tagged
      type: string
      value: "python;data-engineering"
  schema:
    columns:
      - name: question_id
        path: question_id
        type: integer
      - name: title
        path: title
        type: string
      - name: creation_date
        path: creation_date
        type: integer

cadence:
  strategy: incremental
  primary_key: question_id
  iterate:
    - type: date_range
      params: [fromdate, todate]
      format: "%s"
      start: "2026-01-01"
      end: today
      step: 30 days

# deployment:
#   schedule: "0 8 * * *"   # cron expression — required
#   paused: false             # optional, default false
"""


@app.command()
def init():
    """Scaffold a new dcf project."""
    from .project import find_project_root

    try:
        root = find_project_root()
    except RuntimeError:
        root = Path.cwd()

    created = []

    pyproject = root / "pyproject.toml"
    if not pyproject.exists():
        pyproject.write_text(_PYPROJECT_TEMPLATE.format(name=root.name))
        created.append("pyproject.toml")

    gitignore = root / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(_GITIGNORE_CONTENT)
        created.append(".gitignore")

    collectors_dir = root / "collectors"
    if not collectors_dir.exists():
        collectors_dir.mkdir()
        created.append("collectors/")

    example = collectors_dir / "so_questions.yml"
    if not example.exists():
        example.write_text(_EXAMPLE_COLLECTOR)
        created.append("collectors/so_questions.yml")

    profiles = root / "profiles.yml"
    if not profiles.exists():
        profiles.write_text(_PROFILES_YML_TEMPLATE)
        created.append("profiles.yml")

    claude_commands = root / ".claude" / "commands"
    claude_commands.mkdir(parents=True, exist_ok=True)
    skill_dest = claude_commands / "new-collector.md"
    if not skill_dest.exists():
        import importlib.resources as resources
        skill_content = resources.files("dcf").joinpath("skills/new-collector.md").read_text()
        skill_dest.write_text(skill_content)
        created.append(".claude/commands/new-collector.md")

    if created:
        typer.echo(f"Created: {', '.join(created)}")
    typer.echo("\nNext steps:")
    typer.echo("  uv sync")
    typer.echo("  uv run dcf run so_questions")


# ------------------------------------------------------------------ #
# import                                                               #
# ------------------------------------------------------------------ #

@app.command(name="import")
def import_(
    source: str = typer.Argument(..., metavar="SOURCE",
        help="hub name, user/repo, or pypi:name"),
    name: str | None = typer.Option(None, "--name", "-n",
        help="Override the output filename (without .yml)"),
):
    """Import a collector template from dcf-hub, a GitHub repo, or a PyPI package."""
    from .hub import resolve, required_env_vars

    collectors_dir = _collectors_dir()

    try:
        yaml_content, suggested_name = resolve(source)
    except RuntimeError as e:
        typer.echo(f"[dcf] Error: {e}", err=True)
        raise typer.Exit(1)

    out_name = name or suggested_name
    dest = collectors_dir / f"{out_name}.yml"

    if dest.exists():
        typer.echo(
            f"[dcf] collectors/{dest.name} already exists. "
            "Use --name to import under a different filename.",
            err=True,
        )
        raise typer.Exit(1)

    dest.write_text(yaml_content)
    typer.echo(f"[dcf] Imported → collectors/{dest.name}")

    env_vars = required_env_vars(yaml_content)
    if env_vars:
        typer.echo("\nRequired env vars:")
        for var in env_vars:
            typer.echo(f"  export {var}=...")


# ------------------------------------------------------------------ #
# run                                                                  #
# ------------------------------------------------------------------ #

@app.command()
def run(
    collector_name: str = typer.Argument(..., help="Collector name (without .yml) or 'all'"),
    start: str | None = typer.Option(None, help="Override backfill start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="Override backfill end date (YYYY-MM-DD)"),
    limit: int | None = typer.Option(None, help="Run only the first N iterations"),
    param: list[str] = typer.Option([], help="Override a param value: key=value (repeatable)"),
):
    """Run one or all collectors."""
    from .config import load_collector, load_all_collectors
    from .engine import run_collector

    catalog = _get_catalog()
    param_overrides = _parse_params(param)

    collectors_dir = _collectors_dir()
    if collector_name == "all":
        collectors = load_all_collectors(collectors_dir)
    else:
        path = collectors_dir / f"{collector_name}.yml"
        if not path.exists():
            typer.echo(f"Collector not found: {path}", err=True)
            raise typer.Exit(1)
        collectors = [load_collector(path, on_missing=_prompt_for_missing_var)]

    for collector in collectors:
        if start or end:
            _override_date_range(collector, start, end)
        run_collector(collector, catalog=catalog, limit=limit, param_overrides=param_overrides)


# ------------------------------------------------------------------ #
# validate                                                             #
# ------------------------------------------------------------------ #


@app.command()
def validate(
    collector_name: str = typer.Argument(..., help="Collector name (without .yml) or 'all'"),
):
    """Parse and validate collector YAML without running it."""
    from .config import load_collector, load_all_collectors

    collectors_dir = _collectors_dir()
    if collector_name == "all":
        collectors = load_all_collectors(
            collectors_dir, resolve_env=True, on_missing=_prompt_for_missing_var
        )
        names = [c.name for c in collectors]
        typer.echo(f"OK — {len(collectors)} collector(s): {', '.join(names)}")
    else:
        path = collectors_dir / f"{collector_name}.yml"
        if not path.exists():
            typer.echo(f"Collector not found: {path}", err=True)
            raise typer.Exit(1)
        try:
            collector = load_collector(path, resolve_env=True, on_missing=_prompt_for_missing_var)
        except Exception as e:
            from pydantic import ValidationError
            if isinstance(e, ValidationError):
                for err in e.errors():
                    loc = ".".join(str(x) for x in err["loc"])
                    typer.echo(f"Validation error in '{collector_name}': {loc} — {err['msg']}", err=True)
            else:
                typer.echo(f"Error loading '{collector_name}': {e}", err=True)
            raise typer.Exit(1)
        from .config.models import SqlSource
        if isinstance(collector.source, SqlSource):
            tables = ", ".join(t.table for t in collector.source.tables)
            typer.echo(
                f"OK — '{collector.name}' (sql, "
                f"{len(collector.source.tables)} tables: {tables})"
            )
        else:
            typer.echo(f"OK — '{collector.name}' ({len(collector.source.params)} params, "
                       f"{len(collector.cadence.iterate)} cadence axes, "
                       f"{len(collector.source.schema_.columns)} columns)")


# ------------------------------------------------------------------ #
# query                                                                #
# ------------------------------------------------------------------ #

@app.command()
def query(
    sql: str | None = typer.Argument(None, help="SQL query string"),
    file: Path | None = typer.Option(None, "--file", "-f", help="Path to a .sql file"),
):
    """Run a SQL query against the warehouse and print results."""
    from .warehouse_reader import query as run_query

    if sql is None and file is None:
        typer.echo("Error: provide a SQL string or --file <path>.sql", err=True)
        raise typer.Exit(1)
    if sql is not None and file is not None:
        typer.echo("Error: provide either a SQL string or --file, not both", err=True)
        raise typer.Exit(1)

    if file is not None:
        if not file.exists():
            typer.echo(f"Error: file not found: {file}", err=True)
            raise typer.Exit(1)
        sql = file.read_text()

    try:
        rows = run_query(sql)
    except Exception as e:
        typer.echo(f"Query error: {e}", err=True)
        raise typer.Exit(1)

    if not rows:
        typer.echo("0 rows")
        return

    from rich.console import Console
    from rich.table import Table

    table = Table(show_header=True, header_style="bold")
    for col in rows[0].keys():
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(v) if v is not None else "" for v in row.values()])

    Console().print(table)
    noun = "row" if len(rows) == 1 else "rows"
    typer.echo(f"{len(rows)} {noun}")


# ------------------------------------------------------------------ #
# deploy                                                               #
# ------------------------------------------------------------------ #

@app.command()
def deploy(
    profile: str = typer.Option("default", "--profile", "-p", help="Profile name from profiles.yml"),
):
    """Create or connect to the cloud warehouse lakehouse and update local state.

    Reads project_id and region from profiles.yml.
    Requires Application Default Credentials: gcloud auth application-default login
    Safe to re-run — bucket creation is idempotent.
    """
    from .profiles import load_profile
    from google.auth.exceptions import DefaultCredentialsError

    try:
        prof = load_profile(profile)
    except FileNotFoundError:
        typer.echo("Error: profiles.yml not found. Run: dcf init", err=True)
        raise typer.Exit(1)
    except KeyError:
        typer.echo(f"Error: profile '{profile}' not found in profiles.yml", err=True)
        raise typer.Exit(1)

    if prof.get("type") != "gcp":
        typer.echo(
            f"Error: profile '{profile}' has type '{prof.get('type')}'. "
            "Local catalog needs no deployment.",
            err=True,
        )
        raise typer.Exit(1)

    from .deploy.gcp.deploy import deploy as gcp_deploy

    typer.echo(f"[dcf] Deploying warehouse for profile '{profile}'...")
    try:
        gcp_state = gcp_deploy(prof)
    except DefaultCredentialsError:
        typer.echo(
            "Error: No Google credentials found.\n"
            "  Run: gcloud auth application-default login",
            err=True,
        )
        raise typer.Exit(1)
    except RuntimeError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    st = _load_state()
    st["catalog"] = "gcp"
    st["active_profile"] = profile
    st["gcp"] = gcp_state
    _save_state(st)

    typer.echo("[dcf] Warehouse ready.")
    typer.echo(f"  Bucket:     gs://{gcp_state['warehouse_bucket']}")
    typer.echo(f"  Project ID: {gcp_state['project_id']}")
    typer.echo(f"  Region:     {gcp_state['region']}")
    typer.echo("\nRun collectors with: dcf run <collector_name>")


# ------------------------------------------------------------------ #
# status                                                               #
# ------------------------------------------------------------------ #

@app.command()
def status():
    """Show the current warehouse location and catalog type."""
    catalog = _get_catalog()
    typer.echo(f"Catalog: {catalog}")

    if catalog == "gcp":
        gcp = _load_gcp_state()
        bucket = gcp.get("warehouse_bucket", "-")
        project_id = gcp.get("project_id", "-")
        region = gcp.get("region", "-")
        setup_status = gcp.get("setup_status", "unknown")
        typer.echo(f"Warehouse: gs://{bucket}")
        typer.echo(f"Project:   {project_id}")
        typer.echo(f"Region:    {region}")
        typer.echo(f"Status:    {setup_status}")
    else:
        try:
            root = _project_root()
            typer.echo(f"Warehouse: {root / 'warehouse'}")
        except RuntimeError:
            typer.echo("Warehouse: (project root not found)")
        typer.echo("Run 'dcf deploy' to provision a cloud warehouse.")


# ------------------------------------------------------------------ #
# mcp                                                                  #
# ------------------------------------------------------------------ #

@mcp_app.command("serve")
def mcp_serve():
    """Start the dcf MCP server (stdio transport for Claude Desktop)."""
    from .mcp_server import serve
    serve()


@mcp_app.command("setup-desktop")
def mcp_setup_desktop():
    """Register dcf as an MCP server in Claude Desktop's config."""
    import json
    import shutil

    claude_config = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    if not claude_config.exists():
        typer.echo(f"Claude Desktop config not found at {claude_config}", err=True)
        typer.echo("Is Claude Desktop installed?", err=True)
        raise typer.Exit(1)

    project_dir = str(_project_root())
    uv_path = shutil.which("uv") or "uv"

    cfg = json.loads(claude_config.read_text()) if claude_config.stat().st_size else {}
    cfg.setdefault("mcpServers", {})
    cfg["mcpServers"]["dcf"] = {
        "command": uv_path,
        "args": ["--directory", project_dir, "run", "dcf", "mcp", "serve"],
    }
    claude_config.write_text(json.dumps(cfg, indent=2))
    typer.echo(f"Registered dcf MCP server in {claude_config}")
    typer.echo("Restart Claude Desktop to pick up the change.")


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _parse_params(raw: list[str]) -> dict:
    result = {}
    for item in raw:
        if "=" not in item:
            typer.echo(f"Invalid --param format (expected key=value): '{item}'", err=True)
            raise typer.Exit(1)
        k, v = item.split("=", 1)
        for cast in (int, float):
            try:
                v = cast(v)
                break
            except ValueError:
                pass
        result[k.strip()] = v
    return result


def _override_date_range(collector, start: str | None, end: str | None) -> None:
    from .config.models import DateRangeIterate
    for spec in collector.cadence.iterate:
        if isinstance(spec, DateRangeIterate):
            if start:
                spec.start = start
            if end:
                spec.end = end
