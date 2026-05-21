from __future__ import annotations

import re
import shutil
from pathlib import Path

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mGKHJF]')

import typer

app = typer.Typer(help="dcf", no_args_is_help=True)
gcp_app = typer.Typer(help="GCP lake provisioning", no_args_is_help=True)
mcp_app = typer.Typer(help="MCP server for AI-driven collector development", no_args_is_help=True)
app.add_typer(gcp_app, name="gcp")
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
# Cloud deployment target. Commit this file — it contains no secrets.
# Credentials come from: gcloud auth application-default login
default:
  type: gcp
  project_name: my-dcf-project
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
      start: "2025-01-01"
      end: today
      step: 30 days
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
        from .config.models import PubSubSource
        if isinstance(collector.source, PubSubSource):
            typer.echo(
                f"OK — '{collector.name}' (streaming, "
                f"subscription: {collector.source.subscription}, "
                f"{len(collector.source.schema_.columns)} columns)"
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
# gcp                                                                  #
# ------------------------------------------------------------------ #

@gcp_app.command("setup")
def gcp_setup(
    project_id: str = typer.Option(..., "--project-id", "-p", help="GCP project ID"),
    region: str = typer.Option(..., "--region", "-r", help="GCP region (e.g. us-central1)"),
):
    """Provision a GCP data lake. Tip: dcf deploy handles this automatically when profiles.yml is configured."""
    from .deploy.gcp import bootstrap, terraform
    from .deploy.gcp.gcloud import get_credentials

    profile = _load_profile()

    if profile.get("setup_status") in ("running", "complete"):
        typer.echo(f"GCP setup already '{profile['setup_status']}'. Use --force to re-run.", err=True)
        raise typer.Exit(1)

    typer.echo("Checking Google credentials...")
    try:
        credentials = get_credentials()
    except RuntimeError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)
    typer.echo("Credentials OK.")

    profile.update({"project_id": project_id, "region": region, "setup_status": "running"})
    _save_profile(profile)

    try:
        typer.echo("Creating Terraform state bucket...")
        tf_state_bucket = bootstrap.create_state_bucket(project_id, region, credentials)

        typer.echo("Creating service account...")
        sa_email = bootstrap.create_service_account(project_id, credentials)

        typer.echo("Creating service account key...")
        key_data = bootstrap.create_service_account_key(project_id, sa_email, credentials)

        typer.echo("Storing key in Secret Manager...")
        secret_name = bootstrap.store_key_in_secret_manager(project_id, key_data, credentials)

        typer.echo("Provisioning lake infrastructure (terraform apply)...")
        warehouse_bucket = terraform.provision(
            project_id=project_id,
            region=region,
            sa_email=sa_email,
            tf_state_bucket=tf_state_bucket,
        )

        profile.update({
            "sa_email": sa_email,
            "secret_name": secret_name,
            "tf_state_bucket": tf_state_bucket,
            "warehouse_bucket": warehouse_bucket,
            "setup_status": "complete",
            "setup_error": None,
        })
        _save_profile(profile)
        state = _load_state()
        state["catalog"] = "gcp"
        state["active_profile"] = _active_profile_name()
        _save_state(state)

        typer.echo(f"\nGCP lake provisioned successfully!")
        typer.echo(f"  Warehouse bucket: {warehouse_bucket}")
        typer.echo(f"  Service account:  {sa_email}")

    except Exception as e:
        profile.update({"setup_status": "failed", "setup_error": _ANSI_RE.sub("", str(e))})
        _save_profile(profile)
        typer.echo(f"\nSetup failed: {e}", err=True)
        raise typer.Exit(1)


def _gcp_teardown_lake(gcp: dict, credentials) -> list[str]:
    """Destroy GCP lake resources. Returns list of destroyed resource names."""
    from .deploy.gcp import bootstrap, terraform

    destroyed: list[str] = []
    project_id = gcp.get("project_id", "")

    tf_state_bucket = gcp.get("tf_state_bucket", "")
    if tf_state_bucket:
        typer.echo("Running terraform destroy (warehouse bucket)...")
        try:
            terraform.destroy(
                project_id=project_id,
                region=gcp.get("region", ""),
                sa_email=gcp.get("sa_email", ""),
                tf_state_bucket=tf_state_bucket,
            )
            destroyed.append("warehouse bucket")
        except Exception as e:
            typer.echo(f"  terraform destroy failed (continuing): {e}", err=True)

    secret_name = gcp.get("secret_name", "")
    if secret_name:
        typer.echo("Deleting Secret Manager secret...")
        try:
            bootstrap.delete_secret(secret_name, credentials)
            destroyed.append("SA key secret")
        except Exception as e:
            typer.echo(f"  secret delete failed (continuing): {e}", err=True)

    sa_email = gcp.get("sa_email", "")
    if sa_email:
        typer.echo("Deleting service account...")
        try:
            bootstrap.delete_service_account(project_id, sa_email, credentials)
            destroyed.append("service account")
        except Exception as e:
            typer.echo(f"  service account delete failed (continuing): {e}", err=True)

    return destroyed


@gcp_app.command("teardown")
def gcp_teardown(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Destroy GCP lake resources (warehouse bucket, service account, Secret Manager secret)."""
    from .deploy.gcp.gcloud import get_credentials

    profile = _load_profile()

    if profile.get("setup_status") not in ("complete", "failed"):
        typer.echo("No completed GCP setup found in profiles.yml. Nothing to tear down.")
        return

    project_id = profile.get("project_id", "")
    if not yes:
        typer.confirm(
            f"This will destroy all GCP resources for project '{project_id}'. Continue?",
            abort=True,
        )

    try:
        credentials = get_credentials()
    except RuntimeError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    destroyed = _gcp_teardown_lake(profile, credentials)

    for key in ("project_id", "setup_status", "setup_error", "sa_email",
                "secret_name", "tf_state_bucket", "warehouse_bucket", "deployments"):
        profile.pop(key, None)
    _save_profile(profile)
    state = _load_state()
    state["catalog"] = "local"
    state.pop("active_profile", None)
    _save_state(state)

    if destroyed:
        typer.echo(f"\nDestroyed: {', '.join(destroyed)}. Catalog reset to local.")
    else:
        typer.echo("\nNo GCP resources were found to destroy. Catalog reset to local.")


@gcp_app.command("status")
def gcp_status():
    """Show GCP lake setup status."""
    profile = _load_profile()

    if not profile.get("setup_status"):
        typer.echo("No GCP configuration found. Run: dcf gcp setup --project-id X --region Y")
        return

    typer.echo(f"Status:           {profile.get('setup_status', 'unknown')}")
    typer.echo(f"Project ID:       {profile.get('project_id', '-')}")
    typer.echo(f"Region:           {profile.get('region', '-')}")
    typer.echo(f"Warehouse bucket: {profile.get('warehouse_bucket', '-')}")
    typer.echo(f"Service account:  {profile.get('sa_email', '-')}")
    if profile.get("setup_error"):
        typer.echo(f"\nLast error:\n{profile['setup_error']}", err=True)


# ------------------------------------------------------------------ #
# deploy / undeploy                                                    #
# ------------------------------------------------------------------ #

def _require_gcp_config() -> dict:
    """Return the active profile. Exits with a clear error if GCP is not ready."""
    if _get_catalog() != "gcp":
        typer.echo(
            "Error: catalog is not 'gcp'. Deployment requires a GCP data lake.\n"
            "  Configure profiles.yml and run: dcf deploy",
            err=True,
        )
        raise typer.Exit(1)
    profile = _load_profile()
    if profile.get("setup_status") != "complete":
        typer.echo(
            "Error: GCP setup is not complete. Run: dcf gcp setup --project-id X --region Y",
            err=True,
        )
        raise typer.Exit(1)
    for key in ("project_id", "region", "warehouse_bucket", "sa_email"):
        if not profile.get(key):
            typer.echo(f"Error: {key} is missing from profiles.yml. Re-run: dcf gcp setup", err=True)
            raise typer.Exit(1)
    return profile


def _ensure_gcp_provisioned(profile: dict) -> None:
    """Provision GCP lake if not already done, writing state to profiles.yml."""
    stored = _load_profile()

    if stored.get("setup_status") == "complete":
        for key in ("project_id", "region", "warehouse_bucket", "sa_email"):
            if not stored.get(key):
                typer.echo(
                    f"Error: {key} missing from profiles.yml. Re-run dcf deploy.",
                    err=True,
                )
                raise typer.Exit(1)
        return

    project_name = profile.get("project_name", "")
    region = profile.get("region", "")
    if not project_name or project_name == "my-dcf-project":
        typer.echo(
            "Error: set project_name in profiles.yml before deploying to GCP.", err=True
        )
        raise typer.Exit(1)

    from .deploy.gcp.gcloud import get_credentials
    try:
        credentials = get_credentials()
    except RuntimeError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    from .deploy.gcp import bootstrap, terraform

    project_id = stored.get("project_id")
    if project_id:
        typer.echo(f"[dcf] Resuming provisioning for existing project: {project_id}")
    else:
        typer.echo("[dcf] First GCP deploy — provisioning lake (~2 min)...")
        try:
            typer.echo("[dcf] Creating GCP project...")
            project_id = bootstrap.create_project(project_name, credentials)
            typer.echo(f"[dcf] Project created: {project_id}")
        except Exception as e:
            typer.echo(f"\n[dcf] Project creation failed: {e}", err=True)
            raise typer.Exit(1)
        stored.update({"project_id": project_id, "region": region, "setup_status": "running"})
        _save_profile(stored)
        st = _load_state()
        st["catalog"] = "gcp"
        st["active_profile"] = _active_profile_name()
        _save_state(st)

    try:
        tf_state_bucket  = bootstrap.create_state_bucket(project_id, region, credentials)
        sa_email         = bootstrap.create_service_account(project_id, credentials)
        key_data         = bootstrap.create_service_account_key(project_id, sa_email, credentials)
        secret_name      = bootstrap.store_key_in_secret_manager(project_id, key_data, credentials)
        warehouse_bucket = terraform.provision(
            project_id=project_id, region=region,
            sa_email=sa_email, tf_state_bucket=tf_state_bucket,
        )
        stored.update({
            "sa_email": sa_email, "secret_name": secret_name,
            "tf_state_bucket": tf_state_bucket, "warehouse_bucket": warehouse_bucket,
            "setup_status": "complete", "setup_error": None,
        })
        _save_profile(stored)
        typer.echo("[dcf] Lake provisioned.")
    except Exception as e:
        stored.update({"setup_status": "failed", "setup_error": _ANSI_RE.sub("", str(e))})
        _save_profile(stored)
        typer.echo(f"\n[dcf] Provisioning failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def deploy(
    collector_name: str | None = typer.Argument(None, help="Collector name (without .yml), or omit to deploy all"),
    profile: str = typer.Option("default", "--profile", "-p", help="Profile name from profiles.yml"),
):
    """Deploy a collector locally (Docker + Airflow) or to GCP based on catalog in project.yml."""
    if collector_name is None:
        _deploy_all(profile_name=profile)
        return

    _deploy_one(collector_name, profile_name=profile)


def _deploy_all(profile_name: str = "default") -> None:
    """Deploy every collector YAML that has a deployment: block."""
    collectors_dir = _collectors_dir()
    from .config import load_collector

    candidates = []
    for path in sorted(collectors_dir.rglob("*.yml")):
        try:
            collector = load_collector(path, resolve_env=False)
            if collector.deployment is not None:
                candidates.append(path.stem)
        except Exception:
            pass

    if not candidates:
        typer.echo("No collectors with a 'deployment:' block found in collectors/.")
        raise typer.Exit(0)

    typer.echo(f"Deploying {len(candidates)} collector(s): {', '.join(candidates)}")
    failures = []
    for name in candidates:
        typer.echo(f"\n--- {name} ---")
        try:
            _deploy_one(name, profile_name=profile_name)
        except SystemExit:
            failures.append(name)
        except Exception as e:
            typer.echo(f"Deploy failed for '{name}': {e}", err=True)
            failures.append(name)

    if failures:
        typer.echo(f"\nFailed: {', '.join(failures)}", err=True)
        raise typer.Exit(1)


def _deploy_one(collector_name: str, profile_name: str = "default") -> None:
    from .config import load_collector
    from .config.models import PubSubSource

    path = _collectors_dir() / f"{collector_name}.yml"
    if not path.exists():
        typer.echo(f"Collector not found: {path}", err=True)
        raise typer.Exit(1)

    try:
        collector = load_collector(path, resolve_env=False)
    except Exception as e:
        typer.echo(f"Error loading collector: {e}", err=True)
        raise typer.Exit(1)

    if collector.deployment is None:
        typer.echo(
            f"Error: '{collector_name}' has no 'deployment:' block in its collector YAML.\n"
            "For a batch collector, add a deploy block with a schedule:\n\n"
            "  deployment:\n"
            "    schedule: \"0 8 * * *\"\n\n"
            "For a streaming collector (source.type: pubsub), add:\n\n"
            "  deployment:\n"
            "    type: streaming\n"
            "    window_seconds: 60\n",
            err=True,
        )
        raise typer.Exit(1)

    # If profiles.yml is present and targets GCP, auto-provision if needed
    try:
        from .profiles import load_profile
        prof = load_profile(profile_name)
        if prof.get("type") == "gcp":
            _ensure_gcp_provisioned(prof)
    except FileNotFoundError:
        pass  # no profiles.yml — fall through to existing catalog check
    except KeyError as e:
        typer.echo(f"[dcf] {e}", err=True)
        raise typer.Exit(1)

    catalog = _get_catalog()
    deploy_type = collector.deployment.type

    try:
        if catalog == "local":
            from .deploy.local import deploy as local
            subscription = None
            if deploy_type == "streaming":
                if not isinstance(collector.source, PubSubSource):
                    typer.echo(
                        "Error: deploy.type: streaming requires source.type: pubsub", err=True
                    )
                    raise typer.Exit(1)
                subscription = collector.source.subscription
                typer.echo(f"Deploying '{collector_name}' (local streaming, Kafka)...")
            else:
                typer.echo(f"Deploying '{collector_name}' (local batch, Terraform + Airflow)...")

            state = local.deploy(
                collector_name=collector_name,
                deployment=collector.deployment,
                project_root=_project_root(),
                subscription=subscription,
            )
            st = _load_state()
            st.setdefault("deployments", {})[collector_name] = state
            _save_state(st)

            typer.echo(f"\nDeployed '{collector_name}' successfully.")
            if deploy_type == "streaming":
                typer.echo(f"  Type:         streaming (local Docker + Kafka)")
                typer.echo(f"  Kafka:        {state['kafka_container']}  ({state['kafka_external_bootstrap']})")
                typer.echo(f"  Runner:       {state['runner_container']}")
                typer.echo(f"  Warehouse:    {state['warehouse_path']}")
                typer.echo(f"  Window:       {state['window_seconds']}s")
                typer.echo(f"  To publish:   dcf publish {collector_name} '{{\"field\": \"value\"}}'")
            else:
                typer.echo(f"  Type:         batch (local Terraform)")
                typer.echo(f"  Image:        {state['image_tag']}")
                typer.echo(f"  Warehouse:    {state['warehouse_path']}")
                typer.echo(f"  Airflow UI:   {state.get('airflow_url', 'http://localhost:8080')}")
            return

        gcp = _require_gcp_config()
        if deploy_type == "streaming":
            from .deploy.gcp import streaming_deploy
            assert isinstance(collector.source, PubSubSource)
            typer.echo(
                f"Deploying '{collector_name}' (streaming, "
                f"subscription: {collector.source.subscription})..."
            )
            state = streaming_deploy.deploy(
                collector_name=collector_name,
                subscription=collector.source.subscription,
                window_seconds=collector.deployment.window_seconds,
                project_root=_project_root(),
                gcp_config=gcp,
            )
        else:
            from .deploy.gcp import batch_deploy
            typer.echo(f"Deploying '{collector_name}' (schedule: {collector.deployment.schedule})...")
            state = batch_deploy.deploy(
                collector_name=collector_name,
                schedule=collector.deployment.schedule,
                paused=collector.deployment.paused,
                project_root=_project_root(),
                gcp_config=gcp,
            )
    except (typer.Exit, SystemExit):
        raise
    except Exception as e:
        typer.echo(f"\nDeploy failed: {e}", err=True)
        raise typer.Exit(1)

    profile = _load_profile()
    profile.setdefault("deployments", {})[collector_name] = state
    _save_profile(profile)

    typer.echo(f"\nDeployed '{collector_name}' successfully.")
    if deploy_type == "streaming":
        typer.echo(f"  Type:         streaming (GCP Dataflow)")
        typer.echo(f"  Dataflow job: {state['dataflow_job_name']}")
        typer.echo(f"  Subscription: {state['subscription']}")
        typer.echo(f"  Window:       {state['window_seconds']}s")
    else:
        typer.echo(f"  DAG:          {state['dag_id']}")
        typer.echo(f"  Cloud Run:    {state['cloud_run_job']}")
        typer.echo(f"  Schedule:     {state['schedule']}")
        if state.get("airflow_url"):
            typer.echo(f"  Airflow UI:   {state['airflow_url']}")


@app.command()
def undeploy(
    collector_name: str | None = typer.Argument(None, help="Collector name (without .yml). Omit to undeploy everything."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Stop and remove deployed collector(s) (warehouse data is untouched).

    Omit COLLECTOR_NAME to destroy all deployments including the Airflow stack.
    """
    st = _load_state()
    profile = _load_profile()
    local_deps = st.get("deployments", {})
    gcp_deps = profile.get("deployments", {})
    all_deps = {**local_deps, **gcp_deps}

    if collector_name is None:
        catalog = _get_catalog()
        is_gcp = catalog == "gcp" and profile.get("setup_status") in ("complete", "failed")
        project_root = _project_root()
        warehouse_path = project_root / "warehouse"
        has_local_warehouse = catalog == "local" and warehouse_path.exists()

        if not all_deps and not is_gcp and not has_local_warehouse:
            typer.echo("Nothing to undeploy.")
            return

        if not yes:
            if catalog == "local":
                parts = []
                if all_deps:
                    parts.append(f"stop all {len(all_deps)} local deployment(s)")
                if has_local_warehouse:
                    parts.append("permanently delete the local warehouse directory (all collected data will be lost)")
                typer.confirm("This will " + " and ".join(parts) + ". Proceed?", abort=True)
            else:
                msg = f"Destroy all {len(all_deps)} collector deployment(s)"
                if is_gcp:
                    msg += " and all GCP lake infrastructure (warehouse bucket, service account, secrets)"
                typer.confirm(msg + "?", abort=True)

        if catalog == "local":
            try:
                from .deploy.local import deploy as local
                local.undeploy_all(local_deps, project_root)
            except Exception as e:
                typer.echo(f"\nUndeploy failed: {e}", err=True)
                raise typer.Exit(1)

            if has_local_warehouse:
                typer.echo("Deleting local warehouse...")
                shutil.rmtree(warehouse_path)
                typer.echo("Local warehouse deleted.")
        else:
            for name, dep in list(gcp_deps.items()):
                typer.echo(f"Undeploying '{name}'...")
                try:
                    if dep.get("type") == "streaming":
                        from .deploy.gcp import streaming_deploy
                        streaming_deploy.undeploy(
                            collector_name=name, deployment=dep, gcp_config=profile
                        )
                    else:
                        from .deploy.gcp import batch_deploy
                        batch_deploy.undeploy(
                            collector_name=name, deployment=dep,
                            gcp_config=profile, project_root=project_root,
                        )
                except Exception as e:
                    typer.echo(f"  failed (continuing): {e}", err=True)

        st.pop("deployments", None)
        _save_state(st)

        if is_gcp:
            from .deploy.gcp.gcloud import get_credentials
            try:
                credentials = get_credentials()
                destroyed = _gcp_teardown_lake(profile, credentials)
                if destroyed:
                    typer.echo(f"Lake torn down: {', '.join(destroyed)}.")
            except Exception as e:
                typer.echo(f"\nLake teardown failed: {e}", err=True)
            for key in ("project_id", "setup_status", "setup_error", "sa_email",
                        "secret_name", "tf_state_bucket", "warehouse_bucket", "deployments"):
                profile.pop(key, None)
            _save_profile(profile)
            st["catalog"] = "local"
            st.pop("active_profile", None)
            _save_state(st)

        typer.echo("All collectors undeployed.")
        return

    if collector_name not in all_deps:
        typer.echo(
            f"Error: '{collector_name}' is not in deployments. "
            "Nothing to undeploy.",
            err=True,
        )
        raise typer.Exit(1)

    deployment = all_deps[collector_name]
    deploy_type = deployment.get("type", "batch")
    is_local = "kafka_container" in deployment or (
        "image_tag" in deployment and "dag_id" not in deployment
    )

    if not yes:
        if is_local:
            if deploy_type == "streaming":
                typer.confirm(
                    f"Stop and remove local Docker containers for '{collector_name}'? "
                    "(warehouse data will NOT be deleted)",
                    abort=True,
                )
            else:
                typer.confirm(
                    f"Remove local Docker image for '{collector_name}'? "
                    "(warehouse data will NOT be deleted)",
                    abort=True,
                )
        elif deploy_type == "streaming":
            typer.confirm(
                f"Drain and remove Dataflow job '{deployment.get('dataflow_job_name', collector_name)}'? "
                "(warehouse data will NOT be deleted)",
                abort=True,
            )
        else:
            typer.confirm(
                f"Remove collector '{collector_name}' deployment and stop its scheduling? "
                "(warehouse data will NOT be deleted)",
                abort=True,
            )

    typer.echo(f"Undeploying '{collector_name}'...")
    try:
        if is_local:
            from .deploy.local import deploy as local
            local.undeploy(collector_name, deployment, _project_root())
        elif deploy_type == "streaming":
            gcp = _require_gcp_config()
            from .deploy.gcp import streaming_deploy
            streaming_deploy.undeploy(
                collector_name=collector_name,
                deployment=deployment,
                gcp_config=gcp,
            )
        else:
            gcp = _require_gcp_config()
            from .deploy.gcp import batch_deploy
            batch_deploy.undeploy(
                collector_name=collector_name,
                deployment=deployment,
                gcp_config=gcp,
                project_root=_project_root(),
            )
    except Exception as e:
        typer.echo(f"\nUndeploy failed: {e}", err=True)
        raise typer.Exit(1)

    if is_local:
        local_deps.pop(collector_name, None)
        if local_deps:
            st["deployments"] = local_deps
        else:
            st.pop("deployments", None)
        _save_state(st)
    else:
        gcp_deps.pop(collector_name, None)
        if gcp_deps:
            profile["deployments"] = gcp_deps
        else:
            profile.pop("deployments", None)
        _save_profile(profile)

    typer.echo(f"'{collector_name}' undeployed. Warehouse data is untouched.")


@app.command(name="deploy-status")
def deploy_status(
    collector_name: str | None = typer.Argument(None, help="Collector name, or omit for all"),
):
    """Show deployment state for one or all collectors."""
    st = _load_state()
    profile = _load_profile()
    deployments = {**st.get("deployments", {}), **profile.get("deployments", {})}

    if not deployments:
        typer.echo("No collectors are currently deployed.")
        return

    targets = {collector_name: deployments[collector_name]} if collector_name else deployments

    if collector_name and collector_name not in deployments:
        typer.echo(f"'{collector_name}' is not deployed.", err=True)
        raise typer.Exit(1)

    for name, state in targets.items():
        typer.echo(f"\n{name}")
        if "kafka_container" in state:
            typer.echo(f"  Type:         streaming (local Docker + Kafka)")
            typer.echo(f"  Kafka:        {state.get('kafka_container', '-')}  ({state.get('kafka_external_bootstrap', '-')})")
            typer.echo(f"  Runner:       {state.get('runner_container', '-')}")
            typer.echo(f"  Topic:        {state.get('kafka_topic', '-')}")
            typer.echo(f"  Window:       {state.get('window_seconds', '-')}s")
        elif state.get("type") == "batch" and "image_tag" in state and "dag_id" not in state:
            typer.echo(f"  Type:         batch (local Docker)")
            typer.echo(f"  Image:        {state.get('image_tag', '-')}")
        elif state.get("type") == "streaming":
            typer.echo(f"  Type:         streaming (GCP Dataflow)")
            typer.echo(f"  Dataflow job: {state.get('dataflow_job_name', '-')}")
            typer.echo(f"  Subscription: {state.get('subscription', '-')}")
            typer.echo(f"  Window:       {state.get('window_seconds', '-')}s")
        else:
            typer.echo(f"  Type:         batch (GCP)")
            typer.echo(f"  Schedule:     {state.get('schedule', '-')}")
            typer.echo(f"  DAG:          {state.get('dag_id', '-')}")
            typer.echo(f"  Cloud Run:    {state.get('cloud_run_job', '-')}")
            if state.get("airflow_url"):
                typer.echo(f"  Airflow UI:   {state.get('airflow_url', '-')}")
        typer.echo(f"  Deployed at:  {state.get('deployed_at', '-')}")


@app.command()
def publish(
    collector_name: str = typer.Argument(..., help="Collector name"),
    message: str = typer.Argument(..., help="JSON message body to publish"),
    count: int = typer.Option(1, "--count", "-n", help="Number of times to publish the message"),
):
    """Publish a JSON message to the local Kafka topic for a deployed streaming collector."""
    import json

    st = _load_state()
    profile = _load_profile()
    all_deps = {**st.get("deployments", {}), **profile.get("deployments", {})}
    state = all_deps.get(collector_name)
    if not state:
        typer.echo(
            f"Error: '{collector_name}' is not deployed. Run: dcf deploy {collector_name}",
            err=True,
        )
        raise typer.Exit(1)

    if state.get("type") != "streaming":
        typer.echo(
            f"Error: '{collector_name}' is a batch deployment. dcf publish only works for streaming.",
            err=True,
        )
        raise typer.Exit(1)

    if "kafka_topic" not in state:
        typer.echo(
            f"Error: '{collector_name}' is deployed on GCP, not locally. "
            "Use gcloud pubsub topics publish to inject messages.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        json.loads(message)
    except json.JSONDecodeError as e:
        typer.echo(f"Error: message is not valid JSON: {e}", err=True)
        raise typer.Exit(1)

    from .deploy.local import deploy as local
    local.publish(collector_name, state, message, count)

    noun = "message" if count == 1 else "messages"
    typer.echo(f"Published {count} {noun} to topic '{state['kafka_topic']}'.")
    typer.echo(f"Data will appear in warehouse after the {state['window_seconds']}s window closes.")


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
