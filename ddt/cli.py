from __future__ import annotations

import os
import re
from pathlib import Path

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*[mGKHJF]')

import typer
import yaml

app = typer.Typer(help="ddt", no_args_is_help=True)
gcp_app = typer.Typer(help="GCP lake provisioning", no_args_is_help=True)
mcp_app = typer.Typer(help="MCP server for AI-driven pipeline development", no_args_is_help=True)
app.add_typer(gcp_app, name="gcp")
app.add_typer(mcp_app, name="mcp")

def _project_root() -> Path:
    from .project import find_project_root
    return find_project_root()


def _pipelines_dir() -> Path:
    return _project_root() / "pipelines"


def _load_config() -> dict:
    cfg = _project_root() / "project.yml"
    if not cfg.exists():
        return {}
    return yaml.safe_load(cfg.read_text()) or {}


def _save_config(config: dict) -> None:
    cfg = _project_root() / "project.yml"
    cfg.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))


def _get_catalog() -> str:
    return _load_config().get("catalog", "local")


# ------------------------------------------------------------------ #
# init                                                                 #
# ------------------------------------------------------------------ #

@app.command()
def init(
    catalog: str = typer.Option("local", prompt="Catalog type (local or gcp)", show_default=True),
):
    """Create or update project.yml configuration."""
    cfg = _load_config()
    cfg["catalog"] = catalog
    _save_config(cfg)
    typer.echo(f"Config saved to {_project_root() / 'project.yml'}")
    typer.echo("\nTo store API keys, add them to project.yml as plain keys:")
    typer.echo("  my_api_key: sk-xxxx")
    typer.echo("Then reference them in pipeline YAML: value: \"{{ env.MY_API_KEY }}\"")
    typer.echo("Or set them as environment variables before running.")


# ------------------------------------------------------------------ #
# run                                                                  #
# ------------------------------------------------------------------ #

@app.command()
def run(
    pipeline_name: str = typer.Argument(..., help="Pipeline name (without .yml) or 'all'"),
    start: str | None = typer.Option(None, help="Override backfill start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="Override backfill end date (YYYY-MM-DD)"),
    limit: int | None = typer.Option(None, help="Run only the first N iterations"),
    param: list[str] = typer.Option([], help="Override a param value: key=value (repeatable)"),
):
    """Run one or all pipelines."""
    from .config import load_pipeline, load_all_pipelines
    from .engine import run_pipeline

    catalog = _get_catalog()
    param_overrides = _parse_params(param)

    pipelines_dir = _pipelines_dir()
    if pipeline_name == "all":
        pipelines = load_all_pipelines(pipelines_dir)
    else:
        path = pipelines_dir / f"{pipeline_name}.yml"
        if not path.exists():
            typer.echo(f"Pipeline not found: {path}", err=True)
            raise typer.Exit(1)
        pipelines = [load_pipeline(path)]

    for pipeline in pipelines:
        if start or end:
            _override_date_range(pipeline, start, end)
        run_pipeline(pipeline, catalog=catalog, limit=limit, param_overrides=param_overrides)


# ------------------------------------------------------------------ #
# validate                                                             #
# ------------------------------------------------------------------ #

def _check_unset_env_refs(path: Path) -> list[str]:
    """Return sorted list of {{ env.VAR }} references in a YAML file that are not currently set."""
    raw = path.read_text()
    refs = re.findall(r"\{\{\s*env\.(\w+)\s*\}\}", raw)
    cfg = _load_config()
    return sorted({v for v in refs if not os.environ.get(v) and not cfg.get(v.lower())})


@app.command()
def validate(
    pipeline_name: str = typer.Argument(..., help="Pipeline name (without .yml) or 'all'"),
):
    """Parse and validate pipeline YAML without running it."""
    from .config import load_pipeline, load_all_pipelines

    pipelines_dir = _pipelines_dir()
    if pipeline_name == "all":
        pipelines = load_all_pipelines(pipelines_dir, resolve_env=False)
        names = [p.name for p in pipelines]
        for path in sorted(pipelines_dir.glob("*.yml")):
            unset = _check_unset_env_refs(path)
            if unset:
                typer.echo(
                    f"  WARNING '{path.stem}': these env vars are not set: {', '.join(unset)}\n"
                    f"    Set them as environment variables or add to project.yml before running.",
                    err=True,
                )
        typer.echo(f"OK — {len(pipelines)} pipeline(s): {', '.join(names)}")
    else:
        path = pipelines_dir / f"{pipeline_name}.yml"
        if not path.exists():
            typer.echo(f"Pipeline not found: {path}", err=True)
            raise typer.Exit(1)
        try:
            pipeline = load_pipeline(path, resolve_env=False)
        except Exception as e:
            from pydantic import ValidationError
            if isinstance(e, ValidationError):
                for err in e.errors():
                    loc = ".".join(str(x) for x in err["loc"])
                    typer.echo(f"Validation error in '{pipeline_name}': {loc} — {err['msg']}", err=True)
            else:
                typer.echo(f"Error loading '{pipeline_name}': {e}", err=True)
            raise typer.Exit(1)
        unset = _check_unset_env_refs(path)
        if unset:
            typer.echo(
                f"WARNING: these env vars are referenced but not set: {', '.join(unset)}\n"
                f"  Set them as environment variables or add to project.yml before running.",
                err=True,
            )
        from .config.models import PubSubSource
        if isinstance(pipeline.source, PubSubSource):
            typer.echo(
                f"OK — '{pipeline.name}' (streaming, "
                f"subscription: {pipeline.source.subscription}, "
                f"{len(pipeline.schema_.columns)} columns)"
            )
        else:
            typer.echo(f"OK — '{pipeline.name}' ({len(pipeline.source.params)} params, "
                       f"{len(pipeline.source.iterate)} iterate axes, "
                       f"{len(pipeline.schema_.columns)} columns)")


# ------------------------------------------------------------------ #
# gcp                                                                  #
# ------------------------------------------------------------------ #

@gcp_app.command("setup")
def gcp_setup(
    project_id: str = typer.Option(..., "--project-id", "-p", help="GCP project ID"),
    region: str = typer.Option(..., "--region", "-r", help="GCP region (e.g. us-central1)"),
):
    """Provision a GCP data lake."""
    from .gcp import bootstrap, terraform
    from .gcp.gcloud import get_credentials

    cfg = _load_config()
    gcp = cfg.get("gcp", {})

    if gcp.get("setup_status") in ("running", "complete"):
        typer.echo(f"GCP setup already '{gcp['setup_status']}'. Use --force to re-run.", err=True)
        raise typer.Exit(1)

    typer.echo("Checking Google credentials...")
    try:
        credentials = get_credentials()
    except RuntimeError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)
    typer.echo("Credentials OK.")

    gcp.update({"project_id": project_id, "region": region, "setup_status": "running"})
    cfg["gcp"] = gcp
    _save_config(cfg)

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

        gcp.update({
            "sa_email": sa_email,
            "secret_name": secret_name,
            "tf_state_bucket": tf_state_bucket,
            "warehouse_bucket": warehouse_bucket,
            "setup_status": "complete",
            "setup_error": None,
        })
        cfg["gcp"] = gcp
        _save_config(cfg)

        typer.echo(f"\nGCP lake provisioned successfully!")
        typer.echo(f"  Warehouse bucket: {warehouse_bucket}")
        typer.echo(f"  Service account:  {sa_email}")

    except Exception as e:
        gcp.update({"setup_status": "failed", "setup_error": _ANSI_RE.sub("", str(e))})
        cfg["gcp"] = gcp
        _save_config(cfg)
        typer.echo(f"\nSetup failed: {e}", err=True)
        raise typer.Exit(1)


@gcp_app.command("teardown")
def gcp_teardown(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Destroy GCP lake resources (warehouse bucket, service account, Secret Manager secret)."""
    from .gcp import bootstrap, terraform
    from .gcp.gcloud import get_credentials

    cfg = _load_config()
    gcp = cfg.get("gcp", {})

    if not gcp or gcp.get("setup_status") not in ("complete", "failed"):
        typer.echo("No completed GCP setup found in project.yml. Nothing to tear down.")
        return

    project_id = gcp.get("project_id", "")
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

    destroyed: list[str] = []

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

    cfg.pop("gcp", None)
    cfg["catalog"] = "local"
    _save_config(cfg)

    if destroyed:
        typer.echo(f"\nDestroyed: {', '.join(destroyed)}. project.yml reset to catalog: local.")
    else:
        typer.echo("\nNo GCP resources were found to destroy. project.yml reset to catalog: local.")


@gcp_app.command("status")
def gcp_status():
    """Show GCP lake setup status."""
    cfg = _load_config()
    gcp = cfg.get("gcp")

    if not gcp:
        typer.echo("No GCP configuration found. Run: ddt gcp setup --project-id X --region Y")
        return

    typer.echo(f"Status:           {gcp.get('setup_status', 'unknown')}")
    typer.echo(f"Project ID:       {gcp.get('project_id', '-')}")
    typer.echo(f"Region:           {gcp.get('region', '-')}")
    typer.echo(f"Warehouse bucket: {gcp.get('warehouse_bucket', '-')}")
    typer.echo(f"Service account:  {gcp.get('sa_email', '-')}")
    if gcp.get("setup_error"):
        typer.echo(f"\nLast error:\n{gcp['setup_error']}", err=True)


# ------------------------------------------------------------------ #
# deploy / undeploy                                                    #
# ------------------------------------------------------------------ #

def _require_gcp_config() -> tuple[dict, dict]:
    """Return (full_config, gcp_section). Exits with a clear error if GCP is not ready."""
    cfg = _load_config()
    if cfg.get("catalog") != "gcp":
        typer.echo(
            "Error: catalog is not 'gcp'. Deployment requires a GCP data lake.\n"
            "  Set catalog: gcp in project.yml or run: ddt init",
            err=True,
        )
        raise typer.Exit(1)
    gcp = cfg.get("gcp", {})
    if gcp.get("setup_status") != "complete":
        typer.echo(
            "Error: GCP setup is not complete. Run: ddt gcp setup --project-id X --region Y",
            err=True,
        )
        raise typer.Exit(1)
    for key in ("project_id", "region", "warehouse_bucket", "sa_email"):
        if not gcp.get(key):
            typer.echo(f"Error: gcp.{key} is missing from project.yml. Re-run: ddt gcp setup", err=True)
            raise typer.Exit(1)
    return cfg, gcp


@app.command()
def deploy(
    pipeline_name: str = typer.Argument(..., help="Pipeline name (without .yml)"),
):
    """Deploy a pipeline locally (Docker) or to GCP based on catalog in project.yml."""
    from .config import load_pipeline
    from .config.models import PubSubSource

    path = _pipelines_dir() / f"{pipeline_name}.yml"
    if not path.exists():
        typer.echo(f"Pipeline not found: {path}", err=True)
        raise typer.Exit(1)

    try:
        pipeline = load_pipeline(path, resolve_env=False)
    except Exception as e:
        typer.echo(f"Error loading pipeline: {e}", err=True)
        raise typer.Exit(1)

    if pipeline.deploy is None:
        typer.echo(
            f"Error: '{pipeline_name}' has no 'deploy:' block in its pipeline YAML.\n"
            "For a batch pipeline, add a deploy block with a schedule:\n\n"
            "  deploy:\n"
            "    schedule: \"0 8 * * *\"\n\n"
            "For a streaming pipeline (source.type: pubsub), add:\n\n"
            "  deploy:\n"
            "    type: streaming\n"
            "    window_seconds: 60\n",
            err=True,
        )
        raise typer.Exit(1)

    catalog = _get_catalog()
    deploy_type = pipeline.deploy.type

    try:
        if catalog == "local":
            from . import local_deploy
            subscription = None
            if deploy_type == "streaming":
                if not isinstance(pipeline.source, PubSubSource):
                    typer.echo(
                        "Error: deploy.type: streaming requires source.type: pubsub", err=True
                    )
                    raise typer.Exit(1)
                subscription = pipeline.source.subscription
                typer.echo(f"Deploying '{pipeline_name}' (local streaming, Kafka)...")
            else:
                typer.echo(f"Deploying '{pipeline_name}' (local batch, Docker)...")

            cfg = _load_config()
            state = local_deploy.deploy(
                pipeline_name=pipeline_name,
                deployment=pipeline.deploy,
                project_root=_project_root(),
                subscription=subscription,
            )
            cfg.setdefault("deployments", {})[pipeline_name] = state
            _save_config(cfg)

            typer.echo(f"\nDeployed '{pipeline_name}' successfully.")
            if deploy_type == "streaming":
                typer.echo(f"  Type:         streaming (local Docker + Kafka)")
                typer.echo(f"  Kafka:        {state['kafka_container']}  ({state['kafka_external_bootstrap']})")
                typer.echo(f"  Runner:       {state['runner_container']}")
                typer.echo(f"  Warehouse:    {state['warehouse_path']}")
                typer.echo(f"  Window:       {state['window_seconds']}s")
                typer.echo(f"  To publish:   ddt publish {pipeline_name} '{{\"field\": \"value\"}}'")
            else:
                typer.echo(f"  Type:         batch (local Docker)")
                typer.echo(f"  Image:        {state['image_tag']}")
                typer.echo(f"  Warehouse:    {state['warehouse_path']}")
                typer.echo(
                    f"  To run again: docker run --rm "
                    f"-e PIPELINE_NAME={pipeline_name} "
                    f"-v {state['warehouse_path']}:/app/warehouse "
                    f"{state['image_tag']}"
                )
            return

        cfg, gcp = _require_gcp_config()
        if deploy_type == "streaming":
            from .gcp import streaming_deploy
            assert isinstance(pipeline.source, PubSubSource)
            typer.echo(
                f"Deploying '{pipeline_name}' (streaming, "
                f"subscription: {pipeline.source.subscription})..."
            )
            state = streaming_deploy.deploy(
                pipeline_name=pipeline_name,
                subscription=pipeline.source.subscription,
                window_seconds=pipeline.deploy.window_seconds,
                project_root=_project_root(),
                gcp_config=gcp,
            )
        else:
            from .gcp import batch_deploy
            typer.echo(f"Deploying '{pipeline_name}' (schedule: {pipeline.deploy.schedule})...")
            state = batch_deploy.deploy(
                pipeline_name=pipeline_name,
                schedule=pipeline.deploy.schedule,
                paused=pipeline.deploy.paused,
                project_root=_project_root(),
                gcp_config=gcp,
            )
    except (typer.Exit, SystemExit):
        raise
    except Exception as e:
        typer.echo(f"\nDeploy failed: {e}", err=True)
        raise typer.Exit(1)

    cfg.setdefault("deployments", {})[pipeline_name] = state
    _save_config(cfg)

    typer.echo(f"\nDeployed '{pipeline_name}' successfully.")
    if deploy_type == "streaming":
        typer.echo(f"  Type:         streaming (GCP Dataflow)")
        typer.echo(f"  Dataflow job: {state['dataflow_job_name']}")
        typer.echo(f"  Subscription: {state['subscription']}")
        typer.echo(f"  Window:       {state['window_seconds']}s")
    else:
        typer.echo(f"  DAG:          {state['dag_id']}")
        typer.echo(f"  Cloud Run:    {state['cloud_run_job']}")
        typer.echo(f"  Composer env: {state['composer_env']}")
        typer.echo(f"  Schedule:     {state['schedule']}")


@app.command()
def undeploy(
    pipeline_name: str = typer.Argument(..., help="Pipeline name (without .yml)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Stop and remove a deployed pipeline (warehouse data is untouched)."""
    cfg = _load_config()
    deployments = cfg.get("deployments", {})

    if pipeline_name not in deployments:
        typer.echo(
            f"Error: '{pipeline_name}' is not in project.yml deployments. "
            "Nothing to undeploy.",
            err=True,
        )
        raise typer.Exit(1)

    deployment = deployments[pipeline_name]
    catalog = _get_catalog()
    deploy_type = deployment.get("type", "batch")
    is_local = "kafka_container" in deployment or (
        "image_tag" in deployment and "dag_id" not in deployment
    )

    if not yes:
        if is_local:
            if deploy_type == "streaming":
                typer.confirm(
                    f"Stop and remove local Docker containers for '{pipeline_name}'? "
                    "(warehouse data will NOT be deleted)",
                    abort=True,
                )
            else:
                typer.confirm(
                    f"Remove local Docker image for '{pipeline_name}'? "
                    "(warehouse data will NOT be deleted)",
                    abort=True,
                )
        elif deploy_type == "streaming":
            typer.confirm(
                f"Drain and remove Dataflow job '{deployment.get('dataflow_job_name', pipeline_name)}'? "
                "(warehouse data will NOT be deleted)",
                abort=True,
            )
        else:
            typer.confirm(
                f"Remove Cloud Run job '{deployment.get('cloud_run_job', pipeline_name)}' and "
                f"Composer DAG '{deployment.get('dag_id', pipeline_name)}'? "
                "(warehouse data will NOT be deleted)",
                abort=True,
            )

    typer.echo(f"Undeploying '{pipeline_name}'...")
    try:
        if is_local:
            from . import local_deploy
            local_deploy.undeploy(pipeline_name, deployment)
        elif deploy_type == "streaming":
            _, gcp = _require_gcp_config()
            from .gcp import streaming_deploy
            streaming_deploy.undeploy(
                pipeline_name=pipeline_name,
                deployment=deployment,
                gcp_config=gcp,
            )
        else:
            _, gcp = _require_gcp_config()
            from .gcp import batch_deploy
            batch_deploy.undeploy(
                pipeline_name=pipeline_name,
                deployment=deployment,
                gcp_config=gcp,
            )
    except Exception as e:
        typer.echo(f"\nUndeploy failed: {e}", err=True)
        raise typer.Exit(1)

    del cfg["deployments"][pipeline_name]
    if not cfg["deployments"]:
        del cfg["deployments"]
    _save_config(cfg)

    typer.echo(f"'{pipeline_name}' undeployed. Warehouse data is untouched.")


@app.command(name="deploy-status")
def deploy_status(
    pipeline_name: str | None = typer.Argument(None, help="Pipeline name, or omit for all"),
):
    """Show deployment state for one or all pipelines."""
    cfg = _load_config()
    deployments = cfg.get("deployments", {})

    if not deployments:
        typer.echo("No pipelines are currently deployed.")
        return

    targets = {pipeline_name: deployments[pipeline_name]} if pipeline_name else deployments

    if pipeline_name and pipeline_name not in deployments:
        typer.echo(f"'{pipeline_name}' is not deployed.", err=True)
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
            typer.echo(f"  Composer env: {state.get('composer_env', '-')}")
        typer.echo(f"  Deployed at:  {state.get('deployed_at', '-')}")


@app.command()
def publish(
    pipeline_name: str = typer.Argument(..., help="Pipeline name"),
    message: str = typer.Argument(..., help="JSON message body to publish"),
    count: int = typer.Option(1, "--count", "-n", help="Number of times to publish the message"),
):
    """Publish a JSON message to the local Kafka topic for a deployed streaming pipeline."""
    import json

    cfg = _load_config()
    state = cfg.get("deployments", {}).get(pipeline_name)
    if not state:
        typer.echo(
            f"Error: '{pipeline_name}' is not deployed. Run: ddt deploy {pipeline_name}",
            err=True,
        )
        raise typer.Exit(1)

    if state.get("type") != "streaming":
        typer.echo(
            f"Error: '{pipeline_name}' is a batch deployment. ddt publish only works for streaming.",
            err=True,
        )
        raise typer.Exit(1)

    if "kafka_topic" not in state:
        typer.echo(
            f"Error: '{pipeline_name}' is deployed on GCP, not locally. "
            "Use gcloud pubsub topics publish to inject messages.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        json.loads(message)
    except json.JSONDecodeError as e:
        typer.echo(f"Error: message is not valid JSON: {e}", err=True)
        raise typer.Exit(1)

    from . import local_deploy
    local_deploy.publish(pipeline_name, state, message, count)

    noun = "message" if count == 1 else "messages"
    typer.echo(f"Published {count} {noun} to topic '{state['kafka_topic']}'.")
    typer.echo(f"Data will appear in warehouse after the {state['window_seconds']}s window closes.")


# ------------------------------------------------------------------ #
# mcp                                                                  #
# ------------------------------------------------------------------ #

@mcp_app.command("serve")
def mcp_serve():
    """Start the ddt MCP server (stdio transport for Claude Desktop)."""
    from .mcp_server import serve
    serve()


@mcp_app.command("setup-desktop")
def mcp_setup_desktop():
    """Register ddt as an MCP server in Claude Desktop's config."""
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
    cfg["mcpServers"]["ddt"] = {
        "command": uv_path,
        "args": ["--directory", project_dir, "run", "ddt", "mcp", "serve"],
    }
    claude_config.write_text(json.dumps(cfg, indent=2))
    typer.echo(f"Registered ddt MCP server in {claude_config}")
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


def _override_date_range(pipeline, start: str | None, end: str | None) -> None:
    from .config.models import DateRangeIterate
    for spec in pipeline.source.iterate:
        if isinstance(spec, DateRangeIterate):
            if start:
                spec.start = start
            if end:
                spec.end = end
