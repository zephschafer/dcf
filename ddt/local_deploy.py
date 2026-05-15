"""Local Docker-based deployment for batch and streaming pipelines.

No GCP account required. Batch pipelines are built and scheduled via local
Terraform modules (batch_pipeline_local + airflow_local). Streaming pipelines
run a Kafka broker + local stream runner container.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DDT_PKG_DIR = Path(__file__).parent
_DDT_REPO_ROOT = _DDT_PKG_DIR.parent

_BATCH_PIPELINE_MODULE = _DDT_PKG_DIR / "infra" / "modules" / "batch_pipeline"


def _write_pyproject_toml(dest: Path) -> None:
    """Write ddt's pyproject.toml to dest/pyproject.toml.

    Works whether ddt is running from a development checkout or an installed
    package (where the repo root is not on disk and pyproject.toml lives only
    in package metadata).
    """
    repo_pyproject = _DDT_REPO_ROOT / "pyproject.toml"
    if repo_pyproject.exists():
        shutil.copy2(repo_pyproject, dest / "pyproject.toml")
        return

    import importlib.metadata

    meta = importlib.metadata.metadata("ddt")
    version = meta["Version"]
    reqs = importlib.metadata.requires("ddt") or []
    direct_deps = [r for r in reqs if "extra ==" not in r]
    deps_str = "\n".join(f'    "{r}",' for r in direct_deps)
    (dest / "pyproject.toml").write_text(
        f'[project]\n'
        f'name = "ddt"\n'
        f'version = "{version}"\n'
        f'requires-python = ">=3.12"\n'
        f'dependencies = [\n{deps_str}\n]\n\n'
        f'[project.scripts]\n'
        f'ddt = "ddt.cli:app"\n\n'
        f'[tool.setuptools.packages.find]\n'
        f'include = ["ddt*"]\n'
    )

_BUILD_DIR = Path.home() / ".ddt" / "build"
_TF_DIR = Path.home() / ".ddt" / "terraform"
_TF_PLUGIN_CACHE = _TF_DIR / ".plugin-cache"
_AIRFLOW_DAGS_DIR = Path.home() / ".ddt" / "airflow" / "dags"
_AIRFLOW_COMPOSE_FILE = Path.home() / ".ddt" / "airflow" / "docker-compose.yml"


def _collect_env_vars(project_root: Path, pipeline_name: str) -> list[str]:
    """Scan pipeline YAML for {{ env.VAR }} references and return ['-e', 'VAR=value', ...]."""
    pipeline_path = project_root / "pipelines" / f"{pipeline_name}.yml"
    if not pipeline_path.exists():
        return []

    raw_yaml = pipeline_path.read_text()
    var_names = re.findall(r"\{\{\s*env\.(\w+)\s*\}\}", raw_yaml)
    if not var_names:
        return []

    project_cfg: dict = {}
    cfg_path = project_root / "project.yml"
    if cfg_path.exists():
        project_cfg = yaml.safe_load(cfg_path.read_text()) or {}

    args: list[str] = []
    for var in dict.fromkeys(var_names):
        value = os.environ.get(var) or project_cfg.get(var.lower())
        if not value:
            raise EnvironmentError(
                f"Pipeline references '{{{{ env.{var} }}}}' but '{var}' is not set "
                f"in the host environment and '{var.lower()}' is not in project.yml"
            )
        args += ["-e", f"{var}={value}"]
    return args


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #

def deploy(
    pipeline_name: str,
    deployment,
    project_root: Path,
    subscription: str | None = None,
) -> dict:
    """Build and start local Docker containers for a pipeline."""
    _check_docker()
    if deployment.type == "streaming":
        if subscription is None:
            raise ValueError("subscription is required for streaming local deploy")
        return _deploy_streaming(pipeline_name, subscription, deployment.window_seconds, project_root)
    else:
        return _deploy_batch(pipeline_name, deployment, project_root)


def undeploy(pipeline_name: str, deployment_state: dict) -> None:
    """Stop and remove all local Docker resources for this pipeline."""
    if deployment_state.get("type") == "streaming":
        _undeploy_streaming(pipeline_name, deployment_state)
    else:
        _undeploy_batch(pipeline_name, deployment_state)


def publish(pipeline_name: str, deployment_state: dict, message_json: str, count: int = 1) -> None:
    """Publish a JSON message to the pipeline's local Kafka topic."""
    from kafka import KafkaProducer

    bootstrap = deployment_state.get("kafka_external_bootstrap", "localhost:29092")
    topic = deployment_state.get("kafka_topic", f"ddt-{pipeline_name}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
    )
    for _ in range(count):
        producer.send(topic, value=message_json)
    producer.flush()
    producer.close()


# ------------------------------------------------------------------ #
# Batch — Terraform path                                               #
# ------------------------------------------------------------------ #

def _deploy_batch(pipeline_name: str, deployment, project_root: Path) -> dict:
    image_tag = f"ddt-local/{pipeline_name}:latest"
    warehouse_path = project_root / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    print(f"  Syncing build context for '{pipeline_name}'...", flush=True)
    build_context = _sync_build_context(project_root, pipeline_name)

    content_hash = _content_hash(build_context)

    print(f"  Applying Terraform (pipeline image)...", flush=True)
    _tf_apply_local_pipeline(pipeline_name, build_context, image_tag, content_hash)

    print(f"  Writing DAG file...", flush=True)
    _AIRFLOW_DAGS_DIR.mkdir(parents=True, exist_ok=True)
    dag_content = _local_dag_content(
        pipeline_name=pipeline_name,
        schedule=deployment.schedule,
        paused=getattr(deployment, "paused", False),
        image_tag=image_tag,
        warehouse_path=str(warehouse_path),
    )
    _write_local_dag(pipeline_name, dag_content)

    print(f"  Applying Terraform (Airflow stack)...", flush=True)
    credentials = _generate_airflow_credentials(project_root)
    airflow_outputs = _tf_apply_airflow_local(
        dag_dir=str(_AIRFLOW_DAGS_DIR),
        warehouse_path=str(warehouse_path),
        credentials=credentials,
    )

    airflow_url = airflow_outputs.get("webserver_url", {}).get("value", "http://localhost:8080")
    print(f"  Airflow UI: {airflow_url}", flush=True)

    return {
        "type": "batch",
        "image_tag": image_tag,
        "warehouse_path": str(warehouse_path),
        "airflow_url": airflow_url,
        "schedule": deployment.schedule,
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def _undeploy_batch(pipeline_name: str, state: dict) -> None:
    print(f"  Destroying pipeline Terraform resources...", flush=True)
    _tf_destroy_local_pipeline(pipeline_name)

    dag_file = _AIRFLOW_DAGS_DIR / f"{pipeline_name}.py"
    if dag_file.exists():
        dag_file.unlink()
        print(f"  Removed DAG file: {dag_file}", flush=True)


# ------------------------------------------------------------------ #
# Build context helpers                                                #
# ------------------------------------------------------------------ #

def _sync_build_context(project_root: Path, pipeline_name: str) -> Path:
    """Create a stable build context dir at ~/.ddt/build/local/<name>/."""
    build_context = _BUILD_DIR / "local" / pipeline_name
    shutil.rmtree(build_context, ignore_errors=True)
    build_context.mkdir(parents=True)

    shutil.copytree(_DDT_PKG_DIR, build_context / "ddt")
    _write_pyproject_toml(build_context)

    for subdir in ("pipelines", "connectors"):
        src = project_root / subdir
        dst = build_context / subdir
        if src.exists():
            shutil.copytree(src, dst)
        else:
            dst.mkdir()

    (build_context / "project.yml").write_text("catalog: local\n")

    return build_context


def _content_hash(build_context: Path) -> str:
    """SHA256 of all files in build_context, excluding Dockerfile (written by Terraform)."""
    h = hashlib.sha256()
    for path in sorted(build_context.rglob("*")):
        if path.is_file() and path.name != "Dockerfile":
            h.update(path.read_bytes())
    return h.hexdigest()


# ------------------------------------------------------------------ #
# Terraform helpers — pipeline                                         #
# ------------------------------------------------------------------ #

def _tf_env() -> dict:
    return {
        **os.environ,
        "TF_INPUT": "0",
        "TF_PLUGIN_CACHE_DIR": str(_TF_PLUGIN_CACHE),
    }


def _tf_run(cmd: list[str], work_dir: Path, env: dict) -> None:
    result = subprocess.run(cmd, cwd=str(work_dir), env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"terraform {cmd[1]} failed (exit {result.returncode}):\n{result.stderr[-2000:]}"
        )
    logger.info("terraform %s OK", cmd[1])


def _copy_module_to_work_dir(module_dir: Path, work_dir: Path) -> None:
    """Copy a leaf Terraform module's .tf files + shared templates into work_dir."""
    for item in module_dir.iterdir():
        if item.name in (".terraform", ".terraform.lock.hcl"):
            continue
        if item.is_file() and item.suffix == ".tf":
            shutil.copy2(item, work_dir / item.name)
    templates_src = _DDT_PKG_DIR / "infra" / "templates"
    templates_dst = work_dir / "templates"
    if templates_dst.exists():
        shutil.rmtree(templates_dst)
    shutil.copytree(templates_src, templates_dst)


def _tf_apply_local_pipeline(
    pipeline_name: str,
    build_context: Path,
    image_tag: str,
    content_hash: str,
) -> None:
    work_dir = _TF_DIR / "pipelines" / pipeline_name / "local"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_PIPELINE_MODULE / "local", work_dir)

    tfvars = {
        "pipeline_name": pipeline_name,
        "build_context": str(build_context),
        "image_tag": image_tag,
        "content_hash": content_hash,
        "java_enabled": True,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)


def _tf_destroy_local_pipeline(pipeline_name: str) -> None:
    work_dir = _TF_DIR / "pipelines" / pipeline_name / "local"
    if not work_dir.exists():
        logger.warning("No Terraform state found at %s — skipping destroy", work_dir)
        return

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


# ------------------------------------------------------------------ #
# DAG content                                                          #
# ------------------------------------------------------------------ #

def _local_dag_content(
    pipeline_name: str,
    schedule: str,
    paused: bool,
    image_tag: str,
    warehouse_path: str,
) -> str:
    paused_str = "True" if paused else "False"
    return f"""\
# Generated by ddt — do not edit manually
from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="{pipeline_name}",
    schedule_interval="{schedule}",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation={paused_str},
    tags=["ddt"],
) as dag:
    run_pipeline = DockerOperator(
        task_id="run_{pipeline_name}",
        image="{image_tag}",
        environment={{"PIPELINE_NAME": "{pipeline_name}"}},
        mounts=[Mount(target="/app/warehouse", source="{warehouse_path}", type="bind")],
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
    )
"""


def _write_local_dag(pipeline_name: str, dag_content: str) -> None:
    _AIRFLOW_DAGS_DIR.mkdir(parents=True, exist_ok=True)
    (_AIRFLOW_DAGS_DIR / f"{pipeline_name}.py").write_text(dag_content)


# ------------------------------------------------------------------ #
# Terraform helpers — Airflow                                          #
# ------------------------------------------------------------------ #

def _airflow_build_context() -> Path:
    """Return the stable build context dir for the local Airflow image."""
    build_context = _BUILD_DIR / "airflow-local"
    build_context.mkdir(parents=True, exist_ok=True)
    return build_context


def _airflow_content_hash() -> str:
    """Hash of the airflow Dockerfile template to detect when Airflow image needs rebuild."""
    template = _DDT_PKG_DIR / "infra" / "modules" / "templates" / "airflow.Dockerfile.tftpl"
    return hashlib.sha256(template.read_bytes()).hexdigest()


def _generate_airflow_credentials(project_root: Path) -> dict:
    """Read/generate Airflow credentials from project.yml."""
    cfg_path = project_root / "project.yml"
    cfg: dict = yaml.safe_load(cfg_path.read_text()) or {} if cfg_path.exists() else {}

    admin_password = cfg.get("airflow_admin_password")
    if not admin_password:
        raise RuntimeError(
            "airflow_admin_password is missing from project.yml.\n"
            "Add it before running ddt deploy:\n\n"
            "  airflow_admin_password: \"your-password-here\"\n"
        )

    fernet_key = cfg.get("airflow_fernet_key")
    if not fernet_key:
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key().decode()
        cfg["airflow_fernet_key"] = fernet_key
        cfg_path.write_text(yaml.dump(cfg, default_flow_style=False, sort_keys=False))
        logger.info("Generated and saved airflow_fernet_key to project.yml")

    return {
        "db_password": "airflow",
        "admin_password": admin_password,
        "fernet_key": fernet_key,
    }


def _tf_apply_airflow_local(dag_dir: str, warehouse_path: str, credentials: dict) -> dict:
    work_dir = _TF_DIR / "airflow" / "local"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_PIPELINE_MODULE / "local" / "airflow", work_dir)

    build_context = _airflow_build_context()
    content_hash = _airflow_content_hash()

    tfvars = {
        "image_tag": "ddt-airflow-local:latest",
        "build_context": str(build_context),
        "content_hash": content_hash,
        "dag_dir": dag_dir,
        "warehouse_path": warehouse_path,
        "docker_socket": "/var/run/docker.sock",
        "db_password": credentials["db_password"],
        "admin_password": credentials["admin_password"],
        "fernet_key": credentials["fernet_key"],
        "compose_file_path": str(_AIRFLOW_COMPOSE_FILE),
        "webserver_port": 8090,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    _AIRFLOW_COMPOSE_FILE.parent.mkdir(parents=True, exist_ok=True)

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)

    raw = subprocess.run(
        ["terraform", "output", "-json"],
        cwd=str(work_dir), env=env, capture_output=True, text=True,
    ).stdout
    return json.loads(raw) if raw.strip() else {}


# ------------------------------------------------------------------ #
# Streaming                                                            #
# ------------------------------------------------------------------ #

def _kafka_container(name: str) -> str:
    return f"ddt-kafka-{name}"


def _runner_container(name: str) -> str:
    return f"ddt-runner-{name}"


def _network_name(name: str) -> str:
    return f"ddt-{name}"


def _deploy_streaming(
    pipeline_name: str,
    subscription: str,
    window_seconds: int,
    project_root: Path,
) -> dict:
    network = _network_name(pipeline_name)
    kafka_cname = _kafka_container(pipeline_name)
    runner_cname = _runner_container(pipeline_name)
    image_tag = f"ddt-local/{pipeline_name}-stream:latest"
    kafka_topic = f"ddt-{pipeline_name}"
    warehouse_path = project_root / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    _stop_remove(runner_cname)
    _stop_remove(kafka_cname)
    _remove_network(network)

    print(f"  Creating Docker network '{network}'...", flush=True)
    subprocess.run(["docker", "network", "create", network], check=True, capture_output=True)

    print(f"  Starting Kafka broker (apache/kafka, KRaft)...", flush=True)
    _start_kafka(kafka_cname, network, pipeline_name)

    print(f"  Waiting for Kafka to be ready...", flush=True)
    _wait_for_kafka("localhost:29092", timeout=30)

    print(f"  Creating topic '{kafka_topic}'...", flush=True)
    _create_kafka_topic("localhost:29092", kafka_topic)

    print(f"  Building local runner image '{image_tag}'...", flush=True)
    print("  (First build downloads python:3.12-slim + kafka-python, ~1 minute)", flush=True)
    _build_stream_image(project_root, image_tag)

    print(f"  Starting stream runner...", flush=True)
    _start_runner(
        runner_cname, image_tag, network, pipeline_name,
        kafka_cname, kafka_topic, window_seconds, warehouse_path,
        project_root=project_root,
    )

    time.sleep(4)
    status = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Status}}", runner_cname],
        capture_output=True, text=True,
    ).stdout.strip()
    if status != "running":
        logs = subprocess.run(
            ["docker", "logs", "--tail", "40", runner_cname],
            capture_output=True, text=True,
        )
        raise RuntimeError(
            f"Stream runner container stopped unexpectedly (status: {status}).\n"
            f"Logs:\n{logs.stdout}{logs.stderr}"
        )

    return {
        "type": "streaming",
        "window_seconds": window_seconds,
        "docker_network": network,
        "kafka_container": kafka_cname,
        "runner_container": runner_cname,
        "kafka_topic": kafka_topic,
        "kafka_external_bootstrap": "localhost:29092",
        "image_tag": image_tag,
        "warehouse_path": str(warehouse_path),
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def _start_kafka(container_name: str, network: str, pipeline_name: str) -> None:
    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", network,
            "-p", "29092:29092",
            "-e", "KAFKA_NODE_ID=1",
            "-e", "KAFKA_PROCESS_ROLES=broker,controller",
            "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e", (
                "KAFKA_LISTENERS="
                "INTERNAL://0.0.0.0:9092,"
                "EXTERNAL://0.0.0.0:29092,"
                "CONTROLLER://0.0.0.0:9093"
            ),
            "-e", (
                f"KAFKA_ADVERTISED_LISTENERS="
                f"INTERNAL://{container_name}:9092,"
                f"EXTERNAL://localhost:29092"
            ),
            "-e", (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
                "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
            ),
            "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
            "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
            "apache/kafka:latest",
        ],
        check=True, capture_output=True, text=True,
    )


def _wait_for_kafka(bootstrap: str, timeout: int = 30) -> None:
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                request_timeout_ms=3000,
                connections_max_idle_ms=5000,
            )
            admin.close()
            return
        except (NoBrokersAvailable, Exception):
            time.sleep(2)
    raise RuntimeError(
        f"Kafka did not become available at {bootstrap} within {timeout}s.\n"
        "Check: docker logs ddt-kafka-<pipeline>"
    )


def _create_kafka_topic(bootstrap: str, topic_name: str) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
    try:
        admin.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def _build_stream_image(project_root: Path, image_tag: str) -> None:
    import tempfile
    from textwrap import dedent

    with tempfile.TemporaryDirectory(prefix="ddt-local-stream-") as tmp:
        tmp_path = Path(tmp)
        shutil.copytree(_DDT_PKG_DIR, tmp_path / "ddt")
        _write_pyproject_toml(tmp_path)

        for subdir in ("pipelines", "connectors"):
            src = project_root / subdir
            if src.exists():
                shutil.copytree(src, tmp_path / subdir)
            else:
                (tmp_path / subdir).mkdir()

        (tmp_path / "project.yml").write_text("catalog: local\n")

        (tmp_path / "Dockerfile").write_text(dedent("""\
            FROM python:3.12-slim
            WORKDIR /app
            COPY pyproject.toml .
            COPY ddt/ ./ddt/
            RUN pip install --no-cache-dir -e . 'kafka-python>=2.0'
            COPY pipelines/ ./pipelines/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENTRYPOINT ["python", "-m", "ddt.local_stream_runner"]
        """))

        result = subprocess.run(["docker", "build", "-t", image_tag, "."], cwd=tmp)
        if result.returncode != 0:
            raise RuntimeError(f"docker build failed for '{image_tag}'")


def _start_runner(
    container_name: str,
    image_tag: str,
    network: str,
    pipeline_name: str,
    kafka_cname: str,
    kafka_topic: str,
    window_seconds: int,
    warehouse_path: Path,
    project_root: Path | None = None,
) -> None:
    env_args = _collect_env_vars(project_root, pipeline_name) if project_root else []
    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", network,
            *env_args,
            "-v", f"{warehouse_path}:/warehouse",
            image_tag,
            "--pipeline_name", pipeline_name,
            "--bootstrap_servers", f"{kafka_cname}:9092",
            "--topic", kafka_topic,
            "--output_path", f"/warehouse/{pipeline_name}/{pipeline_name}/data/",
            "--window_seconds", str(window_seconds),
        ],
        check=True, capture_output=True, text=True,
    )


def _undeploy_streaming(pipeline_name: str, state: dict) -> None:
    runner = state.get("runner_container", _runner_container(pipeline_name))
    kafka = state.get("kafka_container", _kafka_container(pipeline_name))
    network = state.get("docker_network", _network_name(pipeline_name))
    image_tag = state.get("image_tag", f"ddt-local/{pipeline_name}-stream:latest")

    print(f"  Stopping stream runner '{runner}'...", flush=True)
    _stop_remove(runner)

    print(f"  Stopping Kafka broker '{kafka}'...", flush=True)
    _stop_remove(kafka)

    print(f"  Removing Docker network '{network}'...", flush=True)
    _remove_network(network)

    print(f"  Removing local image '{image_tag}'...", flush=True)
    subprocess.run(["docker", "rmi", "-f", image_tag], capture_output=True)

    warehouse = state.get("warehouse_path", "warehouse/")
    print(f"  Warehouse data at {warehouse} is untouched.", flush=True)


# ------------------------------------------------------------------ #
# Docker helpers                                                       #
# ------------------------------------------------------------------ #

def _check_docker() -> None:
    result = subprocess.run(["docker", "info"], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError("Docker is not running. Start Docker Desktop and retry.")


def _stop_remove(container_name: str) -> None:
    exists = subprocess.run(
        ["docker", "inspect", container_name], capture_output=True,
    ).returncode == 0
    if exists:
        subprocess.run(["docker", "stop", container_name], capture_output=True)
        subprocess.run(["docker", "rm", container_name], capture_output=True)


def _remove_network(network: str) -> None:
    exists = subprocess.run(
        ["docker", "network", "inspect", network], capture_output=True,
    ).returncode == 0
    if exists:
        subprocess.run(["docker", "network", "rm", network], capture_output=True)
