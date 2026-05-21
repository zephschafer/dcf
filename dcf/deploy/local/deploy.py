"""Local Docker-based deployment for batch and streaming collectors.

No GCP account required. Batch collectors are built and scheduled via local
Terraform modules (batch_collector_local + airflow_local). Streaming collectors
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

_DEPLOY_DIR = Path(__file__).parent.parent  # dcf/deploy/
_DCF_PKG_DIR = _DEPLOY_DIR.parent           # dcf/
_DCF_REPO_ROOT = _DCF_PKG_DIR.parent

_BATCH_COLLECTOR_MODULE = _DEPLOY_DIR / "infra" / "modules" / "batch_collector"


def _write_pyproject_toml(dest: Path) -> None:
    """Write dcf's pyproject.toml to dest/pyproject.toml.

    Works whether dcf is running from a development checkout or an installed
    package (where the repo root is not on disk and pyproject.toml lives only
    in package metadata).
    """
    repo_pyproject = _DCF_REPO_ROOT / "pyproject.toml"
    if repo_pyproject.exists():
        shutil.copy2(repo_pyproject, dest / "pyproject.toml")
        return

    import importlib.metadata

    meta = importlib.metadata.metadata("dcf-core")
    version = meta["Version"]
    reqs = importlib.metadata.requires("dcf-core") or []
    direct_deps = [r for r in reqs if "extra ==" not in r]
    deps_str = "\n".join(f'    "{r}",' for r in direct_deps)
    (dest / "pyproject.toml").write_text(
        f'[project]\n'
        f'name = "dcf"\n'
        f'version = "{version}"\n'
        f'requires-python = ">=3.12"\n'
        f'dependencies = [\n{deps_str}\n]\n\n'
        f'[project.scripts]\n'
        f'dcf = "dcf.cli:app"\n\n'
        f'[tool.setuptools.packages.find]\n'
        f'include = ["dcf*"]\n'
    )

_BUILD_DIR = Path.home() / ".dcf" / "build"
_TF_PLUGIN_CACHE = Path.home() / ".dcf" / ".plugin-cache"
_AIRFLOW_DAGS_DIR = Path.home() / ".dcf" / "airflow" / "dags"
_AIRFLOW_COMPOSE_FILE = Path.home() / ".dcf" / "airflow" / "docker-compose.yml"


def _tf_state_dir(project_root: Path) -> Path:
    """Return the Terraform state directory for this project.

    Defaults to <project_root>/.dcf/terraform; can be overridden with
    `terraform_state_dir` in .dcf/state.yml.
    """
    try:
        from ...state import load_state
        custom = load_state().get("terraform_state_dir")
        if custom:
            return Path(custom).expanduser()
    except RuntimeError:
        pass
    return project_root / ".dcf" / "terraform"


def _collect_env_vars(project_root: Path, collector_name: str) -> dict[str, str]:
    """Scan collector YAML for {{ env.VAR }} references and return {VAR: value}."""
    collector_path = project_root / "collectors" / f"{collector_name}.yml"
    if not collector_path.exists():
        return {}

    raw_yaml = collector_path.read_text()
    var_names = re.findall(r"\{\{\s*env\.(\w+)\s*\}\}", raw_yaml)
    if not var_names:
        return {}

    from ...state import load_env
    env_file = load_env()

    result: dict[str, str] = {}
    for var in dict.fromkeys(var_names):
        value = os.environ.get(var) or env_file.get(var) or env_file.get(var.upper())
        if not value:
            raise EnvironmentError(
                f"Collector references '{{{{ env.{var} }}}}' but '{var}' is not set "
                f"in the environment or .env"
            )
        result[var] = value
    return result


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #

def deploy(
    collector_name: str,
    deployment,
    project_root: Path,
    subscription: str | None = None,
) -> dict:
    """Build and start local Docker containers for a collector."""
    _check_docker()
    if deployment.type == "streaming":
        if subscription is None:
            raise ValueError("subscription is required for streaming local deploy")
        return _deploy_streaming(collector_name, subscription, deployment.window_seconds, project_root)
    else:
        return _deploy_batch(collector_name, deployment, project_root)


def undeploy(collector_name: str, deployment_state: dict, project_root: Path) -> None:
    """Stop and remove all local Docker resources for this collector."""
    if deployment_state.get("type") == "streaming":
        _undeploy_streaming(collector_name, deployment_state)
    else:
        _undeploy_batch(collector_name, deployment_state, project_root)


def undeploy_all(deployments: dict, project_root: Path) -> None:
    """Destroy all collector resources then tear down the shared Airflow stack."""
    for name, state in deployments.items():
        print(f"  Undeploying '{name}'...", flush=True)
        undeploy(name, state, project_root)
    _tf_destroy_airflow_local(project_root)


def publish(collector_name: str, deployment_state: dict, message_json: str, count: int = 1) -> None:
    """Publish a JSON message to the collector's local Kafka topic."""
    from kafka import KafkaProducer

    bootstrap = deployment_state.get("kafka_external_bootstrap", "localhost:29092")
    topic = deployment_state.get("kafka_topic", f"dcf-{collector_name}")

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

def _deploy_batch(collector_name: str, deployment, project_root: Path) -> dict:
    image_tag = f"dcf-local/{collector_name}:latest"
    warehouse_path = project_root / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    print(f"  Syncing build context for '{collector_name}'...", flush=True)
    build_context = _sync_build_context(project_root, collector_name)

    content_hash = _content_hash(build_context)

    print(f"  Applying Terraform (collector image)...", flush=True)
    _tf_apply_local_collector(collector_name, build_context, image_tag, content_hash, project_root)

    print(f"  Writing DAG file...", flush=True)
    _AIRFLOW_DAGS_DIR.mkdir(parents=True, exist_ok=True)
    dag_content = _local_dag_content(
        collector_name=collector_name,
        schedule=deployment.schedule,
        paused=getattr(deployment, "paused", False),
        image_tag=image_tag,
        warehouse_path=str(warehouse_path),
        env_vars=_collect_env_vars(project_root, collector_name),
    )
    _write_local_dag(collector_name, dag_content)

    print(f"  Applying Terraform (Airflow stack)...", flush=True)
    credentials = _generate_airflow_credentials(project_root)
    airflow_outputs = _tf_apply_airflow_local(
        dag_dir=str(_AIRFLOW_DAGS_DIR),
        warehouse_path=str(warehouse_path),
        credentials=credentials,
        project_root=project_root,
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


def _undeploy_batch(collector_name: str, state: dict, project_root: Path) -> None:
    print(f"  Destroying collector Terraform resources...", flush=True)
    _tf_destroy_local_collector(collector_name, project_root)

    dag_file = _AIRFLOW_DAGS_DIR / f"{collector_name}.py"
    if dag_file.exists():
        dag_file.unlink()
        print(f"  Removed DAG file: {dag_file}", flush=True)


# ------------------------------------------------------------------ #
# Build context helpers                                                #
# ------------------------------------------------------------------ #

def _sync_build_context(project_root: Path, collector_name: str) -> Path:
    """Create a stable build context dir at ~/.dcf/build/local/<name>/."""
    build_context = _BUILD_DIR / "local" / collector_name
    shutil.rmtree(build_context, ignore_errors=True)
    build_context.mkdir(parents=True)

    shutil.copytree(_DCF_PKG_DIR, build_context / "dcf")
    _write_pyproject_toml(build_context)

    for subdir in ("collectors", "connectors"):
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
# Terraform helpers — collector                                        #
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
    templates_src = _DEPLOY_DIR / "infra" /"templates"
    templates_dst = work_dir / "templates"
    if templates_dst.exists():
        shutil.rmtree(templates_dst)
    shutil.copytree(templates_src, templates_dst)


def _tf_apply_local_collector(
    collector_name: str,
    build_context: Path,
    image_tag: str,
    content_hash: str,
    project_root: Path,
) -> None:
    work_dir = _tf_state_dir(project_root) / "collectors" / collector_name / "local"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_COLLECTOR_MODULE / "local", work_dir)

    tfvars = {
        "collector_name": collector_name,
        "build_context": str(build_context),
        "image_tag": image_tag,
        "content_hash": content_hash,
        "java_enabled": True,
    }
    (work_dir / "terraform.tfvars.json").write_text(json.dumps(tfvars, indent=2))

    env = _tf_env()
    _tf_run(["terraform", "init", "-reconfigure"], work_dir, env)
    _tf_run(["terraform", "apply", "-auto-approve"], work_dir, env)


def _tf_destroy_local_collector(collector_name: str, project_root: Path) -> None:
    work_dir = _tf_state_dir(project_root) / "collectors" / collector_name / "local"
    if not work_dir.exists():
        logger.warning("No Terraform state found at %s — skipping destroy", work_dir)
        return

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


def _tf_destroy_airflow_local(project_root: Path) -> None:
    work_dir = _tf_state_dir(project_root) / "airflow" / "local"
    if not work_dir.exists():
        logger.warning("No Airflow Terraform state found at %s — skipping destroy", work_dir)
        return

    print("  Destroying Airflow stack...", flush=True)
    if _AIRFLOW_COMPOSE_FILE.exists():
        subprocess.run(
            ["docker", "compose", "-f", str(_AIRFLOW_COMPOSE_FILE), "down", "--volumes"],
            check=False,
        )

    env = _tf_env()
    _tf_run(["terraform", "destroy", "-auto-approve"], work_dir, env)
    shutil.rmtree(work_dir)


# ------------------------------------------------------------------ #
# DAG content                                                          #
# ------------------------------------------------------------------ #

def _local_dag_content(
    collector_name: str,
    schedule: str,
    paused: bool,
    image_tag: str,
    warehouse_path: str,
    env_vars: dict[str, str] | None = None,
) -> str:
    paused_str = "True" if paused else "False"
    environment = {"COLLECTOR_NAME": collector_name, **(env_vars or {})}
    return f"""\
# Generated by dcf — do not edit manually
from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="{collector_name}",
    schedule_interval="{schedule}",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation={paused_str},
    tags=["dcf"],
) as dag:
    run_collector = DockerOperator(
        task_id="run_{collector_name}",
        image="{image_tag}",
        environment={environment!r},
        mounts=[Mount(target="/app/warehouse", source="{warehouse_path}", type="bind")],
        docker_url="unix:///var/run/docker.sock",
        auto_remove="success",
    )
"""


def _write_local_dag(collector_name: str, dag_content: str) -> None:
    _AIRFLOW_DAGS_DIR.mkdir(parents=True, exist_ok=True)
    (_AIRFLOW_DAGS_DIR / f"{collector_name}.py").write_text(dag_content)


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
    template = _DEPLOY_DIR / "infra" /"templates" / "airflow.Dockerfile.tftpl"
    return hashlib.sha256(template.read_bytes()).hexdigest()


def _generate_airflow_credentials(project_root: Path) -> dict:
    """Read/generate Airflow credentials from .env."""
    from ...state import load_env, save_env
    env = load_env()

    admin_password = env.get("AIRFLOW_ADMIN_PASSWORD")
    if not admin_password:
        import getpass
        admin_password = getpass.getpass("Enter Airflow admin password: ").strip()
        if not admin_password:
            raise RuntimeError("Airflow admin password cannot be empty.")
        save_env("AIRFLOW_ADMIN_PASSWORD", admin_password)
        logger.info("Saved AIRFLOW_ADMIN_PASSWORD to .env")

    fernet_key = env.get("AIRFLOW_FERNET_KEY")
    if not fernet_key:
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key().decode()
        save_env("AIRFLOW_FERNET_KEY", fernet_key)

    db_password = env.get("AIRFLOW_DB_PASSWORD")
    if not db_password:
        import secrets as _secrets
        db_password = _secrets.token_urlsafe(16)
        save_env("AIRFLOW_DB_PASSWORD", db_password)

    return {
        "db_password": db_password,
        "admin_password": admin_password,
        "fernet_key": fernet_key,
    }


def _tf_apply_airflow_local(dag_dir: str, warehouse_path: str, credentials: dict, project_root: Path) -> dict:
    work_dir = _tf_state_dir(project_root) / "airflow" / "local"
    work_dir.mkdir(parents=True, exist_ok=True)
    _TF_PLUGIN_CACHE.mkdir(parents=True, exist_ok=True)

    _copy_module_to_work_dir(_BATCH_COLLECTOR_MODULE / "local" / "airflow", work_dir)

    build_context = _airflow_build_context()
    content_hash = _airflow_content_hash()

    tfvars = {
        "image_tag": "dcf-airflow-local:latest",
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
    return f"dcf-kafka-{name}"


def _runner_container(name: str) -> str:
    return f"dcf-runner-{name}"


def _network_name(name: str) -> str:
    return f"dcf-{name}"


def _deploy_streaming(
    collector_name: str,
    subscription: str,
    window_seconds: int,
    project_root: Path,
) -> dict:
    network = _network_name(collector_name)
    kafka_cname = _kafka_container(collector_name)
    runner_cname = _runner_container(collector_name)
    image_tag = f"dcf-local/{collector_name}-stream:latest"
    kafka_topic = f"dcf-{collector_name}"
    warehouse_path = project_root / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    _stop_remove(runner_cname)
    _stop_remove(kafka_cname)
    _remove_network(network)

    print(f"  Creating Docker network '{network}'...", flush=True)
    subprocess.run(["docker", "network", "create", network], check=True, capture_output=True)

    print(f"  Starting Kafka broker (apache/kafka, KRaft)...", flush=True)
    _start_kafka(kafka_cname, network, collector_name)

    print(f"  Waiting for Kafka to be ready...", flush=True)
    _wait_for_kafka("localhost:29092", timeout=30)

    print(f"  Creating topic '{kafka_topic}'...", flush=True)
    _create_kafka_topic("localhost:29092", kafka_topic)

    print(f"  Building local runner image '{image_tag}'...", flush=True)
    print("  (First build downloads python:3.12-slim + kafka-python, ~1 minute)", flush=True)
    _build_stream_image(project_root, image_tag)

    print(f"  Starting stream runner...", flush=True)
    _start_runner(
        runner_cname, image_tag, network, collector_name,
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


def _start_kafka(container_name: str, network: str, collector_name: str) -> None:
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
        "Check: docker logs dcf-kafka-<collector>"
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

    with tempfile.TemporaryDirectory(prefix="dcf-local-stream-") as tmp:
        tmp_path = Path(tmp)
        shutil.copytree(_DCF_PKG_DIR, tmp_path / "dcf")
        _write_pyproject_toml(tmp_path)

        for subdir in ("collectors", "connectors"):
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
            COPY dcf/ ./dcf/
            RUN pip install --no-cache-dir -e . 'kafka-python>=2.0'
            COPY collectors/ ./collectors/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENTRYPOINT ["python", "-m", "dcf.local_stream_runner"]
        """))

        result = subprocess.run(["docker", "build", "-t", image_tag, "."], cwd=tmp)
        if result.returncode != 0:
            raise RuntimeError(f"docker build failed for '{image_tag}'")


def _start_runner(
    container_name: str,
    image_tag: str,
    network: str,
    collector_name: str,
    kafka_cname: str,
    kafka_topic: str,
    window_seconds: int,
    warehouse_path: Path,
    project_root: Path | None = None,
) -> None:
    env_dict = _collect_env_vars(project_root, collector_name) if project_root else {}
    env_args = [arg for var, val in env_dict.items() for arg in ("-e", f"{var}={val}")]
    subprocess.run(
        [
            "docker", "run", "-d",
            "--name", container_name,
            "--network", network,
            *env_args,
            "-v", f"{warehouse_path}:/warehouse",
            image_tag,
            "--collector_name", collector_name,
            "--bootstrap_servers", f"{kafka_cname}:9092",
            "--topic", kafka_topic,
            "--output_path", f"/warehouse/{collector_name}/{collector_name}/data/",
            "--window_seconds", str(window_seconds),
        ],
        check=True, capture_output=True, text=True,
    )


def _undeploy_streaming(collector_name: str, state: dict) -> None:
    runner = state.get("runner_container", _runner_container(collector_name))
    kafka = state.get("kafka_container", _kafka_container(collector_name))
    network = state.get("docker_network", _network_name(collector_name))
    image_tag = state.get("image_tag", f"dcf-local/{collector_name}-stream:latest")

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
    if result.returncode == 0:
        return

    import platform
    if platform.system() == "Darwin":
        print("  Docker is not running — starting Docker Desktop...", flush=True)
        subprocess.run(["open", "-a", "Docker"], check=False)
        deadline = time.time() + 60
        while time.time() < deadline:
            time.sleep(3)
            if subprocess.run(["docker", "info"], capture_output=True).returncode == 0:
                print("  Docker is ready.", flush=True)
                return
        raise RuntimeError("Docker Desktop did not start within 60s. Start it manually and retry.")

    raise RuntimeError("Docker is not running. Start Docker and retry.")


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
