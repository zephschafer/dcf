"""Local Docker-based deployment for batch and streaming pipelines.

No GCP account required. Batch pipelines run as a one-shot Docker container;
streaming pipelines run a Kafka broker + a local stream runner container.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import yaml

_DDT_PKG_DIR = Path(__file__).parent
_DDT_REPO_ROOT = _DDT_PKG_DIR.parent


def _collect_env_vars(project_root: Path, pipeline_name: str) -> list[str]:
    """Scan pipeline YAML for {{ env.VAR }} references and return ['-e', 'VAR=value', ...].

    Resolution order for each VAR:
      1. Host OS environment variable
      2. project.yml key (lowercased, e.g. GH_PAT → gh_pat)
    Raises EnvironmentError if a referenced var cannot be resolved.
    """
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
    for var in dict.fromkeys(var_names):  # deduplicate, preserve order
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
    """Build and start local Docker containers for a pipeline.

    Returns the deployment state dict to write into project.yml.
    """
    _check_docker()
    if deployment.type == "streaming":
        if subscription is None:
            raise ValueError("subscription is required for streaming local deploy")
        return _deploy_streaming(pipeline_name, subscription, deployment.window_seconds, project_root)
    else:
        return _deploy_batch(pipeline_name, project_root)


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
# Batch                                                                #
# ------------------------------------------------------------------ #

def _deploy_batch(pipeline_name: str, project_root: Path) -> dict:
    image_tag = f"ddt-local/{pipeline_name}:latest"
    warehouse_path = project_root / "warehouse"
    warehouse_path.mkdir(exist_ok=True)

    print(f"  Building local image '{image_tag}'...", flush=True)
    print("  (First build downloads python:3.12-slim, ~1 minute)", flush=True)
    _build_batch_image(project_root, image_tag)

    env_args = _collect_env_vars(project_root, pipeline_name)
    if env_args:
        var_names = [env_args[i] for i in range(1, len(env_args), 2)]
        names_str = ", ".join(v.split("=")[0] for v in var_names)
        print(f"  Forwarding env vars: {names_str}", flush=True)

    print(f"  Running '{pipeline_name}' in container to verify...", flush=True)
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "--name", f"ddt-verify-{pipeline_name}",
            "-e", f"PIPELINE_NAME={pipeline_name}",
            *env_args,
            "-v", f"{warehouse_path}:/app/warehouse",
            image_tag,
        ],
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Verification run for '{pipeline_name}' failed (exit {result.returncode}).\n"
            "Check the output above for details."
        )

    return {
        "type": "batch",
        "image_tag": image_tag,
        "warehouse_path": str(warehouse_path),
        "deployed_at": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
    }


def _build_batch_image(project_root: Path, image_tag: str) -> None:
    with tempfile.TemporaryDirectory(prefix="ddt-local-batch-") as tmp:
        tmp_path = Path(tmp)
        shutil.copytree(_DDT_PKG_DIR, tmp_path / "ddt")
        shutil.copy2(_DDT_REPO_ROOT / "pyproject.toml", tmp_path / "pyproject.toml")

        for subdir in ("pipelines", "connectors"):
            src = project_root / subdir
            if src.exists():
                shutil.copytree(src, tmp_path / subdir)
            else:
                (tmp_path / subdir).mkdir()

        (tmp_path / "project.yml").write_text("catalog: local\n")

        (tmp_path / "Dockerfile").write_text(dedent("""\
            FROM python:3.12-slim
            RUN apt-get update && apt-get install -y --no-install-recommends \
                openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*
            WORKDIR /app
            COPY pyproject.toml .
            COPY ddt/ ./ddt/
            RUN pip install --no-cache-dir -e .
            COPY pipelines/ ./pipelines/
            COPY connectors/ ./connectors/
            COPY project.yml .
            ENV PIPELINE_NAME=""
            CMD ["sh", "-c", "ddt run $PIPELINE_NAME"]
        """))

        result = subprocess.run(
            ["docker", "build", "-t", image_tag, "."],
            cwd=tmp,
        )
        if result.returncode != 0:
            raise RuntimeError(f"docker build failed for '{image_tag}'")


def _undeploy_batch(pipeline_name: str, state: dict) -> None:
    image_tag = state.get("image_tag", f"ddt-local/{pipeline_name}:latest")
    print(f"  Removing local image '{image_tag}'...", flush=True)
    subprocess.run(["docker", "rmi", "-f", image_tag], capture_output=True)


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

    # Idempotency: tear down any existing containers and network
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

    # Brief pause then confirm the runner is still up
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
    with tempfile.TemporaryDirectory(prefix="ddt-local-stream-") as tmp:
        tmp_path = Path(tmp)
        shutil.copytree(_DDT_PKG_DIR, tmp_path / "ddt")
        shutil.copy2(_DDT_REPO_ROOT / "pyproject.toml", tmp_path / "pyproject.toml")

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

        result = subprocess.run(
            ["docker", "build", "-t", image_tag, "."],
            cwd=tmp,
        )
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
        raise RuntimeError(
            "Docker is not running. Start Docker Desktop and retry."
        )


def _stop_remove(container_name: str) -> None:
    exists = subprocess.run(
        ["docker", "inspect", container_name],
        capture_output=True,
    ).returncode == 0
    if exists:
        subprocess.run(["docker", "stop", container_name], capture_output=True)
        subprocess.run(["docker", "rm", container_name], capture_output=True)


def _remove_network(network: str) -> None:
    exists = subprocess.run(
        ["docker", "network", "inspect", network],
        capture_output=True,
    ).returncode == 0
    if exists:
        subprocess.run(["docker", "network", "rm", network], capture_output=True)
