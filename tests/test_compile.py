"""Unit tests for the GCP compile step (dcf/deploy/gcp/compile.py)."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dcf.deploy.gcp.compile import (
    CompilePlan,
    _render_dockerfile,
    compile_project,
    generate_collector_tf_json,
)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _make_collector_yaml(project: Path, name: str, schedule: str = "0 8 * * *") -> None:
    (project / "collectors").mkdir(exist_ok=True)
    (project / "collectors" / f"{name}.yml").write_text(
        f"name: {name}\n"
        f"source:\n  type: http\n  url: https://example.com\n"
        f"  schema:\n    columns:\n      - name: id\n        path: id\n        type: integer\n"
        f"cadence:\n  strategy: incremental\n  primary_key: id\n"
        f"deployment:\n  schedule: \"{schedule}\"\n"
    )


def _gcp_config(project_id: str = "my-project", region: str = "us-central1", deployments: dict | None = None) -> dict:
    return {
        "project_id": project_id,
        "region": region,
        "warehouse_bucket": f"dcf-warehouse-{project_id}",
        "dags_bucket": f"dcf-dags-{project_id}",
        "deployments": deployments or {},
    }


# ------------------------------------------------------------------ #
# _render_dockerfile                                                   #
# ------------------------------------------------------------------ #

def test_render_dockerfile_java_disabled():
    result = _render_dockerfile(java_enabled=False)
    assert "openjdk" not in result
    assert "FROM python:3.12-slim" in result
    assert "%{" not in result


def test_render_dockerfile_java_enabled():
    result = _render_dockerfile(java_enabled=True)
    assert "openjdk" in result
    assert "FROM python:3.12-slim" in result
    assert "%{" not in result


# ------------------------------------------------------------------ #
# generate_collector_tf_json                                           #
# ------------------------------------------------------------------ #

def _base_tf_json(name: str = "events") -> dict:
    return generate_collector_tf_json(
        name=name,
        schedule="0 8 * * *",
        paused=False,
        project_id="my-project",
        region="us-central1",
        build_context_str="/tmp/build/events",
        content_hash="abc123",
        java_enabled=False,
        cloud_sql_instances=[],
        image_uri="us-central1-docker.pkg.dev/my-project/dcf-runner/events:latest",
    )


def test_generate_tf_json_structure():
    tf = _base_tf_json()
    resources = tf["resource"]
    assert "local_file" in resources
    assert "null_resource" in resources
    assert "google_cloud_run_v2_job" in resources
    assert "google_storage_bucket_object" in resources


def test_generate_tf_json_no_sql():
    tf = _base_tf_json()
    job = tf["resource"]["google_cloud_run_v2_job"]["collector_events"]
    inner = job["template"]["template"]
    assert "volumes" not in inner
    containers = inner["containers"]
    assert len(containers) == 1
    assert "volume_mounts" not in containers[0]


def test_generate_tf_json_with_sql():
    tf = generate_collector_tf_json(
        name="orders",
        schedule="0 9 * * *",
        paused=False,
        project_id="my-project",
        region="us-central1",
        build_context_str="/tmp/build/orders",
        content_hash="def456",
        java_enabled=False,
        cloud_sql_instances=["my-project:us-central1:my-db"],
        image_uri="us-central1-docker.pkg.dev/my-project/dcf-runner/orders:latest",
    )
    inner = tf["resource"]["google_cloud_run_v2_job"]["collector_orders"]["template"]["template"]
    assert "volumes" in inner
    assert inner["volumes"][0]["name"] == "cloudsql"
    assert "volume_mounts" in inner["containers"][0]


def test_generate_tf_json_job_name():
    tf = generate_collector_tf_json(
        name="my_collector",
        schedule="0 8 * * *",
        paused=False,
        project_id="p",
        region="us-central1",
        build_context_str="/tmp",
        content_hash="x",
        java_enabled=False,
        cloud_sql_instances=[],
        image_uri="uri",
    )
    job = tf["resource"]["google_cloud_run_v2_job"]["collector_my_collector"]
    assert job["name"] == "dcf-job-my-collector"


def test_generate_tf_json_dag_config_content():
    tf = _base_tf_json("events")
    dag_obj = tf["resource"]["google_storage_bucket_object"]["dag_config_events"]
    assert dag_obj["name"] == "collectors/events.yml"
    assert "${google_storage_bucket.dags.name}" in dag_obj["bucket"]
    import yaml
    cfg = yaml.safe_load(dag_obj["content"])
    assert cfg["name"] == "events"
    assert cfg["schedule"] == "0 8 * * *"
    assert cfg["cloud_run_job"] == "dcf-job-events"


def test_generate_tf_json_depends_on():
    tf = _base_tf_json("events")
    build = tf["resource"]["null_resource"]["collector_build_events"]
    assert "local_file.collector_dockerfile_events" in build["depends_on"]
    assert "google_artifact_registry_repository.dcf_runner" in build["depends_on"]

    job = tf["resource"]["google_cloud_run_v2_job"]["collector_events"]
    assert "null_resource.collector_build_events" in job["depends_on"]


def test_generate_tf_json_content_hash_trigger():
    tf = _base_tf_json("events")
    triggers = tf["resource"]["null_resource"]["collector_build_events"]["triggers"]
    assert triggers["content_hash"] == "abc123"
    assert triggers["java_enabled"] == "false"


def test_generate_tf_json_is_valid_json():
    tf = _base_tf_json()
    # Must round-trip through JSON without error
    serialized = json.dumps(tf, indent=2)
    restored = json.loads(serialized)
    assert restored["resource"]["google_cloud_run_v2_job"]["collector_events"]["name"] == "dcf-job-events"


# ------------------------------------------------------------------ #
# compile_project — classification logic                               #
# ------------------------------------------------------------------ #

def _compile(tmp_path, gcp, content_hash="newhash", filter_names=None):
    """Run compile_project with all heavy operations mocked."""
    tf_dir = tmp_path / ".dcf" / "terraform" / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)
    build_ctx = tmp_path / ".build"
    build_ctx.mkdir(parents=True, exist_ok=True)

    with patch("dcf.deploy.gcp.compile._sync_build_context", return_value=build_ctx), \
         patch("dcf.deploy.gcp.compile._content_hash", return_value=content_hash), \
         patch("dcf.deploy.gcp.compile._image_uri", return_value="uri"), \
         patch("dcf.deploy.gcp.compile._tf_state_dir", return_value=tmp_path / ".dcf" / "terraform"):
        return compile_project(tmp_path, gcp, filter_names=filter_names), tf_dir


def test_classify_new(tmp_path):
    _make_collector_yaml(tmp_path, "events")
    gcp = _gcp_config()

    plan, tf_dir = _compile(tmp_path, gcp, content_hash="newhash")

    assert "events" in plan.new
    assert plan.changed == []
    assert plan.unchanged == []
    assert (tf_dir / "collector_events.tf.json").exists()
    assert "events" in plan.hashes


def test_classify_unchanged(tmp_path):
    _make_collector_yaml(tmp_path, "events")
    stored_hash = "stable_hash"
    gcp = _gcp_config(deployments={"events": {"content_hash": stored_hash}})

    # Pre-create the .tf.json so compile sees it as already present
    tf_dir = tmp_path / ".dcf" / "terraform" / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)
    (tf_dir / "collector_events.tf.json").write_text("{}")
    mtime_before = (tf_dir / "collector_events.tf.json").stat().st_mtime

    plan, _ = _compile(tmp_path, gcp, content_hash=stored_hash)

    assert "events" in plan.unchanged
    assert plan.new == []
    assert plan.changed == []
    assert (tf_dir / "collector_events.tf.json").stat().st_mtime == mtime_before


def test_classify_changed(tmp_path):
    _make_collector_yaml(tmp_path, "events")
    gcp = _gcp_config(deployments={"events": {"content_hash": "old_hash"}})

    tf_dir = tmp_path / ".dcf" / "terraform" / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)
    (tf_dir / "collector_events.tf.json").write_text("{}")

    plan, _ = _compile(tmp_path, gcp, content_hash="new_hash")

    assert "events" in plan.changed
    assert plan.new == []
    assert plan.unchanged == []
    tf_content = json.loads((tf_dir / "collector_events.tf.json").read_text())
    assert "resource" in tf_content


def test_classify_deleted(tmp_path):
    _make_collector_yaml(tmp_path, "events")
    gcp = _gcp_config()

    tf_dir = tmp_path / ".dcf" / "terraform" / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)
    stale_path = tf_dir / "collector_stale.tf.json"
    stale_path.write_text("{}")

    plan, _ = _compile(tmp_path, gcp, filter_names=None)

    assert "stale" in plan.deleted
    assert not stale_path.exists()


def test_classify_deleted_skipped_when_filter_names(tmp_path):
    _make_collector_yaml(tmp_path, "events")
    gcp = _gcp_config()

    tf_dir = tmp_path / ".dcf" / "terraform" / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)
    stale_path = tf_dir / "collector_stale.tf.json"
    stale_path.write_text("{}")

    plan, _ = _compile(tmp_path, gcp, filter_names=["events"])

    assert plan.deleted == []
    assert stale_path.exists()


def test_all_collector_names_property(tmp_path):
    plan = CompilePlan(new=["a"], changed=["b"], unchanged=["c"], deleted=["d"])
    assert set(plan.all_collector_names) == {"a", "b", "c"}
    assert "d" not in plan.all_collector_names
