"""Tests for dcf deploy / status CLI commands."""

import yaml
from pathlib import Path
from unittest.mock import patch
from typer.testing import CliRunner

from dcf.cli import app

runner = CliRunner()


def _make_project(tmp_path: Path, profiles: dict | None = None, state: dict | None = None) -> Path:
    if profiles is not None:
        (tmp_path / "profiles.yml").write_text(yaml.dump(profiles))
    if state is not None:
        dcf_dir = tmp_path / ".dcf"
        dcf_dir.mkdir(exist_ok=True)
        (dcf_dir / "state.yml").write_text(yaml.dump(state))
    (tmp_path / "collectors").mkdir(exist_ok=True)
    return tmp_path


def _gcp_profile(project_id: str = "my-project", region: str = "us-central1") -> dict:
    return {"default": {"type": "gcp", "project_id": project_id, "region": region}}


# ------------------------------------------------------------------ #
# dcf deploy — happy path                                              #
# ------------------------------------------------------------------ #

def test_deploy_gcp_creates_bucket_and_writes_state(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles=_gcp_profile())
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))

    with patch("dcf.deploy.gcp.deploy.ensure_warehouse_bucket", return_value="dcf-warehouse-my-project") as mock_bucket:
        result = runner.invoke(app, ["deploy"])

    assert result.exit_code == 0, result.output
    assert "gs://dcf-warehouse-my-project" in result.output
    mock_bucket.assert_called_once_with("my-project", "us-central1")

    state = yaml.safe_load((tmp_path / ".dcf" / "state.yml").read_text())
    assert state["catalog"] == "gcp"
    assert state["gcp"]["warehouse_bucket"] == "dcf-warehouse-my-project"
    assert state["gcp"]["project_id"] == "my-project"
    assert state["gcp"]["region"] == "us-central1"


def test_deploy_idempotent(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles=_gcp_profile())
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))

    # First deploy
    with patch("dcf.deploy.gcp.deploy.ensure_warehouse_bucket", return_value="dcf-warehouse-my-project"):
        result1 = runner.invoke(app, ["deploy"])
    assert result1.exit_code == 0, result1.output

    # Second deploy (bucket exists — Conflict is caught internally)
    with patch("dcf.deploy.gcp.deploy.ensure_warehouse_bucket", return_value="dcf-warehouse-my-project"):
        result2 = runner.invoke(app, ["deploy"])
    assert result2.exit_code == 0, result2.output


# ------------------------------------------------------------------ #
# dcf deploy — error cases                                             #
# ------------------------------------------------------------------ #

def test_deploy_missing_profiles_yml(tmp_path, monkeypatch):
    _make_project(tmp_path)  # no profiles.yml
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy"])
    assert result.exit_code == 1
    assert "profiles.yml" in result.output.lower()


def test_deploy_local_profile_exits(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles={"default": {"type": "local"}})
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy"])
    assert result.exit_code == 1
    assert "local" in result.output.lower()


def test_deploy_no_adc(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles=_gcp_profile())
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))

    from google.auth.exceptions import DefaultCredentialsError

    with patch("dcf.deploy.gcp.deploy.ensure_warehouse_bucket", side_effect=DefaultCredentialsError("no creds")):
        result = runner.invoke(app, ["deploy"])

    assert result.exit_code == 1
    assert "gcloud auth application-default login" in result.output


def test_deploy_project_name_backward_compat(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles={"default": {"type": "gcp", "project_name": "old-name", "region": "us-central1"}})
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy"])
    assert result.exit_code == 1
    assert "project_name" in result.output
    assert "project_id" in result.output


def test_deploy_unknown_profile(tmp_path, monkeypatch):
    _make_project(tmp_path, profiles=_gcp_profile())
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy", "--profile", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()


# ------------------------------------------------------------------ #
# dcf status                                                           #
# ------------------------------------------------------------------ #

def test_status_local_catalog(tmp_path, monkeypatch):
    _make_project(tmp_path)  # no state.yml → defaults to local
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "local" in result.output


def test_status_gcp_catalog(tmp_path, monkeypatch):
    _make_project(tmp_path, state={
        "catalog": "gcp",
        "active_profile": "default",
        "gcp": {
            "project_id": "my-project",
            "region": "us-central1",
            "warehouse_bucket": "dcf-warehouse-my-project",
            "setup_status": "complete",
        },
    })
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "gs://dcf-warehouse-my-project" in result.output
    assert "my-project" in result.output
    assert "complete" in result.output


# ------------------------------------------------------------------ #
# removed commands no longer exist                                     #
# ------------------------------------------------------------------ #

def test_compile_command_removed(tmp_path, monkeypatch):
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["compile"])
    assert result.exit_code != 0


def test_airflow_command_removed(tmp_path, monkeypatch):
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["airflow"])
    assert result.exit_code != 0


def test_undeploy_command_removed(tmp_path, monkeypatch):
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["undeploy"])
    assert result.exit_code != 0


def test_gcp_subcommand_removed(tmp_path, monkeypatch):
    monkeypatch.setenv("DCF_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["gcp", "setup"])
    assert result.exit_code != 0
