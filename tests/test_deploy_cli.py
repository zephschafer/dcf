"""Tests for ddt deploy / undeploy CLI error paths (F-031)."""

import pytest
import yaml
from pathlib import Path
from typer.testing import CliRunner

from ddt.cli import app

runner = CliRunner()


def _make_project(tmp_path: Path, catalog: str = "local", gcp: dict | None = None) -> Path:
    config = {"catalog": catalog}
    if gcp:
        config["gcp"] = gcp
    (tmp_path / "project.yml").write_text(yaml.dump(config))
    (tmp_path / "pipelines").mkdir()
    return tmp_path


def _make_pipeline(project: Path, name: str, with_deploy: bool = True) -> None:
    deploy_block = 'deploy:\n  schedule: "0 8 * * *"\n' if with_deploy else ""
    (project / "pipelines" / f"{name}.yml").write_text(
        f"version: 1\n"
        f"name: {name}\n"
        f"source:\n  type: http\n  url: https://example.com\n"
        f"schema:\n  columns:\n    - name: id\n      path: id\n      type: integer\n"
        f"build:\n  strategy: incremental\n  primary_key: id\n"
        f"{deploy_block}"
    )


def test_deploy_missing_pipeline(tmp_path, monkeypatch):
    _make_project(tmp_path)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()


def test_deploy_no_deploy_block(tmp_path, monkeypatch):
    project = _make_project(tmp_path)
    _make_pipeline(project, "my_pipeline", with_deploy=False)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy", "my_pipeline"])
    assert result.exit_code == 1
    assert "no 'deploy:' block" in result.output


def test_deploy_requires_gcp_catalog(tmp_path, monkeypatch):
    project = _make_project(tmp_path, catalog="local")
    _make_pipeline(project, "my_pipeline", with_deploy=True)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy", "my_pipeline"])
    assert result.exit_code == 1
    assert "catalog is not 'gcp'" in result.output


def test_deploy_requires_gcp_setup_complete(tmp_path, monkeypatch):
    project = _make_project(tmp_path, catalog="gcp", gcp={"setup_status": "failed"})
    _make_pipeline(project, "my_pipeline", with_deploy=True)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy", "my_pipeline"])
    assert result.exit_code == 1
    assert "GCP setup is not complete" in result.output


def test_undeploy_not_deployed(tmp_path, monkeypatch):
    project = _make_project(tmp_path, catalog="gcp", gcp={"setup_status": "complete"})
    _make_pipeline(project, "my_pipeline", with_deploy=True)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["undeploy", "my_pipeline"])
    assert result.exit_code == 1
    assert "not in project.yml deployments" in result.output


def test_deploy_status_none(tmp_path, monkeypatch):
    _make_project(tmp_path)
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy-status"])
    assert result.exit_code == 0
    assert "No pipelines are currently deployed" in result.output


def test_deploy_status_shows_deployments(tmp_path, monkeypatch):
    project = _make_project(tmp_path)
    config = yaml.safe_load((project / "project.yml").read_text())
    config["deployments"] = {
        "my_pipeline": {
            "schedule": "0 8 * * *",
            "dag_id": "my_pipeline",
            "cloud_run_job": "ddt-job-my-pipeline",
            "composer_env": "ddt-composer",
            "deployed_at": "2026-05-11T08:00:00+00:00",
        }
    }
    (project / "project.yml").write_text(yaml.dump(config))
    monkeypatch.setenv("DDT_PROJECT_DIR", str(tmp_path))
    result = runner.invoke(app, ["deploy-status"])
    assert result.exit_code == 0
    assert "my_pipeline" in result.output
    assert "0 8 * * *" in result.output
    assert "ddt-job-my-pipeline" in result.output
