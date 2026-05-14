"""
Test that MCP run_pipeline forwards the catalog from project.yml (F-022).

Previously, run_pipeline always passed catalog='local' regardless of
what project.yml contained, causing GCP-configured projects to silently
write to local warehouse instead of GCS.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock, call
import tempfile
import yaml


def _write_pipeline(pipelines_dir: Path, yaml_text: str, name: str) -> None:
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    (pipelines_dir / f"{name}.yml").write_text(yaml_text)


_SIMPLE_PIPELINE = """\
version: 1
name: test_pipe
source:
  type: python
  module: connectors.noop
  function: fetch
  params: []
  iterate: []
schema:
  columns:
    - name: id
      path: id
      type: string
build:
  strategy: incremental
  primary_key: id
"""


def test_mcp_run_pipeline_passes_gcp_catalog(tmp_path):
    """MCP run_pipeline must pass catalog='gcp' when project.yml says so."""
    pipelines_dir = tmp_path / "pipelines"
    _write_pipeline(pipelines_dir, _SIMPLE_PIPELINE, "test_pipe")

    mock_run = MagicMock()

    with (
        patch("ddt.mcp_server._pipelines_dir", return_value=pipelines_dir),
        patch("ddt.warehouse_reader._project_config", return_value={"catalog": "gcp"}),
        patch("ddt.mcp_server.redirect_stdout"),
        patch("ddt.engine.runner.run_pipeline", mock_run),
    ):
        import ddt.mcp_server as mcp
        mcp.run_pipeline("test_pipe", limit=1)

    assert mock_run.called
    _, kwargs = mock_run.call_args
    assert kwargs.get("catalog") == "gcp", (
        f"Expected catalog='gcp' but got {kwargs.get('catalog')!r}. "
        "MCP run_pipeline must read catalog from project.yml."
    )


def test_mcp_run_pipeline_defaults_to_local_catalog(tmp_path):
    """MCP run_pipeline uses 'local' when project.yml has no catalog key."""
    pipelines_dir = tmp_path / "pipelines"
    _write_pipeline(pipelines_dir, _SIMPLE_PIPELINE, "test_pipe")

    mock_run = MagicMock()

    with (
        patch("ddt.mcp_server._pipelines_dir", return_value=pipelines_dir),
        patch("ddt.warehouse_reader._project_config", return_value={}),
        patch("ddt.mcp_server.redirect_stdout"),
        patch("ddt.engine.runner.run_pipeline", mock_run),
    ):
        import ddt.mcp_server as mcp
        mcp.run_pipeline("test_pipe")

    assert mock_run.called
    _, kwargs = mock_run.call_args
    assert kwargs.get("catalog") == "local"
