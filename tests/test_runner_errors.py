"""
Tests for runner error reporting improvements (F-023).

Previously, fetch exceptions produced only "fetch error: {e}" with no traceback
and no failure summary. The pipeline also reported "complete" even when every
iteration failed.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pvc.config.models import Pipeline, PythonSource, Schema, Column, Build


def _make_pipeline(name: str = "test") -> Pipeline:
    return Pipeline(
        name=name,
        source=PythonSource(type="python", module="m", function="f"),
        schema_=Schema(columns=[Column(name="id", path="id", type="string")]),
        build=Build(strategy="incremental", primary_key="id"),
    )


def _run_captured(pipeline, **kwargs) -> str:
    """Run pipeline and return all stdout as a string."""
    from pvc.engine.runner import run_pipeline

    buf = io.StringIO()
    with (
        patch("pvc.spark_session.get_spark", return_value=MagicMock()),
        patch("pvc.engine.runner.iceberg_writer"),
        patch("pvc.project.find_project_root", return_value=Path("/tmp/pvc-test")),
        redirect_stdout(buf),
    ):
        run_pipeline(pipeline, catalog="local", **kwargs)
    return buf.getvalue()


class TestFetchErrorReporting:
    def test_exception_type_in_error_line(self):
        """Error line must include the exception class name."""
        pipeline = _make_pipeline()
        with patch("pvc.engine.runner.fetch_records", side_effect=ValueError("bad value")):
            out = _run_captured(pipeline)
        assert "ValueError" in out

    def test_traceback_included(self):
        """Full traceback must appear in output so the failing line is identifiable."""
        pipeline = _make_pipeline()
        with patch("pvc.engine.runner.fetch_records", side_effect=KeyError("api_key")):
            out = _run_captured(pipeline)
        assert "Traceback" in out

    def test_all_failed_summary(self):
        """When every iteration fails, completion line must say FAILED, not complete."""
        pipeline = _make_pipeline()
        with patch("pvc.engine.runner.fetch_records", side_effect=RuntimeError("boom")):
            out = _run_captured(pipeline)
        assert "FAILED" in out
        assert "complete" not in out.split("FAILED")[0].split("[pvc]")[-1]

    def test_partial_failure_summary(self):
        """When some iterations fail and some succeed, report the failure count."""
        from pvc.config.models import CategoricalIterate
        pipeline = Pipeline(
            name="multi",
            source=PythonSource(
                type="python", module="m", function="f",
                iterate=[CategoricalIterate(type="categorical", param="x", values=["a", "b", "c"])],
                params=[],
            ),
            schema_=Schema(columns=[Column(name="id", path="id", type="string")]),
            build=Build(strategy="append"),
        )

        call_count = 0
        def flaky(_source, _params):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("second always fails")
            return [{"id": str(call_count)}]

        with patch("pvc.engine.runner.fetch_records", side_effect=flaky):
            out = _run_captured(pipeline)

        assert "1/3" in out          # one out of three failed
        assert "FAILED" not in out    # not a total failure
        assert "complete with errors" in out

    def test_clean_run_still_says_complete(self):
        """A run with no failures must still say 'complete' (no regression)."""
        pipeline = _make_pipeline()
        with patch("pvc.engine.runner.fetch_records", return_value=[{"id": "1"}]):
            out = _run_captured(pipeline)
        assert "complete" in out
        assert "FAILED" not in out
        assert "errors" not in out
