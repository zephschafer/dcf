"""Tests for dcf.writer.iceberg — PyIceberg write strategies."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import dcf.writer.iceberg as writer


def _make_catalog(tmp_path: Path):
    """Create a real local SqlCatalog in tmp_path."""
    from pyiceberg.catalog.sql import SqlCatalog
    dcf_dir = tmp_path / ".dcf"
    dcf_dir.mkdir(exist_ok=True)
    return SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{dcf_dir / 'catalog.db'}",
            "warehouse": str(tmp_path / "warehouse"),
        },
    )


# ------------------------------------------------------------------ #
# _upsert_iceberg                                                       #
# ------------------------------------------------------------------ #

class TestUpsertIceberg:
    def test_creates_iceberg_metadata(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._upsert_iceberg(cat, pd.DataFrame([{"id": 1, "v": "a"}]), ("ns", "t"), pk="id")
        meta_dir = tmp_path / "warehouse" / "ns" / "t" / "metadata"
        assert meta_dir.exists()
        assert list(meta_dir.glob("*.metadata.json"))

    def test_deduplicates_by_primary_key(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._upsert_iceberg(cat, pd.DataFrame([{"id": 1, "v": "old"}]), ("ns", "t"), pk="id")
        writer._upsert_iceberg(cat, pd.DataFrame([{"id": 1, "v": "new"}]), ("ns", "t"), pk="id")
        result = cat.load_table(("ns", "t")).scan().to_arrow().to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["v"] == "new"

    def test_new_pk_accumulates(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._upsert_iceberg(cat, pd.DataFrame([{"id": 1}]), ("ns", "t"), pk="id")
        writer._upsert_iceberg(cat, pd.DataFrame([{"id": 2}]), ("ns", "t"), pk="id")
        assert cat.load_table(("ns", "t")).scan().to_arrow().num_rows == 2

    def test_no_pk_accumulates_all_rows(self, tmp_path):
        cat = _make_catalog(tmp_path)
        df = pd.DataFrame([{"x": 1}])
        writer._upsert_iceberg(cat, df, ("ns", "t"), pk=None)
        writer._upsert_iceberg(cat, df, ("ns", "t"), pk=None)
        assert cat.load_table(("ns", "t")).scan().to_arrow().num_rows == 2

    def test_creates_namespace_automatically(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._upsert_iceberg(cat, pd.DataFrame([{"a": 1}]), ("new_ns", "t"), pk="a")
        assert cat.namespace_exists(("new_ns",))


# ------------------------------------------------------------------ #
# _append_iceberg                                                       #
# ------------------------------------------------------------------ #

class TestAppendIceberg:
    def test_creates_table_on_first_write(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._append_iceberg(cat, pd.DataFrame([{"x": 1}]), ("ns", "t"))
        assert cat.load_table(("ns", "t")) is not None

    def test_accumulates_rows(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._append_iceberg(cat, pd.DataFrame([{"x": 1}]), ("ns", "t"))
        writer._append_iceberg(cat, pd.DataFrame([{"x": 2}]), ("ns", "t"))
        assert cat.load_table(("ns", "t")).scan().to_arrow().num_rows == 2


# ------------------------------------------------------------------ #
# _overwrite_iceberg                                                    #
# ------------------------------------------------------------------ #

class TestOverwriteIceberg:
    def test_creates_table_on_first_write(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._overwrite_iceberg(cat, pd.DataFrame([{"x": 1}]), ("ns", "t"))
        assert cat.load_table(("ns", "t")) is not None

    def test_replaces_existing_data(self, tmp_path):
        cat = _make_catalog(tmp_path)
        writer._overwrite_iceberg(cat, pd.DataFrame([{"v": 1}, {"v": 2}]), ("ns", "t"))
        writer._overwrite_iceberg(cat, pd.DataFrame([{"v": 99}]), ("ns", "t"))
        result = cat.load_table(("ns", "t")).scan().to_arrow().to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["v"] == 99


# ------------------------------------------------------------------ #
# write() routing                                                       #
# ------------------------------------------------------------------ #

class TestWriteRouting:
    def _make_collector(self, strategy: str, pk: str | None = None, namespace: str = "ns"):
        from dcf.config.models import Collector, Cadence, HttpSource, Schema, Column
        return Collector(
            name="t",
            namespace=namespace,
            source=HttpSource(
                type="http",
                url="https://example.com",
                schema_=Schema(columns=[Column(name="id", path="id", type="string")]),
            ),
            cadence=Cadence(strategy=strategy, primary_key=pk),
        )

    def test_skips_empty_df(self, tmp_path):
        collector = self._make_collector("incremental", pk="id")
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            writer.write(None, collector, pd.DataFrame(), catalog="local")
        # Nothing written — no catalog.db created
        assert not (tmp_path / ".dcf" / "catalog.db").exists()

    def test_incremental_creates_iceberg_table(self, tmp_path):
        collector = self._make_collector("incremental", pk="id")
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            writer.write(None, collector, pd.DataFrame([{"id": "1", "val": "a"}]), catalog="local")
        assert (tmp_path / ".dcf" / "catalog.db").exists()
        assert (tmp_path / "warehouse" / "ns" / "t" / "metadata").exists()

    def test_append_creates_iceberg_table(self, tmp_path):
        collector = self._make_collector("append")
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            writer.write(None, collector, pd.DataFrame([{"x": 1}]), catalog="local")
        assert (tmp_path / "warehouse" / "ns" / "t" / "metadata").exists()

    def test_full_refresh_creates_iceberg_table(self, tmp_path):
        collector = self._make_collector("full_refresh")
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            writer.write(None, collector, pd.DataFrame([{"x": 1}]), catalog="local")
        assert (tmp_path / "warehouse" / "ns" / "t" / "metadata").exists()

    def test_adds_dcf_updated_at(self, tmp_path):
        collector = self._make_collector("append")
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            writer.write(None, collector, pd.DataFrame([{"x": 1}]), catalog="local")
        from pyiceberg.catalog.sql import SqlCatalog
        cat = SqlCatalog("dcf", **{
            "uri": f"sqlite:///{tmp_path / '.dcf' / 'catalog.db'}",
            "warehouse": str(tmp_path / "warehouse"),
        })
        cols = [f.name for f in cat.load_table(("ns", "t")).schema().fields]
        assert "dcf_updated_at" in cols


# ------------------------------------------------------------------ #
# get_catalog — GCS path                                               #
# ------------------------------------------------------------------ #

class TestGetCatalog:
    def test_gcs_catalog_uses_gs_warehouse(self, tmp_path):
        with (
            patch("dcf.catalog.find_project_root", return_value=tmp_path),
            patch("dcf.catalog.load_state", return_value={"gcp": {"warehouse_bucket": "my-bucket"}}),
        ):
            import dcf.catalog as catalog_module
            # Patch get_catalog's load_state import path
            with patch("dcf.catalog.load_state", return_value={"gcp": {"warehouse_bucket": "my-bucket"}}):
                from dcf.catalog import get_catalog
                cat = get_catalog("gcp")
        assert cat.properties.get("warehouse", "").startswith("gs://my-bucket")

    def test_local_catalog_uses_warehouse_dir(self, tmp_path):
        with patch("dcf.catalog.find_project_root", return_value=tmp_path):
            from dcf.catalog import get_catalog
            cat = get_catalog("local")
        assert cat.properties.get("warehouse") == str(tmp_path / "warehouse")
