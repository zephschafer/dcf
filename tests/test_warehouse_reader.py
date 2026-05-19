"""
Tests for dcf.warehouse_reader covering F-018, F-019, F-020, F-021.

All tests use a temporary local warehouse with real Parquet files — no GCS,
no mocking of DuckDB. GCP-catalog behavior is tested by patching _catalog()
to return "gcp" and supplying a fake bucket resolver.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import dcf.warehouse_reader as wr


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _make_parquet(dir_path: Path, rows: list[dict]) -> None:
    """Write a single-file Parquet table to dir_path/data/part-001.parquet."""
    data_dir = dir_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({k: [r[k] for r in rows] for k in rows[0]})
    pq.write_table(table, data_dir / "part-001.parquet")


# ------------------------------------------------------------------ #
# F-018: list_tables shows local tables when catalog=gcp              #
# ------------------------------------------------------------------ #

class TestListTablesGcpShowsLocalTables:
    def test_local_tables_appear_with_location_local(self, tmp_path):
        """list_tables() must return local tables (location='local') when catalog=gcp."""
        _make_parquet(
            tmp_path / "myns" / "mytable",
            [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
        )

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[]),  # no GCS tables
        ):
            tables = wr.list_tables()

        assert len(tables) == 1
        t = tables[0]
        assert t["full_name"] == "myns.mytable"
        assert t["location"] == "local"
        assert t["row_count"] == 2
        assert any(c["name"] == "id" for c in t["columns"])

    def test_gcs_table_has_location_gcs(self, tmp_path):
        """Tables in GCS must have location='gcs'."""
        arrow_tbl = pa.table({"x": [1, 2, 3]})

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[("ns", "tbl")]),
            patch.object(wr, "_load_gcs_table", return_value=arrow_tbl),
        ):
            tables = wr.list_tables()

        gcs = [t for t in tables if t["location"] == "gcs"]
        assert len(gcs) == 1
        assert gcs[0]["full_name"] == "ns.tbl"
        assert gcs[0]["row_count"] == 3

    def test_both_gcs_and_local_tables_returned(self, tmp_path):
        """When catalog=gcp, returns GCS tables AND local-only tables."""
        # local table that is NOT in GCS
        _make_parquet(
            tmp_path / "local_ns" / "local_tbl",
            [{"id": 99}],
        )
        arrow_tbl = pa.table({"y": [10, 20]})

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[("gcs_ns", "gcs_tbl")]),
            patch.object(wr, "_load_gcs_table", return_value=arrow_tbl),
        ):
            tables = wr.list_tables()

        locations = {t["full_name"]: t["location"] for t in tables}
        assert locations.get("gcs_ns.gcs_tbl") == "gcs"
        assert locations.get("local_ns.local_tbl") == "local"

    def test_local_catalog_has_no_location_gcs(self, tmp_path):
        """For catalog=local, all tables report location='local'."""
        _make_parquet(tmp_path / "ns" / "tbl", [{"v": 1}])

        with (
            patch.object(wr, "_catalog", return_value="local"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
        ):
            tables = wr.list_tables()

        assert all(t["location"] == "local" for t in tables)


# ------------------------------------------------------------------ #
# F-019: query() does not wrap write/DDL statements in SELECT … LIMIT #
# ------------------------------------------------------------------ #

class TestIsWriteStatement:
    @pytest.mark.parametrize("sql", [
        "COPY (SELECT 1) TO '/tmp/x.parquet' (FORMAT PARQUET)",
        "copy (SELECT 1 LIMIT 10) to '/tmp/x.parquet'",
        "CREATE TABLE foo AS SELECT 1",
        "create or replace table foo as select 1",
        "INSERT INTO foo SELECT 1",
        "DROP TABLE foo",
        "DELETE FROM foo WHERE id = 1",
        "UPDATE foo SET x = 1",
        "ALTER TABLE foo ADD COLUMN y INT",
    ])
    def test_write_statements_detected(self, sql):
        assert wr._is_write_statement(sql) is True

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM foo",
        "select count(*) from foo",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "DESCRIBE foo",
    ])
    def test_read_statements_not_detected(self, sql):
        assert wr._is_write_statement(sql) is False


class TestQueryDoesNotWrapDDL:
    def test_copy_without_limit_is_not_wrapped(self, tmp_path):
        """COPY TO must execute without being wrapped in SELECT … LIMIT."""
        _make_parquet(tmp_path / "ns" / "tbl", [{"x": 1}, {"x": 2}])
        out = str(tmp_path / "out.parquet")

        sql = f"COPY (SELECT x FROM ns.tbl) TO '{out}' (FORMAT PARQUET)"

        with (
            patch.object(wr, "_catalog", return_value="local"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
        ):
            result = wr.query(sql)

        # COPY returns a count row, not an error
        assert isinstance(result, list)
        assert "error" not in str(result)
        assert Path(out).exists()

    def test_select_without_limit_is_wrapped(self, tmp_path):
        """SELECT without LIMIT must be capped at _MAX_ROWS."""
        rows = [{"x": i} for i in range(10)]
        _make_parquet(tmp_path / "ns" / "tbl", rows)

        sql = "SELECT x FROM ns.tbl"
        with (
            patch.object(wr, "_catalog", return_value="local"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
        ):
            result = wr.query(sql)

        # All 10 rows returned (well within _MAX_ROWS=500)
        assert len(result) == 10


# ------------------------------------------------------------------ #
# F-020: materialize_model writes a new warehouse table               #
# ------------------------------------------------------------------ #

class TestMaterializeModel:
    def test_local_catalog_writes_parquet(self, tmp_path):
        """materialize_model() creates a Parquet file in the target namespace/table."""
        _make_parquet(tmp_path / "src" / "src_tbl", [{"v": 10}, {"v": 20}])

        with (
            patch.object(wr, "_catalog", return_value="local"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
        ):
            result = wr.materialize_model("SELECT v * 2 AS doubled FROM src.src_tbl", "out", "doubled")

        assert result["ok"] is True
        assert result["row_count"] == 2
        assert result["namespace"] == "out"
        assert result["table"] == "doubled"

        out_file = tmp_path / "out" / "doubled" / "data" / "part-001.parquet"
        assert out_file.exists()

        import duckdb
        conn = duckdb.connect()
        rows = conn.execute(f"SELECT doubled FROM read_parquet('{out_file}') ORDER BY doubled").fetchall()
        assert rows == [(20,), (40,)]

    def test_result_is_queryable_after_materialization(self, tmp_path):
        """After materialize_model(), the new table is queryable via query()."""
        _make_parquet(tmp_path / "src" / "src_tbl", [{"n": 1}, {"n": 2}, {"n": 3}])

        with (
            patch.object(wr, "_catalog", return_value="local"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
        ):
            wr.materialize_model("SELECT SUM(n) AS total FROM src.src_tbl", "agg", "totals")
            rows = wr.query("SELECT total FROM agg.totals")

        assert rows == [{"total": 6}]

    def test_gcp_catalog_uploads_to_gcs(self, tmp_path):
        """When catalog=gcp, materialize_model() uploads the Parquet to GCS."""
        _make_parquet(tmp_path / "src" / "src_tbl", [{"v": 5}])

        mock_blob = MagicMock()
        mock_bucket_obj = MagicMock()
        mock_bucket_obj.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket_obj

        mock_gcs_module = MagicMock()
        mock_gcs_module.Client.return_value = mock_client

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[]),
            patch.dict("sys.modules", {"google.cloud.storage": mock_gcs_module}),
        ):
            result = wr.materialize_model("SELECT v FROM src.src_tbl", "out", "model")

        out_file = tmp_path / "out" / "model" / "data" / "part-001.parquet"
        assert out_file.exists()
        assert result["ok"] is True
        # GCS upload was attempted
        mock_blob.upload_from_filename.assert_called_once()
        assert result["location"].startswith("gs://")


# ------------------------------------------------------------------ #
# F-021: query() transparently resolves local-only tables in GCP mode #
# ------------------------------------------------------------------ #

class TestQueryLocalOnlyGcpFallback:
    def test_local_only_table_resolves_in_gcp_mode(self, tmp_path):
        """query() with catalog=gcp must resolve local-only tables from local warehouse."""
        _make_parquet(tmp_path / "myns" / "mytable", [{"x": 42}])

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[]),
        ):
            result = wr.query("SELECT x FROM myns.mytable")

        assert result == [{"x": 42}]

    def test_gcs_table_takes_priority_over_local(self, tmp_path):
        """When a table exists in both GCS and locally, GCS data is used."""
        _make_parquet(tmp_path / "ns" / "tbl", [{"v": 99}])  # local has 99
        arrow_gcs = pa.table({"v": [7]})                      # GCS has 7

        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[("ns", "tbl")]),
            patch.object(wr, "_load_gcs_table", return_value=arrow_gcs),
        ):
            result = wr.query("SELECT v FROM ns.tbl")

        assert result == [{"v": 7}]

    def test_unknown_table_still_raises(self, tmp_path):
        """query() for a table that exists nowhere raises a DuckDB error."""
        with (
            patch.object(wr, "_catalog", return_value="gcp"),
            patch.object(wr, "_warehouse", return_value=tmp_path),
            patch.object(wr, "_gcs_bucket", return_value="fake-bucket"),
            patch.object(wr, "_iter_gcs_tables", return_value=[]),
        ):
            with pytest.raises(Exception):
                wr.query("SELECT x FROM totally_unknown.table")
