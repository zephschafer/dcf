"""Tests for ddt.engine.fetcher — focused on _parse_response behaviour."""
import json
from unittest.mock import MagicMock

import pytest

from ddt.config.models import HttpSource, Response
from ddt.engine.fetcher import _parse_response


def _make_response(body) -> MagicMock:
    """Return a mock requests.Response with the given JSON body."""
    resp = MagicMock()
    resp.json.return_value = body
    return resp


def _make_source(records_path: str | None) -> HttpSource:
    return HttpSource(
        type="http",
        url="https://example.com",
        response=Response(format="json", records_path=records_path),
    )


class TestParseResponseTopLevelArray:
    def test_no_records_path_returns_array(self):
        records = [{"id": 1}, {"id": 2}]
        source = _make_source(records_path=None)
        result = _parse_response(_make_response(records), source)
        assert result == records

    def test_records_path_on_array_raises_clear_error(self):
        """F-004: records_path navigating into a top-level array must raise, not silently return []."""
        records = [{"id": 1}]
        source = _make_source(records_path="items")
        with pytest.raises(ValueError, match="records_path 'items'"):
            _parse_response(_make_response(records), source)

    def test_records_path_error_message_mentions_omit(self):
        """Error should guide the user toward the correct fix."""
        source = _make_source(records_path="data")
        with pytest.raises(ValueError, match="omit records_path"):
            _parse_response(_make_response([{"id": 1}]), source)


class TestParseResponseNestedObject:
    def test_shallow_records_path(self):
        body = {"data": [{"id": 1}, {"id": 2}]}
        source = _make_source(records_path="data")
        assert _parse_response(_make_response(body), source) == [{"id": 1}, {"id": 2}]

    def test_dot_notation_records_path(self):
        body = {"response": {"results": [{"id": 3}]}}
        source = _make_source(records_path="response.results")
        assert _parse_response(_make_response(body), source) == [{"id": 3}]

    def test_missing_key_returns_empty(self):
        body = {"data": []}
        source = _make_source(records_path="data")
        assert _parse_response(_make_response(body), source) == []

    def test_wrong_key_returns_empty(self):
        body = {"records": [{"id": 1}]}
        source = _make_source(records_path="data")
        assert _parse_response(_make_response(body), source) == []
