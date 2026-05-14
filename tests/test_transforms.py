"""Tests for ddt.engine.transforms — including the new array_join transform."""
import pytest

from ddt.config.models import ArrayJoinTransform
from ddt.engine.transforms import apply_transform


class TestArrayJoin:
    def _t(self, path="topics", separator=","):
        return ArrayJoinTransform(type="array_join", path=path, separator=separator)

    def test_joins_string_elements(self):
        record = {"topics": ["java", "tapestry", "web-framework"]}
        assert apply_transform(self._t(), record) == "java,tapestry,web-framework"

    def test_custom_separator(self):
        record = {"topics": ["a", "b", "c"]}
        assert apply_transform(self._t(separator=" | "), record) == "a | b | c"

    def test_empty_array_returns_empty_string(self):
        record = {"topics": []}
        assert apply_transform(self._t(), record) == ""

    def test_null_field_returns_none(self):
        record = {"topics": None}
        assert apply_transform(self._t(), record) is None

    def test_missing_field_returns_none(self):
        record = {}
        assert apply_transform(self._t(), record) is None

    def test_non_array_value_coerced_to_string(self):
        record = {"topics": "already-a-string"}
        assert apply_transform(self._t(), record) == "already-a-string"

    def test_dot_path_into_nested_array(self):
        record = {"meta": {"tags": ["x", "y"]}}
        assert apply_transform(self._t(path="meta.tags"), record) == "x,y"
