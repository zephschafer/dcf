from __future__ import annotations

from typing import Any, Literal, Annotated, Union
from pydantic import BaseModel, Field, model_validator


# ------------------------------------------------------------------ #
# Source — params                                                      #
# ------------------------------------------------------------------ #

class Param(BaseModel):
    name: str
    type: Literal["string", "integer", "float", "date", "boolean"]
    format: str | None = None   # e.g. "%m/%d/%Y" for date URL serialization
    value: Any | None = None    # present → static; absent → must be covered by iterate


class Auth(BaseModel):
    type: Literal["query_param", "header", "bearer"]
    key: str            # param name or header name
    value: str          # supports "{{ env.VAR }}"


class RateLimit(BaseModel):
    requests: int
    per_minutes: float


class Response(BaseModel):
    format: Literal["json", "csv"]
    records_path: str | None = None   # key (or dot-path) in JSON holding the records array


# ------------------------------------------------------------------ #
# Source — iteration                                                   #
# ------------------------------------------------------------------ #

class DateRangeIterate(BaseModel):
    type: Literal["date_range"]
    params: list[str]       # one or two param names that receive the window start/end
    start: str              # ISO date or "today"
    end: str                # ISO date or "today"
    step: str               # e.g. "1 day", "7 days"
    window: str | None = None  # defaults to step when absent


class CategoricalIterate(BaseModel):
    type: Literal["categorical"]
    param: str
    values: list[Any]


IterateSpec = DateRangeIterate | CategoricalIterate


def _validate_dynamic_params(params: list[Param], iterate: list[IterateSpec]) -> None:
    dynamic = {p.name for p in params if p.value is None}
    covered: set[str] = set()
    for it in iterate:
        if isinstance(it, DateRangeIterate):
            covered.update(it.params)
        elif isinstance(it, CategoricalIterate):
            covered.add(it.param)
    missing = dynamic - covered
    if missing:
        raise ValueError(f"Params declared without a value or iterator: {missing}")


# ------------------------------------------------------------------ #
# Source types                                                         #
# ------------------------------------------------------------------ #

class HttpSource(BaseModel):
    type: Literal["http"]
    url: str
    method: Literal["GET", "POST"] = "GET"
    auth: Auth | None = None
    params: list[Param] = []
    iterate: list[IterateSpec] = []
    response: Response = Response(format="json")
    rate_limit: RateLimit | None = None

    @model_validator(mode="after")
    def all_dynamic_params_have_iterators(self) -> HttpSource:
        _validate_dynamic_params(self.params, self.iterate)
        return self


class PythonSource(BaseModel):
    """Calls a Python function that returns list[dict]; handles its own pagination."""
    type: Literal["python"]
    module: str       # importable module path, e.g. "connectors.craigslist_apts"
    function: str     # function name; called as fn(dynamic_params) -> list[dict]
    params: list[Param] = []
    iterate: list[IterateSpec] = []

    @model_validator(mode="after")
    def all_dynamic_params_have_iterators(self) -> PythonSource:
        _validate_dynamic_params(self.params, self.iterate)
        return self


Source = Annotated[Union[HttpSource, PythonSource], Field(discriminator="type")]


# ------------------------------------------------------------------ #
# Schema                                                               #
# ------------------------------------------------------------------ #

class CrsReprojectTransform(BaseModel):
    type: Literal["crs_reproject"]
    from_columns: list[str]
    from_crs: str
    to_crs: str
    component: Literal["x", "y"]


Transform = CrsReprojectTransform  # extend as new transforms are added


class Column(BaseModel):
    name: str
    path: str | None = None         # key in the raw record (dot-notation for nested)
    type: Literal["string", "integer", "float", "date", "timestamp", "boolean"] | None = None
    transform: Transform | None = None

    @model_validator(mode="after")
    def has_source(self) -> Column:
        if self.path is None and self.transform is None:
            raise ValueError(f"Column '{self.name}' must have either 'path' or 'transform'")
        return self


class Schema(BaseModel):
    columns: list[Column]


# ------------------------------------------------------------------ #
# Build                                                                #
# ------------------------------------------------------------------ #

class StagingConfig(BaseModel):
    partition_param: str        # which iterate param splits into separate staging tables
    table_pattern: str          # e.g. "permits_{date_type}_loader_staging"


class MergeDedup(BaseModel):
    type: Literal["latest_non_null"]
    columns: list[str]


class MergeConfig(BaseModel):
    table: str
    key: str
    dedup: MergeDedup | None = None


class Build(BaseModel):
    strategy: Literal["incremental", "append", "full_refresh"]
    primary_key: str | None = None
    staging: StagingConfig | None = None
    merge: MergeConfig | None = None


# ------------------------------------------------------------------ #
# Pipeline (top-level)                                                 #
# ------------------------------------------------------------------ #

class Pipeline(BaseModel):
    version: int = 1
    name: str
    namespace: str | None = None   # warehouse namespace; defaults to pipeline name when absent
    description: str | None = None
    source: Source
    schema_: Schema
    build: Build

    model_config = {"populate_by_name": True}

    @classmethod
    def model_fields_set(cls):
        return super().model_fields_set()

    # Allow "schema" key in YAML (reserved word in Python)
    @classmethod
    def from_dict(cls, data: dict) -> Pipeline:
        if "schema" in data and "schema_" not in data:
            data = {**data, "schema_": data.pop("schema")}
        return cls.model_validate(data)
