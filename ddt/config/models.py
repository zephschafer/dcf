from __future__ import annotations

import re as _re
from typing import Any, Literal, Annotated, Union
from pydantic import BaseModel, Field, model_validator

_CRON_FIELD_RE = _re.compile(r'^(\*|[0-9,\-\*/]+)$')


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
    key: str | None = None   # param name or header name; unused (and optional) for bearer
    value: str               # supports "{{ env.VAR }}"

    @model_validator(mode="after")
    def key_required_for_non_bearer(self) -> "Auth":
        if self.type in ("query_param", "header") and not self.key:
            raise ValueError(f"auth.key is required when type is '{self.type}'")
        return self


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


class PubSubSource(BaseModel):
    """Continuously reads JSON messages from a GCP Pub/Sub subscription."""
    type: Literal["pubsub"]
    subscription: str   # full resource path: projects/<project>/subscriptions/<name>


Source = Annotated[Union[HttpSource, PythonSource, PubSubSource], Field(discriminator="type")]


# ------------------------------------------------------------------ #
# Schema                                                               #
# ------------------------------------------------------------------ #

class CrsReprojectTransform(BaseModel):
    type: Literal["crs_reproject"]
    from_columns: list[str]
    from_crs: str
    to_crs: str
    component: Literal["x", "y"]


class ArrayJoinTransform(BaseModel):
    type: Literal["array_join"]
    path: str              # dot-notation path to the array field in the raw record
    separator: str = ","   # delimiter used to join elements


Transform = Annotated[
    Union[CrsReprojectTransform, ArrayJoinTransform],
    Field(discriminator="type"),
]


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
# Deploy                                                               #
# ------------------------------------------------------------------ #

class Deploy(BaseModel):
    type: Literal["batch", "streaming"] = "batch"
    # batch fields
    schedule: str | None = None
    paused: bool = False
    # streaming fields
    window_seconds: int = 60

    @model_validator(mode="after")
    def validate_deploy(self) -> "Deploy":
        if self.type == "batch":
            if not self.schedule:
                raise ValueError(
                    "deploy.schedule is required for batch deployments "
                    "(e.g. schedule: \"0 8 * * *\")"
                )
            parts = self.schedule.strip().split()
            if len(parts) != 5 or not all(_CRON_FIELD_RE.match(p) for p in parts):
                raise ValueError(
                    f"deploy.schedule '{self.schedule}' is not a valid cron expression. "
                    "Expected 5 space-separated fields: minute hour day-of-month month day-of-week "
                    "(e.g. '0 8 * * *' for daily at 8 AM UTC)"
                )
        return self


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
    deploy: Deploy | None = None

    model_config = {"populate_by_name": True}

    @classmethod
    def model_fields_set(cls):
        return super().model_fields_set()

    @model_validator(mode="after")
    def streaming_constraints(self) -> "Pipeline":
        if isinstance(self.source, PubSubSource):
            if self.build.strategy != "append":
                raise ValueError(
                    "Streaming pipelines (source.type: pubsub) require "
                    "build.strategy: append — incremental and full_refresh are not supported"
                )
            if self.deploy and self.deploy.type != "streaming":
                raise ValueError(
                    "Pipelines with source.type: pubsub must use deploy.type: streaming"
                )
        return self

    # Allow "schema" key in YAML (reserved word in Python)
    @classmethod
    def from_dict(cls, data: dict) -> Pipeline:
        if "schema" in data and "schema_" not in data:
            data = {**data, "schema_": data["schema"]}
            del data["schema"]
        return cls.model_validate(data)
