"""Tests for Deploy model and cron validation (F-030)."""

import pytest
from pydantic import ValidationError

from ddt.config.models import Deploy, Pipeline


# ------------------------------------------------------------------ #
# Deploy model — cron validation                                       #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("schedule", [
    "0 8 * * *",        # daily at 8am
    "*/15 * * * *",     # every 15 minutes
    "0 0 1 * *",        # monthly
    "0 0 * * 0",        # weekly on Sunday
    "30 6 * * 1-5",     # weekdays at 6:30
])
def test_deploy_accepts_valid_cron(schedule):
    d = Deploy(schedule=schedule)
    assert d.schedule == schedule


@pytest.mark.parametrize("schedule", [
    "not a cron",
    "* * * *",          # only 4 fields
    "* * * * * *",      # 6 fields (seconds — not standard 5-field cron)
    "",
    "8am daily",
])
def test_deploy_rejects_invalid_cron(schedule):
    with pytest.raises(ValidationError) as exc_info:
        Deploy(schedule=schedule)
    assert "valid cron expression" in str(exc_info.value)


def test_deploy_paused_defaults_false():
    d = Deploy(schedule="0 8 * * *")
    assert d.paused is False


def test_deploy_paused_can_be_set():
    d = Deploy(schedule="0 8 * * *", paused=True)
    assert d.paused is True


# ------------------------------------------------------------------ #
# Pipeline.deploy field                                                #
# ------------------------------------------------------------------ #

_PIPELINE_BASE = {
    "name": "test_pipeline",
    "source": {
        "type": "http",
        "url": "https://example.com/api",
    },
    "schema": {
        "columns": [{"name": "id", "path": "id", "type": "integer"}],
    },
    "build": {"strategy": "incremental", "primary_key": "id"},
}


def test_pipeline_deploy_optional():
    p = Pipeline.from_dict(_PIPELINE_BASE)
    assert p.deploy is None


def test_pipeline_deploy_parsed():
    data = {**_PIPELINE_BASE, "deploy": {"schedule": "0 8 * * *"}}
    p = Pipeline.from_dict(data)
    assert p.deploy is not None
    assert p.deploy.schedule == "0 8 * * *"
    assert p.deploy.paused is False


def test_pipeline_deploy_invalid_cron_raises():
    data = {**_PIPELINE_BASE, "deploy": {"schedule": "not a cron"}}
    with pytest.raises(ValidationError) as exc_info:
        Pipeline.from_dict(data)
    assert "valid cron expression" in str(exc_info.value)
