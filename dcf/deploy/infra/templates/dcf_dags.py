# Managed by dcf — do not edit manually.
# This file is uploaded to the GCS dags bucket as a platform resource.
# It discovers per-collector YAML configs and registers one Airflow DAG per collector.
import glob

import yaml
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

_DAGS_DIR = "/opt/airflow/dags"

for _config_path in glob.glob(f"{_DAGS_DIR}/collectors/*.yml"):
    with open(_config_path) as _f:
        _cfg = yaml.safe_load(_f)
    _name = _cfg["name"]
    with DAG(
        dag_id=_name,
        schedule_interval=_cfg["schedule"],
        start_date=datetime(2024, 1, 1),
        catchup=False,
        is_paused_upon_creation=_cfg.get("paused", False),
        tags=["dcf"],
    ) as _dag:
        CloudRunExecuteJobOperator(
            task_id=f"run_{_name}",
            project_id=_cfg["project_id"],
            region=_cfg["region"],
            job_name=_cfg["cloud_run_job"],
        )
    globals()[_name] = _dag
