"""Compile step: diff project collector config against deployed state, generate per-collector .tf.json files."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .batch_deploy import _content_hash, _image_uri, _sync_build_context, _tf_state_dir

_DCF_DEPLOY_DIR = Path(__file__).parent.parent  # dcf/deploy/
_DOCKERFILE_TEMPLATE = _DCF_DEPLOY_DIR / "infra" / "templates" / "batch_collector.Dockerfile.tftpl"
_BUILD_DIR = Path.home() / ".dcf" / "build"

# Matches old for_each Terraform state addresses: TYPE.collector["name"]
_FOREACH_ADDR_RE = re.compile(
    r'^(google_cloud_run_v2_job|local_file|null_resource)\.collector\["(.+)"\]$'
)


@dataclass
class CompilePlan:
    new: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    hashes: dict[str, str] = field(default_factory=dict)
    collectors: dict = field(default_factory=dict)  # name → Collector

    @property
    def all_collector_names(self) -> list[str]:
        return self.new + self.changed + self.unchanged


def compile_project(
    project_root: Path,
    gcp_config: dict,
    filter_names: list[str] | None = None,
) -> CompilePlan:
    """Diff project collector config against deployed state and generate .tf.json files.

    Writes collector_{name}.tf.json into the Terraform work directory for new/changed
    collectors. Deletes .tf.json for removed collectors when filter_names is None (full compile).
    Returns a CompilePlan describing what changed.
    """
    from ...config.loader import load_all_collectors

    project_id = gcp_config["project_id"]
    region = gcp_config["region"]
    tf_dir = _tf_state_dir(project_root) / "gcp"
    tf_dir.mkdir(parents=True, exist_ok=True)

    # Load collectors from project, scoped to filter if provided
    all_collectors = load_all_collectors(project_root / "collectors", resolve_env=False)
    batch_collectors = [c for c in all_collectors if c.deployment is not None and c.deployment.type == "batch"]
    if filter_names is not None:
        batch_collectors = [c for c in batch_collectors if c.name in filter_names]

    deployed = gcp_config.get("deployments", {})

    plan = CompilePlan()

    active_names: set[str] = set()
    for collector in batch_collectors:
        name = collector.name
        active_names.add(name)

        build_context = _sync_build_context(project_root, name, gcp_config)
        content_hash = _content_hash(build_context, collector_name=name)
        image_uri = _image_uri(project_id, region, name)

        tf_json_path = tf_dir / f"collector_{name}.tf.json"
        stored_hash = deployed.get(name, {}).get("content_hash", "")

        if not tf_json_path.exists():
            plan.new.append(name)
        elif content_hash != stored_hash:
            plan.changed.append(name)
        else:
            plan.unchanged.append(name)
            continue  # no file write needed

        plan.hashes[name] = content_hash
        plan.collectors[name] = collector

        from ...config.models import SqlSource, CloudSqlConnection
        cloud_sql_instances: list[str] = []
        if (isinstance(collector.source, SqlSource) and
                isinstance(collector.source.connection, CloudSqlConnection)):
            cloud_sql_instances = [collector.source.connection.instance]

        tf_json = generate_collector_tf_json(
            name=name,
            schedule=collector.deployment.schedule,
            paused=collector.deployment.paused,
            project_id=project_id,
            region=region,
            build_context_str=str(build_context),
            content_hash=content_hash,
            java_enabled=False,
            cloud_sql_instances=cloud_sql_instances,
            image_uri=image_uri,
        )
        tf_json_path.write_text(json.dumps(tf_json, indent=2))

    # Full compile: remove .tf.json for collectors no longer in project
    if filter_names is None:
        for tf_json_path in tf_dir.glob("collector_*.tf.json"):
            # .tf.json has two suffixes; strip the full suffix before extracting the name
            collector_name = tf_json_path.name.removesuffix(".tf.json")[len("collector_"):]
            if collector_name not in active_names:
                tf_json_path.unlink()
                plan.deleted.append(collector_name)

    return plan


def generate_collector_tf_json(
    name: str,
    schedule: str,
    paused: bool,
    project_id: str,
    region: str,
    build_context_str: str,
    content_hash: str,
    java_enabled: bool,
    cloud_sql_instances: list[str],
    image_uri: str,
) -> dict:
    """Generate the Terraform JSON configuration dict for a single collector.

    The returned dict serializes directly to a valid .tf.json file that Terraform
    reads alongside the static main.tf in the work directory.
    """
    job_name = f"dcf-job-{name.replace('_', '-')}"
    dockerfile_content = _render_dockerfile(java_enabled)

    build_cmd = (
        f"n=0; until gcloud builds submit"
        f" --project {project_id}"
        f" --region {region}"
        f" --tag {image_uri}"
        f" --timeout 600s"
        f" {build_context_str};"
        f" do n=$((n+1)); if [ $n -ge 6 ]; then exit 1; fi;"
        f' echo "Cloud Build not ready, retrying in 15s (attempt $n/6)...";'
        f" sleep 15; done"
    )

    dag_config_content = yaml.dump(
        {
            "name": name,
            "schedule": schedule,
            "paused": paused,
            "project_id": project_id,
            "region": region,
            "cloud_run_job": job_name,
        },
        default_flow_style=False,
        sort_keys=False,
    )

    # Build the Cloud Run job spec, conditionally including Cloud SQL blocks
    job_template_inner: dict = {
        "service_account": "${local.sa_email}",
        "max_retries": 0,
        "containers": [
            {
                "image": image_uri,
                "env": [
                    {"name": "COLLECTOR_NAME", "value": name},
                    {"name": "DCF_PROJECT_DIR", "value": "/app"},
                ],
                "resources": {"limits": {"memory": "512Mi"}},
            }
        ],
    }

    if cloud_sql_instances:
        job_template_inner["volumes"] = [
            {
                "name": "cloudsql",
                "cloud_sql_instance": {"instances": cloud_sql_instances},
            }
        ]
        job_template_inner["containers"][0]["volume_mounts"] = [
            {"name": "cloudsql", "mount_path": "/cloudsql"}
        ]

    return {
        "resource": {
            "local_file": {
                f"collector_dockerfile_{name}": {
                    "content": dockerfile_content,
                    "filename": f"{build_context_str}/Dockerfile",
                }
            },
            "null_resource": {
                f"collector_build_{name}": {
                    "depends_on": [
                        f"local_file.collector_dockerfile_{name}",
                        "google_artifact_registry_repository.dcf_runner",
                    ],
                    "triggers": {
                        "content_hash": content_hash,
                        "java_enabled": str(java_enabled).lower(),
                    },
                    "provisioner": [{"local-exec": {"command": build_cmd}}],
                }
            },
            "google_cloud_run_v2_job": {
                f"collector_{name}": {
                    "depends_on": [f"null_resource.collector_build_{name}"],
                    "name": job_name,
                    "location": region,
                    "template": {
                        "template": job_template_inner,
                    },
                }
            },
            "google_storage_bucket_object": {
                f"dag_config_{name}": {
                    "bucket": "${google_storage_bucket.dags.name}",
                    "name": f"collectors/{name}.yml",
                    "content": dag_config_content,
                }
            },
        }
    }


def _render_dockerfile(java_enabled: bool) -> str:
    """Render batch_collector.Dockerfile.tftpl in Python, evaluating the java_enabled conditional."""
    template_text = _DOCKERFILE_TEMPLATE.read_text()
    lines = template_text.splitlines(keepends=True)

    result: list[str] = []
    skip = False
    for line in lines:
        stripped = line.strip()
        if stripped == "%{ if java_enabled ~}":
            skip = not java_enabled
            continue
        if stripped == "%{ endif ~}":
            skip = False
            continue
        if not skip:
            result.append(line)

    return "".join(result)
