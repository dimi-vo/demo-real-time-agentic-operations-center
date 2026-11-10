"""Write the credential card (`DEMO.env` + `DEMO.md`) the developer's tools use.

The card carries everything `ops-sim`, `ops-wall`, `reset`, `setup-rtce` and the Flink
workspace need. On the pre-provisioned track the Flink and RTCE credentials are the
developer's own Global key; the Kafka and Schema Registry keys are their own too.
"""

from __future__ import annotations

from pathlib import Path

from scripts.common import deployment_meta as meta
from scripts.common.credentials import set_active_card
from scripts.common.paths import credentials_dir
from scripts.common.ui import display_value


def _rtce_endpoint(region: str, org_id: str, env_id: str, cluster_id: str) -> str:
    if not (region and org_id and env_id and cluster_id):
        return ""
    return (
        f"https://mcp.{region}.aws.confluent.cloud/mcp/v1/context-engine/"
        f"organizations/{org_id}/environments/{env_id}/kafka-clusters/{cluster_id}"
    )


def card_fields(prefix: str, email: str, out: dict, region: str) -> dict[str, str]:
    """Flatten the resolved deployment into the AIOPS_* card fields.

    `out` carries flat keys plus a nested `attendee_credentials` for the Kafka/SR
    secrets, so the same shape serves whether it came from Terraform outputs or the
    pre-provisioned deploy's REST lookups.
    """
    ac = out.get("attendee_credentials", {})
    cluster_id = out.get("cluster_id", "") or ac.get("cluster_id", "")
    env_id = out.get("environment_id", "")
    env_url = ac.get("environment_url", "") or f"https://confluent.cloud/environments/{env_id}"
    return {
        "AIOPS_PREFIX": prefix,
        "AIOPS_OWNER_EMAIL": email,
        "AIOPS_ENVIRONMENT_ID": env_id,
        "AIOPS_ENVIRONMENT_URL": env_url,
        "AIOPS_ORGANIZATION_ID": out.get("organization_id", ""),
        "AIOPS_CATALOG": out.get("environment_name", ""),
        "AIOPS_DATABASE": out.get("cluster_name", ""),
        "AIOPS_CLUSTER_ID": cluster_id,
        "AIOPS_COMPUTE_POOL_ID": out.get("compute_pool_id", ""),
        "AIOPS_FLINK_REST_ENDPOINT": out.get("flink_rest_endpoint", ""),
        "AIOPS_FLINK_API_KEY": out.get("flink_api_key", ""),
        "AIOPS_FLINK_API_SECRET": out.get("flink_api_secret", ""),
        "AIOPS_KAFKA_BOOTSTRAP": out.get("cluster_bootstrap", "") or ac.get("cluster_bootstrap", ""),
        "AIOPS_KAFKA_API_KEY": ac.get("kafka_api_key", ""),
        "AIOPS_KAFKA_API_SECRET": ac.get("kafka_api_secret", ""),
        "AIOPS_SCHEMA_REGISTRY_URL": ac.get("schema_registry_url", ""),
        "AIOPS_SR_API_KEY": ac.get("sr_api_key", ""),
        "AIOPS_SR_API_SECRET": ac.get("sr_api_secret", ""),
        "AIOPS_RTCE_MCP_ENDPOINT": _rtce_endpoint(region, out.get("organization_id", ""), env_id, cluster_id),
        "AIOPS_RTCE_API_KEY": out.get("rtce_api_key", ""),
        "AIOPS_RTCE_API_SECRET": out.get("rtce_api_secret", ""),
    }


def _write_env(creds_dir: Path, prefix: str, fields: dict[str, str]) -> Path:
    creds_dir.mkdir(parents=True, exist_ok=True)
    path = creds_dir / f"{prefix}.env"
    lines = [f"{k}={v}" for k, v in fields.items() if k != "AIOPS_PREFIX"]
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)
    return path


def _write_md(creds_dir: Path, prefix: str, fields: dict[str, str]) -> Path:
    path = creds_dir / f"{prefix}.md"
    rows = [
        f"| `{k}` | {display_value(k, v)} |"
        for k, v in fields.items()
        if k not in ("AIOPS_PREFIX",)
    ]
    body = (
        f"# Credential card: {prefix}\n\n"
        f"Workshop sign-in: open {fields.get('AIOPS_ENVIRONMENT_URL', '')}\n\n"
        "Secrets are masked here; the full values are in the matching `.env` file "
        "(keep it private — never commit it).\n\n"
        "| Field | Value |\n|---|---|\n" + "\n".join(rows) + "\n"
    )
    path.write_text(body)
    return path


def write_card(root: Path, cfg: dict, out: dict, region: str) -> Path:
    """Write DEMO.env + DEMO.md, point credentials.env at the card, record its path."""
    fields = card_fields(cfg["prefix"], cfg["owner_email"], out, region)
    creds_dir = credentials_dir(root)
    card = _write_env(creds_dir, cfg["prefix"], fields)
    _write_md(creds_dir, cfg["prefix"], fields)
    set_active_card(root, card)
    meta.save_meta(root, **{meta.KEY_CARD: str(card.relative_to(root))})
    return card
