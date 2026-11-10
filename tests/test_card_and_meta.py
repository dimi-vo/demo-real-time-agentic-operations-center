"""Credential card generation, the deployment record, and card resolution."""

from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts.common import card as card_mod
from scripts.common import credentials
from scripts.common import deployment_meta as meta


def _out():
    return {
        "organization_id": "org-1",
        "environment_id": "env-1",
        "environment_name": "jane-doe-7f3a_env",
        "cluster_id": "lkc-1",
        "cluster_name": "RIVER-OOPS-DEMO-CLUSTER",
        "cluster_bootstrap": "SASL_SSL://pkc-x.us-east-1.aws.confluent.cloud:9092",
        "compute_pool_id": "lfcp-1",
        "flink_rest_endpoint": "https://flink.us-east-1.aws.confluent.cloud",
        "flink_api_key": "GKEY",
        "flink_api_secret": "GSEC",
        "rtce_api_key": "GKEY",
        "rtce_api_secret": "GSEC",
        "attendee_credentials": {
            "environment_url": "https://confluent.cloud/environments/env-1",
            "cluster_id": "lkc-1",
            "cluster_bootstrap": "SASL_SSL://pkc-x.us-east-1.aws.confluent.cloud:9092",
            "kafka_api_key": "KKEY",
            "kafka_api_secret": "KSEC",
            "schema_registry_url": "https://psrc-x.us-east-1.aws.confluent.cloud",
            "sr_api_key": "SKEY",
            "sr_api_secret": "SSEC",
        },
    }


def test_card_fields_values():
    f = card_mod.card_fields("DEMO", "jane.doe@x.com", _out(), region="us-east-1")
    assert f["AIOPS_CATALOG"] == "jane-doe-7f3a_env"
    assert f["AIOPS_DATABASE"] == "RIVER-OOPS-DEMO-CLUSTER"
    assert f["AIOPS_FLINK_API_KEY"] == "GKEY"
    assert f["AIOPS_KAFKA_API_KEY"] == "KKEY"
    assert f["AIOPS_SR_API_SECRET"] == "SSEC"
    assert f["AIOPS_RTCE_API_KEY"] == "GKEY"  # RTCE key is the Global key
    assert f["AIOPS_KAFKA_BOOTSTRAP"].endswith(":9092")
    assert f["AIOPS_RTCE_MCP_ENDPOINT"] == (
        "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/"
        "organizations/org-1/environments/env-1/kafka-clusters/lkc-1"
    )


def test_write_card_writes_env_md_pointer_and_record(tmp_path: Path):
    root = tmp_path
    card = card_mod.write_card(root, {"prefix": "DEMO", "owner_email": "jane@x.com"}, _out(), region="us-east-1")
    assert card.name == "DEMO.env"
    assert card.with_suffix(".md").is_file()
    # 0600 perms on the secret-bearing env file
    assert (card.stat().st_mode & 0o777) == 0o600
    values = dotenv_values(card)
    assert values["AIOPS_KAFKA_API_KEY"] == "KKEY"
    assert "AIOPS_PREFIX" not in values  # prefix is the filename, not a field
    # credentials.env points at it; deployment.env records its path
    _, loaded = credentials.load_card(root=root)
    assert loaded["AIOPS_SR_API_KEY"] == "SKEY"
    assert meta.load_meta(root)[meta.KEY_CARD] == "runs/pre-provisioned/credentials/DEMO.env"


def test_meta_roundtrip_and_key_ids(tmp_path: Path):
    root = tmp_path
    meta.save_meta(root, **{meta.KEY_GLOBAL_KEY_ID: "G1", meta.KEY_ENVIRONMENT_ID: "env-1"})
    meta.save_meta(root, **{meta.KEY_KAFKA_KEY_ID: "K1", meta.KEY_SR_KEY_ID: "S1"})
    assert meta.is_deployed(root)
    ids = meta.recorded_key_ids(root)
    assert ids == {meta.KEY_GLOBAL_KEY_ID: "G1", meta.KEY_KAFKA_KEY_ID: "K1", meta.KEY_SR_KEY_ID: "S1"}
    meta.drop_meta(root, meta.KEY_KAFKA_KEY_ID)
    assert meta.KEY_KAFKA_KEY_ID not in meta.recorded_key_ids(root)
    meta.clear_meta(root)
    assert not meta.is_deployed(root)


def test_resolve_card_precedence(tmp_path: Path):
    root = tmp_path
    creds = root / "runs" / "pre-provisioned" / "credentials"
    creds.mkdir(parents=True)
    a = creds / "DEMO.env"
    a.write_text("AIOPS_KAFKA_API_KEY=KKEY\n")
    # single card -> found without a pointer
    assert credentials.resolve_card(None, root) == a
    # explicit wins
    b = tmp_path / "other.env"
    b.write_text("AIOPS_KAFKA_API_KEY=OTHER\n")
    assert credentials.resolve_card(str(b), root) == b


def test_resolve_card_none_and_multiple(tmp_path: Path):
    root = tmp_path
    with pytest.raises(SystemExit):
        credentials.resolve_card(None, root)
    creds = root / "runs" / "pre-provisioned" / "credentials"
    creds.mkdir(parents=True)
    (creds / "A.env").write_text("X=1\n")
    (creds / "B.env").write_text("X=2\n")
    with pytest.raises(SystemExit):
        credentials.resolve_card(None, root)
