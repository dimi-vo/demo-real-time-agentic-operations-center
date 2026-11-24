"""Readiness check behaviour: table check uses one Flink `SHOW TABLES`, both table and
model errors surface with `?` instead of being mistaken for "missing" (D-020)."""

from __future__ import annotations

from pathlib import Path

import pytest
import requests

from scripts.preprovisioned import provision

CARD_FIELDS = {
    "AIOPS_FLINK_REST_ENDPOINT": "https://flink.example/",
    "AIOPS_ORGANIZATION_ID": "org",
    "AIOPS_ENVIRONMENT_ID": "env",
    "AIOPS_COMPUTE_POOL_ID": "pool",
    "AIOPS_CATALOG": "cat",
    "AIOPS_DATABASE": "db",
    "AIOPS_FLINK_API_KEY": "k",
    "AIOPS_FLINK_API_SECRET": "s",
}


def _write_card(tmp_path: Path) -> Path:
    card = tmp_path / "DEMO.env"
    card.write_text("\n".join(f"{k}={v}" for k, v in CARD_FIELDS.items()) + "\n")
    return card


@pytest.fixture
def card_path(tmp_path: Path) -> Path:
    return _write_card(tmp_path)


def _patch(monkeypatch, *, tables=None, tables_error=None, model=None, model_error=None):
    def fake_list_tables(session):
        if tables_error is not None:
            raise tables_error
        return set(tables or ())

    def fake_model_exists(session, name):
        if model_error is not None:
            raise model_error
        return bool(model)

    monkeypatch.setattr(provision, "list_tables", fake_list_tables)
    monkeypatch.setattr(provision, "model_exists", fake_model_exists)


def test_all_present(monkeypatch, capsys, card_path):
    _patch(monkeypatch, tables=provision.PLATFORM_TABLES, model=True)
    assert provision._verify_ready(card_path.parent, card_path) is True
    out = capsys.readouterr().out
    for table in provision.PLATFORM_TABLES:
        assert f"✓ table {table}" in out
    assert f"✓ model {provision.MODEL_NAME}" in out
    assert "✗" not in out and "?" not in out


def test_one_table_missing(monkeypatch, capsys, card_path):
    _patch(monkeypatch, tables=("service_metrics",), model=True)
    assert provision._verify_ready(card_path.parent, card_path) is False
    out = capsys.readouterr().out
    assert "✓ table service_metrics" in out
    assert "✗ table service_deployments is missing" in out
    assert "✗ table service_incidents is missing" in out


def test_table_check_error_prints_question_mark_not_missing(monkeypatch, capsys, card_path):
    _patch(monkeypatch, tables_error=RuntimeError("submit failed (401): Unauthorized"), model=True)
    assert provision._verify_ready(card_path.parent, card_path) is False
    out = capsys.readouterr().out
    for table in provision.PLATFORM_TABLES:
        assert f"? could not check table {table}" in out
        assert "401" in out
        assert f"✗ table {table} is missing" not in out


def test_table_check_network_error_is_also_a_question_mark(monkeypatch, capsys, card_path):
    _patch(monkeypatch, tables_error=requests.ConnectionError("DNS fail"), model=True)
    assert provision._verify_ready(card_path.parent, card_path) is False
    out = capsys.readouterr().out
    assert "? could not check table service_metrics" in out
    assert "DNS fail" in out


def test_model_check_error_surfaces(monkeypatch, capsys, card_path):
    _patch(
        monkeypatch,
        tables=provision.PLATFORM_TABLES,
        model_error=RuntimeError("submit failed (500): Internal Server Error"),
    )
    assert provision._verify_ready(card_path.parent, card_path) is False
    out = capsys.readouterr().out
    assert "? could not check llm_textgen_model" in out
    assert "500" in out


def test_the_two_output_tables_are_checked_and_a_missing_one_names_the_platform_team(
    monkeypatch, capsys, card_path
):
    _patch(monkeypatch, tables=provision.SOURCE_TABLES, model=True)
    assert provision._verify_ready(card_path.parent, card_path) is False
    out = capsys.readouterr().out
    for table in ("service_health", "remediation_decisions"):
        assert f"✗ table {table} is missing — ask your platform team to re-run their apply." in out
    for table in provision.SOURCE_TABLES:
        assert f"✓ table {table}" in out


def test_source_tables_includes_service_incidents():
    # The Phase 5 addition is what motivated discovering this bug: the old SR-based
    # check wouldn't have caught it at all. Pin it so future table additions remember
    # the readiness check.
    assert "service_incidents" in provision.SOURCE_TABLES


def test_the_ticket_source_tables_are_checked_too():
    # D-068: checkout's tickets and their answer key; the generator writes both.
    assert provision.SOURCE_TABLES == (
        "service_metrics",
        "service_deployments",
        "service_incidents",
        "support_tickets",
        "ticket_answer_key",
    )
    assert len(provision.PLATFORM_TABLES) == 8
