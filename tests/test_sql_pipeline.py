"""The 2.x pipeline files build on each other, so their shared pieces must stay identical.

The walkthrough has attendees grow one statement step by step (join, window, detect,
CTAS). These tests keep the canonical files in sql/ in step with each other and with
the brief: the same `enriched` and `windowed` CTEs everywhere, the 2.4 CTAS body equal
to the 2.3 status query, the same detector config for both metrics, the fixed 10-second
window, explicit earliest-offset hints, and the service_health column list.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

PIPELINE_FILES = (
    "2.2_window_aggregation.sql",
    "2.3_anomaly_detection.sql",
    "2.3_anomaly_status.sql",
    "2.4_service_health.sql",
)
DETECTION_FILES = ("2.3_anomaly_detection.sql", "2.3_anomaly_status.sql", "2.4_service_health.sql")

# D-014 tuned in D-029: AR(1), minTrainingSize 12 (tuning history in DECISIONS.md).
DETECTOR_CONFIG = (
    "JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.9, "
    "'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)"
)
DETECTOR_OVER = (
    "OVER (PARTITION BY service_name ORDER BY window_time "
    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
)

# Brief sections 5.2-5.4, in order.
SERVICE_HEALTH_COLUMNS = (
    "service_name", "window_start", "window_end",
    "request_count", "error_count", "error_rate", "avg_latency_ms", "max_latency_ms", "avg_response_bytes",
    "version", "previous_version", "last_change", "seconds_since_deploy",
    "ready_replicas", "desired_replicas", "tier",
    "latency_status", "expected_latency_ms", "latency_upper_bound",
    "traffic_status", "expected_request_count", "traffic_lower_bound", "traffic_upper_bound",
    "error_rate_breach",
)  # fmt: skip

EARLIEST = "/*+ OPTIONS('scan.startup.mode'='earliest-offset') */"


def _body(name: str) -> str:
    """A sql/ file without its leading -- comment lines."""
    lines = (SQL / name).read_text().splitlines()
    return "\n".join(line for line in lines if not line.lstrip().startswith("--")).strip()


def _norm(text: str) -> str:
    return " ".join(text.split())


def _cte(body: str, name: str) -> str:
    """The text of one CTE (`name AS ( ... )`), found by balancing parentheses."""
    m = re.search(rf"\b{name} AS \(", body)
    assert m, f"no CTE {name}"
    depth, i = 0, m.end() - 1
    for j in range(i, len(body)):
        if body[j] == "(":
            depth += 1
        elif body[j] == ")":
            depth -= 1
            if depth == 0:
                return body[m.start() : j + 1]
    raise AssertionError(f"unbalanced CTE {name}")


@pytest.mark.parametrize("name", PIPELINE_FILES)
def test_enriched_cte_identical_everywhere(name):
    reference = _cte(_body("2.2_window_aggregation.sql"), "enriched")
    assert _norm(_cte(_body(name), "enriched")) == _norm(reference)


@pytest.mark.parametrize("name", DETECTION_FILES)
def test_windowed_cte_is_the_22_query(name):
    # 2.2 part 2's SELECT becomes the `windowed` CTE of 2.3 and 2.4.
    q22 = _body("2.2_window_aggregation.sql")
    select_22 = q22[q22.index(")\nSELECT") + 2 :].rstrip(";")
    windowed = _cte(_body(name), "windowed")
    inner = windowed[windowed.index("(") + 1 : -1]
    assert _norm(inner) == _norm(select_22)


def test_ctas_body_is_the_23_status_query():
    ctas = _body("2.4_service_health.sql")
    head = (
        "CREATE TABLE `service_health`\n"
        "WITH ('changelog.mode' = 'append', 'kafka.consumer.isolation-level' = 'read-uncommitted')\n"
        "AS\n"
    )
    assert ctas.startswith(head)
    assert _norm(ctas[len(head) :]) == _norm(_body("2.3_anomaly_status.sql"))


@pytest.mark.parametrize("name", DETECTION_FILES)
def test_two_detectors_same_config_same_window(name):
    body = _norm(_body(name))
    assert body.count("ML_DETECT_ANOMALIES(") == 2
    assert body.count(_norm(DETECTOR_CONFIG)) == 2
    assert body.count(DETECTOR_OVER) == 2
    assert "ML_DETECT_ANOMALIES( avg_latency_ms, window_time," in body
    assert "ML_DETECT_ANOMALIES( CAST(request_count AS DOUBLE), window_time," in body


@pytest.mark.parametrize("name", PIPELINE_FILES)
def test_both_sources_read_from_earliest_offset(name):
    body = _norm(_body(name))
    assert f"`service_requests` {EARLIEST} r" in body
    assert f"`service_deployments` {EARLIEST} FOR SYSTEM_TIME AS OF r.event_time AS d" in body


@pytest.mark.parametrize("name", [*PIPELINE_FILES, "2.2_window_metrics.sql"])
def test_window_is_fixed_at_ten_seconds(name):
    body = _body(name)
    assert "TUMBLE(" in body
    assert re.findall(r"INTERVAL '(\d+)' SECOND", body) == ["10"]


def test_deployment_context_uses_last_value_not_max():
    body = _body("2.2_window_aggregation.sql")
    context = ("version", "previous_version", "last_change", "ready_replicas", "desired_replicas")
    context += ("tier",)
    for col in context:
        assert f"LAST_VALUE({col}) AS {col}" in body
    assert "TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy" in body
    assert not re.search(r"MAX\((version|previous_version|last_change|tier)\)", body)


def test_service_health_columns_in_order():
    body = _body("2.4_service_health.sql")
    final = body[body.rindex("\nSELECT") :]
    # each output column is the last identifier of its select item
    items, depth, cur = [], 0, ""
    for ch in final[len("\nSELECT") : final.rindex("FROM anomaly")]:
        if ch in "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            items.append(cur)
            cur = ""
        else:
            cur += ch
    items.append(cur)
    names = [re.findall(r"\w+", item)[-1] for item in items]
    assert tuple(names) == SERVICE_HEALTH_COLUMNS


def test_regular_join_antipattern_reads_new_requests_only():
    # A regular join keeps every request in state and re-emits all of a service's rows
    # on every update of service_deployments (every 5 s re-emit); starting from the
    # latest offset keeps the demo small.
    for name in ("2.1_regular_join.sql", "2.1_regular_join_by_version.sql"):
        body = _norm(_body(name))
        assert "FOR SYSTEM_TIME AS OF" not in body
        assert "`service_requests` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ r" in body


def test_temporal_join_solution_hints_both_sides():
    body = _norm(_body("2.1_temporal_join.sql"))
    assert f"`service_requests` {EARLIEST} r" in body
    assert f"`service_deployments` {EARLIEST} FOR SYSTEM_TIME AS OF r.event_time AS d" in body
