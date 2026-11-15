"""The 2.x pipeline files build on each other, so their shared pieces must stay identical.

The walkthrough has attendees grow one statement step by step (join, window, detect,
INSERT). These tests keep the canonical files in sql/ in step with each other and with
the brief: the same `enriched` and `windowed` CTEs everywhere, the 2.4 INSERT body equal
to the 2.3 status query, the same detector config for both metrics, the fixed 10-second
window, source watermarks that let a window close on the tick at its end, explicit
earliest-offset hints, and the service_health column list (in the 2.3 query and in the
platform team's DDL).
"""

import re
from pathlib import Path

import pytest

from opsgen.engine import WINDOW_SECONDS
from opsgen.simulation import REEMIT_INTERVAL, SAMPLE_OFFSET_MS

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"

PIPELINE_FILES = (
    "2.2_window_aggregation.sql",
    "2.3_anomaly_detection.sql",
    "2.3_anomaly_status.sql",
    "2.4_service_health.sql",
)
DETECTION_FILES = ("2.3_anomaly_detection.sql", "2.3_anomaly_status.sql", "2.4_service_health.sql")

# D-014 tuned in D-029 and D-059: AR(1), minTrainingSize 12, confidence 99.95 (tuning
# history in DECISIONS.md).
DETECTOR_CONFIG = (
    "JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, "
    "'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)"
)
DETECTOR_OVER = (
    "OVER (PARTITION BY service_name ORDER BY window_time "
    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
)

# Brief sections 5.2-5.4, in order, then detected_at (D-054); no error columns and
# traffic_count names since D-058.
SERVICE_HEALTH_COLUMNS = (
    "service_name", "window_start", "window_end",
    "traffic_count", "avg_latency_ms",
    "version", "previous_version", "last_change", "seconds_since_deploy",
    "ready_replicas", "tier",
    "latency_status", "expected_latency_ms", "latency_upper_bound",
    "traffic_status", "expected_traffic_count", "traffic_lower_bound", "traffic_upper_bound",
    "detected_at",
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


def test_insert_body_is_the_23_status_query():
    """D-050: the table is the platform team's, so 2.4 is the 2.3 query behind one line."""
    insert = _body("2.4_service_health.sql")
    head = "INSERT INTO `service_health`\n"
    assert insert.startswith(head)
    assert _norm(insert[len(head) :]) == _norm(_body("2.3_anomaly_status.sql"))


@pytest.mark.parametrize("name", DETECTION_FILES)
def test_two_detectors_same_config_same_window(name):
    body = _norm(_body(name))
    assert body.count("ML_DETECT_ANOMALIES(") == 2
    assert body.count(_norm(DETECTOR_CONFIG)) == 2
    assert body.count(DETECTOR_OVER) == 2
    assert "ML_DETECT_ANOMALIES( avg_latency_ms, window_time," in body
    assert "ML_DETECT_ANOMALIES( CAST(traffic_count AS DOUBLE), window_time," in body


@pytest.mark.parametrize("name", PIPELINE_FILES)
def test_both_sources_read_from_earliest_offset(name):
    body = _norm(_body(name))
    assert f"`service_metrics` {EARLIEST} m" in body
    assert f"`service_deployments` {EARLIEST} FOR SYSTEM_TIME AS OF m.event_time AS d" in body


@pytest.mark.parametrize("name", [*PIPELINE_FILES, "2.2_window_metrics.sql"])
def test_window_is_fixed_at_ten_seconds(name):
    body = _body(name)
    assert "TUMBLE(" in body
    assert re.findall(r"INTERVAL '(\d+)' SECOND", body) == ["10"]


def _watermark_delay_ms(name: str) -> int:
    """How far a source table's watermark trails its event_time, from its DDL."""
    m = re.search(r"WATERMARK FOR `event_time` AS `event_time` - INTERVAL '([\d.]+)' SECOND", _body(name))
    assert m, f"no event_time watermark in {name}"
    return round(float(m.group(1)) * 1000)


def _first_tick_reaching(target_ms: int, stamp_ms: int, every: int, delay_ms: int) -> int:
    """The first tick (seconds after ops-sim's aligned start) whose record carries a source's
    watermark to `target_ms`. The source writes on every `every`-th tick, stamped `stamp_ms`
    after the tick; its watermark is the latest stamp minus `delay_ms`."""
    k = every
    while k * 1000 + stamp_ms - delay_ms < target_ms:
        k += every
    return k


def test_a_window_closes_on_the_tick_at_its_end():
    """D-064. A window closes once the temporal join's watermark, the lower of its two
    sources', reaches the window's last millisecond. service_metrics writes every second,
    stamped tick + 999 ms; service_deployments only on its heartbeat, stamped on the tick.
    A longer delay or a rarer heartbeat makes every detection wait for the next heartbeat:
    with the old 5-second watermarks, the window [0, 10 s) closed at 15 s."""
    last_ms = WINDOW_SECONDS * 1000 - 1
    deployments_delay = _watermark_delay_ms("source_service_deployments.sql")
    metrics = _first_tick_reaching(last_ms, SAMPLE_OFFSET_MS, 1, _watermark_delay_ms("source_service_metrics.sql"))
    deployments = _first_tick_reaching(last_ms, 0, REEMIT_INTERVAL, deployments_delay)
    assert max(metrics, deployments) == WINDOW_SECONDS, (
        f"window [0, {WINDOW_SECONDS} s) closes at {max(metrics, deployments)} s "
        f"(service_metrics at {metrics} s, service_deployments at {deployments} s)"
    )
    # The heartbeat stamps every service with the same tick: a watermark at the stamp itself
    # would make all but the first of them late (Flink's strictly ascending strategy).
    assert deployments_delay >= 1


def test_deployment_context_uses_last_value_not_max():
    body = _body("2.2_window_aggregation.sql")
    context = ("version", "previous_version", "last_change", "ready_replicas")
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


def test_service_health_ddl_declares_the_columns_in_order():
    """The INSERT is positional and several adjacent columns share a type, so the platform
    team's table must list the same 19 names in the same order as the query."""
    ddl = (SQL / "sink_service_health.sql").read_text()
    declared = re.findall(r"^  `(\w+)` ", ddl, re.MULTILINE)
    assert tuple(declared) == SERVICE_HEALTH_COLUMNS


def test_detected_at_is_the_instant_of_the_verdict():
    """D-054: filled on every row with the current timestamp, kept a TIMESTAMP_LTZ. The
    workspace runs statements in the browser's time zone, so a cast to TIMESTAMP(3) would
    store local wall-clock time (seen live: an hour ahead on GMT+01:00). PROCTIME() is not
    supported on Confluent Cloud."""
    text = (SQL / "sink_service_health.sql").read_text()
    ddl = _norm(text[text.index("CREATE TABLE") :])
    assert "`detected_at` TIMESTAMP_LTZ(3) NOT NULL" in ddl
    status = _norm(_body("2.3_anomaly_status.sql"))
    assert status.endswith("CURRENT_TIMESTAMP AS detected_at FROM anomaly;")
    assert "PROCTIME" not in status


def test_service_health_ddl_declares_both_status_columns_as_string():
    """A declared VARCHAR(n) would truncate; the query's CASE literals are VARCHAR(10) (D-025)."""
    text = (SQL / "sink_service_health.sql").read_text()
    ddl = _norm(text[text.index("CREATE TABLE") :])
    assert "`latency_status` STRING NOT NULL" in ddl
    assert "`traffic_status` STRING NOT NULL" in ddl
    assert "VARCHAR" not in ddl


def test_service_health_ddl_is_append_only_and_can_be_emptied():
    ddl = (SQL / "sink_service_health.sql").read_text()
    assert "CREATE TABLE IF NOT EXISTS `service_health`" in ddl
    assert "'changelog.mode' = 'append'" in ddl
    assert "'kafka.consumer.isolation-level' = 'read-uncommitted'" in ddl
    assert "'kafka.cleanup-policy' = 'delete'" in ddl


def test_regular_join_antipattern_reads_new_samples_only():
    # A regular join keeps every metrics sample in state and re-emits all of a service's
    # rows on every update of service_deployments (every 5 s re-emit); starting from the
    # latest offset keeps the demo small.
    for name in ("2.1_regular_join.sql", "2.1_regular_join_by_version.sql"):
        body = _norm(_body(name))
        assert "FOR SYSTEM_TIME AS OF" not in body
        assert "`service_metrics` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ m" in body


def test_temporal_join_solution_hints_both_sides():
    body = _norm(_body("2.1_temporal_join.sql"))
    assert f"`service_metrics` {EARLIEST} m" in body
    assert f"`service_deployments` {EARLIEST} FOR SYSTEM_TIME AS OF m.event_time AS d" in body


@pytest.mark.parametrize("name", ["2.1_regular_join_by_version.sql", "2.1_temporal_join_by_version.sql"])
def test_per_version_counts_add_up_the_traffic(name):
    # D-058: a record is a one-second sample, so a version's traffic is the SUM of its
    # samples' traffic_count, not a COUNT of rows.
    body = _norm(_body(name))
    assert "SUM(m.traffic_count) AS traffic_count" in body
    assert "COUNT(" not in body


@pytest.mark.parametrize("name", ["2.2_window_metrics.sql", "2.2_window_aggregation.sql"])
def test_windows_sum_the_traffic_and_average_the_latency(name):
    # D-058: 10 per-second samples per window; no error metrics, no MAX(latency_ms).
    body = _norm(_body(name))
    assert "SUM(traffic_count) AS traffic_count" in body
    assert "AVG(latency_ms) AS avg_latency_ms" in body
    assert "COUNT(" not in body
    assert "error" not in body
    assert "MAX(latency_ms)" not in body


def test_unread_columns_are_not_carried_through_the_pipeline():
    """D-044, D-058: nothing downstream reads them, so attendees don't have to read them
    either. The raw message fields stay in the source tables; error_count is display
    only, so it stops at lab 2.1."""
    for path in sorted((ROOT / "sql").glob("2.[2-4]_*.sql")):
        body = path.read_text()
        assert "error_count" not in body, path.name
        assert "desired_replicas" not in body, path.name
    assert "`error_count`" in (ROOT / "sql/source_service_metrics.sql").read_text()
    assert "`desired_replicas`" in (ROOT / "sql/source_service_deployments.sql").read_text()
