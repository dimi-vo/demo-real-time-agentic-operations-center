-- 2.5 The service_health INSERT: the 2.4 part 2 query, written continuously to the table.
-- `service_health` already exists: the platform team's Terraform created it with its 19
-- columns (sql/sink_service_health.sql), so this one statement is all you write. One row
-- per service (payments, D-068) per 10-second window, in the 2.4 column order (the INSERT is
-- positional).
-- Reads both sources from the earliest offset, so it replays the history ops-sim has built
-- (up to 24 hours) and trains on it: no reset needed.
-- Running it twice writes every window twice. To change the query and start clean, run
-- `uv run reset-health`, then Run the new INSERT.
INSERT INTO `service_health`
WITH enriched AS (
  SELECT
    m.event_time,
    m.service_name,
    m.traffic_count,
    m.latency_ms,
    d.version,
    d.previous_version,
    d.deployed_at,
    d.last_change,
    d.ready_replicas,
    d.tier
  FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ m
  JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
    FOR SYSTEM_TIME AS OF m.event_time AS d
    ON m.service_name = d.service_name
  WHERE m.service_name = 'payments'
),
windowed AS (
  SELECT
    window_start,
    window_end,
    window_time,
    service_name,
    SUM(traffic_count) AS traffic_count,
    AVG(latency_ms) AS avg_latency_ms,
    LAST_VALUE(version) AS version,
    LAST_VALUE(previous_version) AS previous_version,
    LAST_VALUE(last_change) AS last_change,
    TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy,
    LAST_VALUE(ready_replicas) AS ready_replicas,
    LAST_VALUE(tier) AS tier
  FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end, window_time, service_name
),
anomaly AS (
  SELECT
    *,
    ML_DETECT_ANOMALIES(
      avg_latency_ms, window_time,
      JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
    ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latency_result,
    ML_DETECT_ANOMALIES(
      CAST(traffic_count AS DOUBLE), window_time,
      JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
    ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS traffic_result
  FROM windowed
)
SELECT
  service_name,
  window_start,
  window_end,
  traffic_count,
  avg_latency_ms,
  version,
  previous_version,
  last_change,
  seconds_since_deploy,
  ready_replicas,
  tier,
  CASE
    WHEN latency_result.is_anomaly IS NULL THEN 'WARMING_UP'
    WHEN latency_result.is_anomaly AND latency_result.actual_value > latency_result.upper_bound THEN 'HIGH'
    ELSE 'NORMAL'
  END AS latency_status,
  latency_result.forecast_value AS expected_latency_ms,
  latency_result.upper_bound AS latency_upper_bound,
  CASE
    WHEN traffic_result.is_anomaly IS NULL THEN 'WARMING_UP'
    WHEN traffic_result.is_anomaly AND traffic_result.actual_value > traffic_result.upper_bound THEN 'HIGH'
    WHEN traffic_result.is_anomaly AND traffic_result.actual_value < traffic_result.lower_bound THEN 'LOW'
    ELSE 'NORMAL'
  END AS traffic_status,
  traffic_result.forecast_value AS expected_traffic_count,
  traffic_result.lower_bound AS traffic_lower_bound,
  traffic_result.upper_bound AS traffic_upper_bound,
  -- When this row's verdict was made: an instant (TIMESTAMP_LTZ), evaluated per row. The
  -- workspace shows it in your time zone, while the window columns are UTC; put
  -- SET 'sql.local-time-zone' = 'UTC'; in front of the query to see both in UTC. Its gap to
  -- window_end is the real-time delay: the window has to close and the watermark pass.
  CURRENT_TIMESTAMP AS detected_at
FROM anomaly;
