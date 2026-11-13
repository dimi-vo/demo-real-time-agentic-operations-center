-- 2.3 Part 2: flatten the detector results into status and expected-value columns.
-- latency_status: WARMING_UP | HIGH | NORMAL (latency drops are not actionable).
-- traffic_status: WARMING_UP | HIGH | LOW | NORMAL.
-- error_rate is not modeled with ARIMA (near-zero, low-variance); it gets a threshold.
WITH enriched AS (
  SELECT
    r.event_time,
    r.service_name,
    r.status_code,
    r.latency_ms,
    d.version,
    d.previous_version,
    d.deployed_at,
    d.last_change,
    d.ready_replicas,
    d.tier
  FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ r
  JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
    FOR SYSTEM_TIME AS OF r.event_time AS d
    ON r.service_name = d.service_name
),
windowed AS (
  SELECT
    window_start,
    window_end,
    window_time,
    service_name,
    COUNT(*) AS request_count,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS error_count,
    CAST(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS error_rate,
    AVG(latency_ms) AS avg_latency_ms,
    MAX(latency_ms) AS max_latency_ms,
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
      JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.9, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
    ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latency_result,
    ML_DETECT_ANOMALIES(
      CAST(request_count AS DOUBLE), window_time,
      JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.9, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
    ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS traffic_result
  FROM windowed
)
SELECT
  service_name,
  window_start,
  window_end,
  request_count,
  error_count,
  error_rate,
  avg_latency_ms,
  max_latency_ms,
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
  traffic_result.forecast_value AS expected_request_count,
  traffic_result.lower_bound AS traffic_lower_bound,
  traffic_result.upper_bound AS traffic_upper_bound,
  error_rate > 0.05 AS error_rate_breach
FROM anomaly;
