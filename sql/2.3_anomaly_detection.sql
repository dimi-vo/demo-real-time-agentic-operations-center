-- 2.3 Part 1: two ML_DETECT_ANOMALIES detectors in one statement, latency and traffic,
-- one ARIMA model per service per metric (PARTITION BY service_name). Both sources are
-- read from the earliest offset, so the detectors train on history immediately.
-- The result fields are NULL until a service has minTrainingSize (12) windows.
-- p=1, d=0, q=0 (AR(1) around the mean): see D-029 for why not auto-ARIMA.
WITH enriched AS (
  SELECT
    r.event_time,
    r.service_name,
    r.status_code,
    r.latency_ms,
    r.response_bytes,
    d.version,
    d.previous_version,
    d.deployed_at,
    d.last_change,
    d.desired_replicas,
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
    AVG(CAST(response_bytes AS DOUBLE)) AS avg_response_bytes,
    LAST_VALUE(version) AS version,
    LAST_VALUE(previous_version) AS previous_version,
    LAST_VALUE(last_change) AS last_change,
    TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy,
    LAST_VALUE(ready_replicas) AS ready_replicas,
    LAST_VALUE(desired_replicas) AS desired_replicas,
    LAST_VALUE(tier) AS tier
  FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end, window_time, service_name
)
SELECT
  service_name,
  window_start,
  window_end,
  avg_latency_ms,
  request_count,
  ML_DETECT_ANOMALIES(
    avg_latency_ms, window_time,
    JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.9, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
  ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latency_result,
  ML_DETECT_ANOMALIES(
    CAST(request_count AS DOUBLE), window_time,
    JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.9, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
  ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS traffic_result
FROM windowed;
