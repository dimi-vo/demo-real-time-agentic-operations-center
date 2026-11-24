-- 2.4 Part 1: two ML_DETECT_ANOMALIES detectors in one statement, latency and traffic,
-- one ARIMA model per service per metric (PARTITION BY service_name), on payments
-- (web-gateway is lab 2.2's threshold, D-067; D-068). With one service the partition holds
-- one series per metric, but it is the general pattern: add a service to the WHERE and it
-- gets its own models. Both sources are read from the earliest offset, so the detectors
-- train on history immediately.
-- The result fields are NULL until a service has minTrainingSize (12) windows.
-- p=1, d=0, q=0 (AR(1) around the mean): see D-029 for why not auto-ARIMA, and D-059 for
-- the confidence (99.95) and the rest of the tuning.
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
)
SELECT
  service_name,
  window_start,
  window_end,
  avg_latency_ms,
  traffic_count,
  ML_DETECT_ANOMALIES(
    avg_latency_ms, window_time,
    JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
  ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latency_result,
  ML_DETECT_ANOMALIES(
    CAST(traffic_count AS DOUBLE), window_time,
    JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
  ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS traffic_result
FROM windowed;
