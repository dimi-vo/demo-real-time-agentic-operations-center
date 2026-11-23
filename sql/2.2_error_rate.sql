-- 2.2 Part 1: the simplest detector, a fixed threshold on one metric. web-gateway's bad
-- deploys fail requests and change nothing else (D-067), so its error rate over each
-- 10-second window is the whole signal: HIGH when the window's errors are 25 % or more of
-- its requests, otherwise NORMAL.
--
-- A ratio of sums: the window's errors over the window's requests, so every request counts
-- once. A window with no requests is never HIGH (its error_rate is null). Every window gets
-- a row and a status, not only the bad ones, so a normal 0.5 % sits next to a breach.
--
-- The deployment context comes from the 2.1 temporal join, as of the end of each window
-- (LAST_VALUE, not MAX: MAX on version strings is lexicographic). A flagged window names the
-- version that went live and the one before it, which is what a rollback goes back to.
-- Both sources are read from the earliest offset, so history is attributed too.
WITH enriched AS (
  SELECT
    m.event_time,
    m.service_name,
    m.traffic_count,
    m.error_count,
    d.version,
    d.previous_version,
    d.deployed_at,
    d.last_change
  FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ m
  JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
    FOR SYSTEM_TIME AS OF m.event_time AS d
    ON m.service_name = d.service_name
  WHERE m.service_name = 'web-gateway'
),
windowed AS (
  SELECT
    window_start,
    window_end,
    service_name,
    SUM(traffic_count) AS traffic_count,
    SUM(error_count) AS error_count,
    LAST_VALUE(version) AS version,
    LAST_VALUE(previous_version) AS previous_version,
    LAST_VALUE(last_change) AS last_change,
    TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy
  FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end, service_name
)
SELECT
  service_name,
  window_start,
  window_end,
  traffic_count,
  error_count,
  CAST(error_count AS DOUBLE) / NULLIF(traffic_count, 0) AS error_rate,
  CASE
    WHEN traffic_count > 0 AND error_count >= 0.25 * traffic_count THEN 'HIGH'
    ELSE 'NORMAL'
  END AS error_status,
  version,
  previous_version,
  last_change,
  seconds_since_deploy,
  -- When this row's verdict was made: an instant (TIMESTAMP_LTZ), evaluated per row, as in
  -- service_health. Its gap to window_end is the real-time delay: the window has to close.
  CURRENT_TIMESTAMP AS detected_at
FROM windowed;
