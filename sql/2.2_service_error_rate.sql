-- 2.2 Part 2: the service_error_rate INSERT, the part 1 query written continuously to the
-- table. `service_error_rate` already exists: the platform team's Terraform created it with
-- its 12 columns (sql/sink_service_error_rate.sql), so this one statement is all you write.
-- One row per web-gateway 10-second window, in the part 1 column order (the INSERT is
-- positional). The release agent reads the HIGH rows (lab 4.3).
-- Reads both sources from the earliest offset, so it replays the history ops-sim has built
-- (up to 24 hours): there is nothing to train, so its verdicts are right from the first row.
-- Running it twice writes every window twice. To change the query and start clean, run
-- `uv run reset-health`, then Run the new INSERT.
INSERT INTO `service_error_rate`
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
