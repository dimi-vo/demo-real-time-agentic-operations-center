-- Developer check: every anomalous service_health row, with actual versus expected, and how
-- long after its window closed the verdict was made (detected_at, D-054).
-- The SET runs this check in UTC, the zone of the window columns: the workspace would
-- otherwise show detected_at (a TIMESTAMP_LTZ) in your browser's zone, and the CAST would
-- turn it into that zone's wall-clock time. Submit both lines together.
SET 'sql.local-time-zone' = 'UTC';
SELECT
  service_name,
  window_end,
  detected_at,
  TIMESTAMPDIFF(SECOND, window_end, CAST(detected_at AS TIMESTAMP(3))) AS detection_delay_s,
  latency_status,
  ROUND(avg_latency_ms, 1) AS avg_latency_ms,
  ROUND(expected_latency_ms, 1) AS expected_latency_ms,
  ROUND(latency_upper_bound, 1) AS latency_upper_bound,
  traffic_status,
  traffic_count,
  ROUND(expected_traffic_count, 1) AS expected_traffic_count,
  ROUND(traffic_lower_bound, 1) AS traffic_lower_bound,
  ROUND(traffic_upper_bound, 1) AS traffic_upper_bound,
  version,
  last_change,
  seconds_since_deploy
FROM `service_health`
WHERE latency_status = 'HIGH' OR traffic_status IN ('HIGH', 'LOW');
