-- Developer check: every anomalous service_health row, with actual versus expected.
SELECT
  service_name,
  window_end,
  latency_status,
  ROUND(avg_latency_ms, 1) AS avg_latency_ms,
  ROUND(expected_latency_ms, 1) AS expected_latency_ms,
  ROUND(latency_upper_bound, 1) AS latency_upper_bound,
  traffic_status,
  request_count,
  ROUND(expected_request_count, 1) AS expected_request_count,
  ROUND(traffic_lower_bound, 1) AS traffic_lower_bound,
  ROUND(traffic_upper_bound, 1) AS traffic_upper_bound,
  ROUND(error_rate, 3) AS error_rate,
  version,
  last_change,
  seconds_since_deploy
FROM `service_health`
WHERE latency_status = 'HIGH' OR traffic_status IN ('HIGH', 'LOW');
