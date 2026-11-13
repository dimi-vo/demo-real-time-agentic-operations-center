-- 2.2 Part 1: request, error and latency metrics per service per 10-second window.
SELECT
  window_start,
  window_end,
  window_time,
  service_name,
  COUNT(*) AS request_count,
  SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS error_count,
  CAST(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS error_rate,
  AVG(latency_ms) AS avg_latency_ms,
  MAX(latency_ms) AS max_latency_ms
FROM TABLE(TUMBLE(TABLE `service_requests`, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, window_time, service_name;
