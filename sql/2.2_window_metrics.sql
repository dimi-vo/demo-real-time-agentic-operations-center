-- 2.2 Part 1: traffic and latency per service per 10-second window. Each window holds
-- ten metrics samples (one per second): their traffic adds up, their latency averages.
SELECT
  window_start,
  window_end,
  window_time,
  service_name,
  SUM(traffic_count) AS traffic_count,
  AVG(latency_ms) AS avg_latency_ms
FROM TABLE(TUMBLE(TABLE `service_metrics`, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, window_time, service_name;
