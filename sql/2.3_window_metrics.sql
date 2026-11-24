-- 2.3 Part 1: traffic and latency per service per 10-second window, for payments, the
-- service ARIMA covers (web-gateway is lab 2.2's threshold, D-067; D-068).
-- Each window holds ten metrics samples (one per second): their traffic adds up, their
-- latency averages.
SELECT
  window_start,
  window_end,
  window_time,
  service_name,
  SUM(traffic_count) AS traffic_count,
  AVG(latency_ms) AS avg_latency_ms
FROM TABLE(TUMBLE(TABLE `service_metrics`, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
WHERE service_name = 'payments'
GROUP BY window_start, window_end, window_time, service_name;
