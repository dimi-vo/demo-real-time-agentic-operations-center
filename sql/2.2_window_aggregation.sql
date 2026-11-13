-- 2.2 Part 2: join first, then window. The metrics from part 1 plus the deployment
-- context as of the end of each window. LAST_VALUE, not MAX: MAX on version strings is
-- lexicographic and picks the wrong version in a window that contains a deploy.
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
)
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
GROUP BY window_start, window_end, window_time, service_name;
