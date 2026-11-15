-- 2.2 Part 2: join first, then window. The metrics from part 1 plus the deployment
-- context as of the end of each window. LAST_VALUE, not MAX: MAX on version strings is
-- lexicographic and picks the wrong version in a window that contains a deploy.
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
)
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
GROUP BY window_start, window_end, window_time, service_name;
