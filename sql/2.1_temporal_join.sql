-- 2.1 Solution: an event-time temporal join. Each metrics sample is enriched with the
-- deployment of its service as of the sample's event_time ("which version served that
-- second?"). Both sides are read from the earliest offset, so history is attributed
-- correctly too.
SELECT
  m.event_time,
  m.service_name,
  m.traffic_count,
  m.latency_ms,
  m.error_count,
  d.version,
  d.last_change,
  d.deployed_at
FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ m
JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
  FOR SYSTEM_TIME AS OF m.event_time AS d
  ON m.service_name = d.service_name;
