-- 2.1 Solution: an event-time temporal join. Each request is enriched with the
-- deployment of its service as of the request's event_time ("which version served it?").
-- Both sides are read from the earliest offset, so history is attributed correctly too.
SELECT
  r.event_time,
  r.service_name,
  r.instance_id,
  r.route,
  r.status_code,
  r.latency_ms,
  d.version,
  d.last_change,
  d.deployed_at
FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ r
JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
  FOR SYSTEM_TIME AS OF r.event_time AS d
  ON r.service_name = d.service_name;
