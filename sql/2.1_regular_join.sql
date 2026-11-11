-- 2.1 Anti-pattern: a regular join for stream enrichment. Shown in the walkthrough, then
-- stopped. It produces an updating table and keeps every request in state forever.
-- service_requests is read from the latest offset only, so the demo stays small.
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
FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ r
JOIN `service_deployments` d
  ON r.service_name = d.service_name;
