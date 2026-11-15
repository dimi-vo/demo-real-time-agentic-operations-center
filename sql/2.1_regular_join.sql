-- 2.1 Anti-pattern: a regular join for stream enrichment. Shown in the walkthrough, then
-- stopped. It produces an updating table and keeps every metrics sample in state forever.
-- service_metrics is read from the latest offset only, so the demo stays small.
SELECT
  m.event_time,
  m.service_name,
  m.traffic_count,
  m.latency_ms,
  m.error_count,
  d.version,
  d.last_change,
  d.deployed_at
FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ m
JOIN `service_deployments` d
  ON m.service_name = d.service_name;
