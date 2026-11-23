-- 2.1 Anti-pattern, made visible: traffic per running version through a regular join.
-- When ops-sim deploys a service, that service's old-version row disappears and the new
-- version takes over all the traffic counted so far, including the seconds the old version
-- served (first_sample is older than the deploy).
SELECT
  m.service_name,
  d.version,
  SUM(m.traffic_count) AS traffic_count,
  MIN(m.event_time) AS first_sample,
  MAX(m.event_time) AS last_sample
FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ m
JOIN `service_deployments` d
  ON m.service_name = d.service_name
GROUP BY m.service_name, d.version;
