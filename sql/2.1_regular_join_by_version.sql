-- 2.1 Anti-pattern, made visible: request counts per running version through a regular
-- join. When ops-sim deploys a service, that service's old-version row disappears and
-- the new version takes over every request counted so far, including the ones the old
-- version served (first_request is older than the deploy).
SELECT
  r.service_name,
  d.version,
  COUNT(*) AS requests,
  MIN(r.event_time) AS first_request,
  MAX(r.event_time) AS last_request
FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ r
JOIN `service_deployments` d
  ON r.service_name = d.service_name
GROUP BY r.service_name, d.version;
