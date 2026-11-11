-- 2.1 Verify: the same per-version counts through the temporal join. When ops-sim
-- deploys a service, the old version's row stops growing (its last_request is just
-- before the deploy) and a new row starts from the deploy time.
SELECT
  r.service_name,
  d.version,
  COUNT(*) AS requests,
  MIN(r.event_time) AS first_request,
  MAX(r.event_time) AS last_request
FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ r
JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
  FOR SYSTEM_TIME AS OF r.event_time AS d
  ON r.service_name = d.service_name
GROUP BY r.service_name, d.version;
