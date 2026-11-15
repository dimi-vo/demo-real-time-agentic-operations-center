-- 2.1 Verify: the same per-version traffic through the temporal join. When ops-sim
-- deploys a service, the old version's row stops growing (its last_sample is just
-- before the deploy) and a new row starts from the deploy time.
SELECT
  m.service_name,
  d.version,
  SUM(m.traffic_count) AS traffic_count,
  MIN(m.event_time) AS first_sample,
  MAX(m.event_time) AS last_sample
FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='latest-offset') */ m
JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
  FOR SYSTEM_TIME AS OF m.event_time AS d
  ON m.service_name = d.service_name
GROUP BY m.service_name, d.version;
