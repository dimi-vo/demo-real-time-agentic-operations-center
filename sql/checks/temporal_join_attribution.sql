-- Developer check: is every request attributed to the version that served it?
-- ops-sim names instances {service}-{sha1(service:version)[0:6]}-{suffix} from its own
-- state, so the hash inside instance_id is ground truth. Recompute it from the version
-- the temporal join picked and count disagreements per service per 10 minutes.
-- Expect mismatches = 0 and unmatched = 0 in every row.
-- A 10-minute window keeps the output append-only and tiny (6 rows per 10 minutes), so
-- it runs at full speed; an unwindowed GROUP BY would push an update per request.
WITH attributed AS (
  SELECT
    r.event_time,
    r.service_name,
    CASE WHEN d.version IS NULL THEN 1 ELSE 0 END AS unmatched,
    CASE
      WHEN d.version IS NOT NULL
       AND REGEXP_EXTRACT(r.instance_id, '-([0-9a-f]{6})-[0-9a-z]{5}$', 1)
           <> SUBSTR(SHA1(CONCAT(r.service_name, ':', d.version)), 1, 6)
      THEN 1 ELSE 0
    END AS mismatch
  FROM `service_requests` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ r
  LEFT JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
    FOR SYSTEM_TIME AS OF r.event_time AS d
    ON r.service_name = d.service_name
)
SELECT
  window_start,
  service_name,
  COUNT(*) AS requests,
  SUM(unmatched) AS unmatched,
  SUM(mismatch) AS mismatches
FROM TABLE(TUMBLE(TABLE attributed, DESCRIPTOR(event_time), INTERVAL '10' MINUTE))
GROUP BY window_start, window_end, service_name;
