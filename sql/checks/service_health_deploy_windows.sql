-- Developer check: windows that contain a deploy or rollback (it happened less than one
-- window before window_end). version must be the NEW version and previous_version the
-- one it replaced, matching the ops-sim console line for that deploy.
SELECT
  service_name,
  window_start,
  window_end,
  version,
  previous_version,
  last_change,
  seconds_since_deploy,
  traffic_count,
  avg_latency_ms
FROM `service_health`
WHERE seconds_since_deploy <= 10;
