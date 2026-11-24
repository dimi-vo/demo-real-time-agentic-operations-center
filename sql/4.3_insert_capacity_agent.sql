-- 4.3 Fan-out leg 3: traffic anomalies to the capacity agent.
--
-- Routing is mutually exclusive with the release leg (see 4.3_insert_release_agent.sql):
-- this one takes every window where traffic is HIGH or LOW, whatever latency is doing.
--
-- The agent answers INVESTIGATE_UPSTREAM for a drop and OBSERVE for more traffic than
-- forecast (D-068), so it gets the count, the forecast and the band, and nothing to compute.
-- Its Target line is always N/A, which NULLIF turns into a null target. The last column,
-- ticket_id, is null, as in the release legs.
INSERT INTO `remediation_decisions`
SELECT
  h.`window_end`,
  h.`service_name`,
  'capacity_agent',
  CASE WHEN h.`traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END,
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('INVESTIGATE_UPSTREAM', 'OBSERVE')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`,
  CAST(NULL AS STRING)
FROM (
  SELECT *
  FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
  WHERE `traffic_status` IN ('HIGH', 'LOW')
) h,
LATERAL TABLE(AI_RUN_AGENT(
  'capacity_agent',
  CONCAT(
    'SERVICE ', h.`service_name`, ' (', h.`tier`, ')\n',
    'Trigger: ', CASE WHEN h.`traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END, '\n',
    'Traffic Count: ', CAST(h.`traffic_count` AS STRING),
    '  Expected Traffic Count: ', CAST(ROUND(h.`expected_traffic_count`, 0) AS STRING),
    '  Expected Band: ', CAST(ROUND(h.`traffic_lower_bound`, 0) AS STRING),
    ' to ', CAST(ROUND(h.`traffic_upper_bound`, 0) AS STRING)
  )
)) AS a(`agent_status`, `agent_response`);
