-- 4.3 Fan-out leg 2: traffic anomalies to the capacity agent.
--
-- Routing is mutually exclusive with the release leg (see 4.3_insert_release_agent.sql):
-- this one takes every window where traffic is HIGH or LOW, whatever latency is doing.
--
-- Suggested Replicas is computed here, in SQL, and handed to the agent: the arithmetic
-- stays deterministic and the model only decides whether to use it. It keeps the load
-- per replica at roughly what it was before the surge and never adds less than one
-- replica. There is no autoscaling ceiling (D-043). COALESCE covers an expected count of
-- zero.
INSERT INTO `remediation_decisions`
SELECT
  h.`window_end`,
  h.`service_name`,
  'capacity_agent',
  CASE WHEN h.`traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END,
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('SCALE_OUT', 'INVESTIGATE_UPSTREAM', 'OBSERVE')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`
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
    ' to ', CAST(ROUND(h.`traffic_upper_bound`, 0) AS STRING), '\n',
    'Ready Replicas: ', CAST(h.`ready_replicas` AS STRING),
    '  Suggested Replicas: ', CAST(GREATEST(
      h.`ready_replicas` + 1,
      COALESCE(
        CAST(CEIL(h.`ready_replicas` * CAST(h.`traffic_count` AS DOUBLE)
                  / NULLIF(h.`expected_traffic_count`, 0)) AS INT),
        h.`ready_replicas` + 1
      )
    ) AS STRING)
  )
)) AS a(`agent_status`, `agent_response`);
