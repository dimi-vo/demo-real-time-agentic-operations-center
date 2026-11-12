-- 4.3 Fan-out leg 1: latency anomalies to the release agent.
--
-- Routing is mutually exclusive with the capacity leg: a window where traffic is also
-- anomalous is a load problem, not a release problem, so it goes to the capacity agent
-- only. No window ever triggers both agents.
--
-- The filter sits in a derived table *below* the LATERAL join on purpose: the agent is
-- called once per anomalous row, not once per window. service_health is read from the
-- latest offset, so starting this statement does not replay history through the model.
--
-- An agent that failed (status other than SUCCESS) or answered outside its allowed set
-- yields action = 'INVALID' (D-007); raw_response always keeps the full answer.
-- REGEXP_EXTRACT tolerates optional markdown bold (**Action:**) that models sometimes
-- emit despite the instruction.
INSERT INTO `remediation_decisions`
SELECT
  h.`window_end`,
  h.`service_name`,
  'release_agent',
  'LATENCY_HIGH',
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('ROLLBACK', 'ESCALATE', 'OBSERVE')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`
FROM (
  SELECT *
  FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
  WHERE `latency_status` = 'HIGH' AND `traffic_status` NOT IN ('HIGH', 'LOW')
) h,
LATERAL TABLE(AI_RUN_AGENT(
  'release_agent',
  CONCAT(
    'SERVICE ', h.`service_name`, ' (', h.`tier`, ')\n',
    'Trigger: LATENCY_HIGH\n',
    'Average Latency: ', CAST(ROUND(h.`avg_latency_ms`, 1) AS STRING), ' ms',
    '  Expected: ', CAST(ROUND(h.`expected_latency_ms`, 1) AS STRING), ' ms',
    '  Expected Upper Bound: ', CAST(ROUND(h.`latency_upper_bound`, 1) AS STRING), ' ms\n',
    'Last Change: ', COALESCE(h.`last_change`, 'N/A'),
    '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING), '\n',
    'Previous Version: ', COALESCE(h.`previous_version`, 'N/A'), '\n',
    'Error Rate Breach: ', CAST(h.`error_rate_breach` AS STRING), ' (true above 5 percent)'
  )
)) AS a(`agent_status`, `agent_response`);
