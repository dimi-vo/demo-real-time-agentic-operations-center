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
-- Parsing is done once in the `parsed` derived table; the outer SELECT only validates.
-- An agent that failed (status other than SUCCESS) or answered outside its allowed set
-- yields action = 'INVALID' (D-007); raw_response always keeps the full answer.
-- REGEXP_EXTRACT tolerates optional markdown bold (**Action:**) that models sometimes
-- emit despite the instruction, and TRY_CAST keeps a non-numeric replica count from
-- failing the whole statement.
INSERT INTO `remediation_decisions`
SELECT
  `window_end`,
  `service_name`,
  `agent_name`,
  `trigger`,
  CASE
    WHEN `agent_status` = 'SUCCESS' AND `action_raw` IN ('ROLLBACK', 'WATCH', 'ESCALATE') THEN `action_raw`
    ELSE 'INVALID'
  END AS `action`,
  CASE WHEN `action_raw` = 'ROLLBACK' THEN `target_version_raw` END AS `target_version`,
  CAST(NULL AS INT) AS `target_replicas`,
  CASE WHEN `confidence_raw` IN ('HIGH', 'MEDIUM', 'LOW') THEN `confidence_raw` END AS `confidence`,
  `summary`,
  `reasoning`,
  `raw_response`,
  `version`,
  `seconds_since_deploy`,
  `avg_latency_ms`,
  `expected_latency_ms`,
  `request_count`,
  `expected_request_count`,
  `error_rate`,
  `ready_replicas`,
  `max_replicas`
FROM (
  SELECT
    h.`window_end`,
    h.`service_name`,
    'release_agent' AS `agent_name`,
    'LATENCY_HIGH' AS `trigger`,
    a.`agent_status`,
    UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) AS `action_raw`,
    NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target Version:\*{0,2}\s*([^\n]+)', 1)), 'N/A') AS `target_version_raw`,
    UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Confidence:\*{0,2}\s*([^\n]+)', 1))) AS `confidence_raw`,
    TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Summary:\*{0,2}\s*([^\n]+)', 1)) AS `summary`,
    TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reasoning:\*{0,2}\s*([\s\S]+?)$', 1)) AS `reasoning`,
    a.`agent_response` AS `raw_response`,
    h.`version`,
    h.`seconds_since_deploy`,
    h.`avg_latency_ms`,
    h.`expected_latency_ms`,
    h.`request_count`,
    h.`expected_request_count`,
    h.`error_rate`,
    h.`ready_replicas`,
    h.`max_replicas`
  FROM (
    SELECT *
    FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
    WHERE `latency_status` = 'HIGH' AND `traffic_status` NOT IN ('HIGH', 'LOW')
  ) h,
  LATERAL TABLE(AI_RUN_AGENT(
    'release_agent',
    CONCAT(
      'SERVICE HEALTH — ', h.`service_name`, ' | window ending ', CAST(h.`window_end` AS STRING), '\n',
      'Trigger: LATENCY_HIGH\n',
      'Tier: ', h.`tier`, '\n',
      '\nRELEASE:\n',
      '  Running Version: ', h.`version`, '\n',
      '  Previous Version: ', COALESCE(h.`previous_version`, 'N/A'), '\n',
      '  Last Change: ', COALESCE(h.`last_change`, 'N/A'), '\n',
      '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING), '\n',
      '\nLATENCY:\n',
      '  Average: ', CAST(ROUND(h.`avg_latency_ms`, 1) AS STRING), ' ms',
      '  Expected: ', CAST(ROUND(h.`expected_latency_ms`, 1) AS STRING), ' ms',
      '  Expected Upper Bound: ', CAST(ROUND(h.`latency_upper_bound`, 1) AS STRING), ' ms\n',
      '  Worst Request: ', CAST(ROUND(h.`max_latency_ms`, 1) AS STRING), ' ms\n',
      '\nERRORS AND LOAD:\n',
      '  Error Rate: ', CAST(ROUND(h.`error_rate` * 100, 2) AS STRING), ' percent',
      '  Error Rate Breach: ', CAST(h.`error_rate_breach` AS STRING), ' (true above 5 percent)\n',
      '  Requests This Window: ', CAST(h.`request_count` AS STRING),
      '  Expected: ', CAST(ROUND(h.`expected_request_count`, 0) AS STRING),
      '  Traffic Status: ', h.`traffic_status`, '\n',
      '  Replicas: ', CAST(h.`ready_replicas` AS STRING), ' ready of ', CAST(h.`max_replicas` AS STRING), ' max'
    ),
    -- request_id: correlates the call with its window in the agent logs and metrics.
    CONCAT(h.`service_name`, '@', CAST(h.`window_end` AS STRING))
  )) AS a(`agent_status`, `agent_response`)
) `parsed`;
