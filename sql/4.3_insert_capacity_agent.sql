-- 4.3 Fan-out leg 2: traffic anomalies to the capacity agent.
--
-- Routing is mutually exclusive with the release leg (see 4.3_insert_release_agent.sql):
-- this one takes every window where traffic is HIGH or LOW, whatever latency is doing.
--
-- suggested_replicas is computed here, in SQL, and handed to the agent: the arithmetic
-- stays deterministic and the model only decides whether to use it. It keeps the load
-- per replica at roughly what it was before the surge, never adds less than one replica,
-- and never exceeds the autoscaling ceiling. COALESCE covers an expected count of zero.
INSERT INTO `remediation_decisions`
SELECT
  `window_end`,
  `service_name`,
  `agent_name`,
  `trigger`,
  CASE
    WHEN `agent_status` = 'SUCCESS'
      AND `action_raw` IN ('SCALE_OUT', 'INVESTIGATE_UPSTREAM', 'ESCALATE', 'WATCH') THEN `action_raw`
    ELSE 'INVALID'
  END AS `action`,
  CAST(NULL AS STRING) AS `target_version`,
  CASE WHEN `action_raw` = 'SCALE_OUT' THEN `target_replicas_raw` END AS `target_replicas`,
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
    'capacity_agent' AS `agent_name`,
    h.`trigger`,
    a.`agent_status`,
    UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) AS `action_raw`,
    TRY_CAST(NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target Replicas:\*{0,2}\s*([^\n]+)', 1)), 'N/A') AS INT) AS `target_replicas_raw`,
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
    SELECT
      `window_end`,
      `service_name`,
      `tier`,
      `version`,
      `last_change`,
      `seconds_since_deploy`,
      `avg_latency_ms`,
      `expected_latency_ms`,
      `latency_status`,
      `request_count`,
      `expected_request_count`,
      `traffic_lower_bound`,
      `traffic_upper_bound`,
      `error_rate`,
      `error_rate_breach`,
      `ready_replicas`,
      `max_replicas`,
      CASE WHEN `traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END AS `trigger`,
      LEAST(
        `max_replicas`,
        GREATEST(
          `ready_replicas` + 1,
          COALESCE(
            CAST(CEIL(`ready_replicas` * CAST(`request_count` AS DOUBLE)
                      / NULLIF(`expected_request_count`, 0)) AS INT),
            `ready_replicas` + 1
          )
        )
      ) AS `suggested_replicas`
    FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
    WHERE `traffic_status` IN ('HIGH', 'LOW')
  ) h,
  LATERAL TABLE(AI_RUN_AGENT(
    'capacity_agent',
    CONCAT(
      'SERVICE HEALTH — ', h.`service_name`, ' | window ending ', CAST(h.`window_end` AS STRING), '\n',
      'Trigger: ', h.`trigger`, '\n',
      'Tier: ', h.`tier`, '\n',
      '\nLOAD:\n',
      '  Requests This Window: ', CAST(h.`request_count` AS STRING),
      '  Expected: ', CAST(ROUND(h.`expected_request_count`, 0) AS STRING), '\n',
      '  Expected Band: ', CAST(ROUND(h.`traffic_lower_bound`, 0) AS STRING),
      ' to ', CAST(ROUND(h.`traffic_upper_bound`, 0) AS STRING), ' requests\n',
      '\nCAPACITY:\n',
      '  Ready Replicas: ', CAST(h.`ready_replicas` AS STRING),
      '  Max Replicas: ', CAST(h.`max_replicas` AS STRING), '\n',
      '  Suggested Replicas: ', CAST(h.`suggested_replicas` AS STRING), '\n',
      '\nLATENCY AND ERRORS:\n',
      '  Average Latency: ', CAST(ROUND(h.`avg_latency_ms`, 1) AS STRING), ' ms',
      '  Expected: ', CAST(ROUND(h.`expected_latency_ms`, 1) AS STRING), ' ms',
      '  Latency Status: ', h.`latency_status`, '\n',
      '  Error Rate: ', CAST(ROUND(h.`error_rate` * 100, 2) AS STRING), ' percent',
      '  Error Rate Breach: ', CAST(h.`error_rate_breach` AS STRING), ' (true above 5 percent)\n',
      '\nRELEASE:\n',
      '  Running Version: ', h.`version`,
      '  Last Change: ', COALESCE(h.`last_change`, 'N/A'),
      '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING)
    ),
    -- request_id: correlates the call with its window in the agent logs and metrics.
    CONCAT(h.`service_name`, '@', CAST(h.`window_end` AS STRING))
  )) AS a(`agent_status`, `agent_response`)
) `parsed`;
