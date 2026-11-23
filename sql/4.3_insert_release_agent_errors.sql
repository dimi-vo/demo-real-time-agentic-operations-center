-- 4.3 Fan-out leg 1: error-rate breaches to the release agent (D-067). Run it first, in the
-- order of the detection labs: lab 2.2's threshold before the ARIMA labs.
--
-- It reads service_error_rate, lab 2.2's table, where only web-gateway has rows, and
-- web-gateway has no rows in service_health. So no window reaches this leg and another one.
-- The release agent answers it by the same rule as a latency anomaly: a recent DEPLOY with a
-- previous version is rolled back.
--
-- Same shape as 4.3_insert_release_agent.sql, which explains the rest: the filter sits below
-- the LATERAL join (one agent call per flagged row), the table is read from the latest
-- offset (starting the statement replays no history through the model), and a failed or
-- out-of-set answer becomes INVALID.
INSERT INTO `remediation_decisions`
SELECT
  h.`window_end`,
  h.`service_name`,
  'release_agent',
  'ERROR_RATE_HIGH',
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('ROLLBACK', 'OBSERVE')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`
FROM (
  SELECT *
  FROM `service_error_rate` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
  WHERE `error_status` = 'HIGH'
) h,
LATERAL TABLE(AI_RUN_AGENT(
  'release_agent',
  CONCAT(
    'SERVICE ', h.`service_name`, '\n',
    'Trigger: ERROR_RATE_HIGH\n',
    'Error Rate: ', CAST(ROUND(h.`error_rate` * 100, 1) AS STRING), ' %',
    '  Threshold: 25 %\n',
    'Errors: ', CAST(h.`error_count` AS STRING), ' of ', CAST(h.`traffic_count` AS STRING), ' requests\n',
    'Last Change: ', COALESCE(h.`last_change`, 'N/A'),
    '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING), '\n',
    'Previous Version: ', COALESCE(h.`previous_version`, 'N/A')
  )
)) AS a(`agent_status`, `agent_response`);
