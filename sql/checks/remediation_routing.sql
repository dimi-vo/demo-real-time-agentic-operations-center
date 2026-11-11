-- Routing is mutually exclusive: no service window reached both agents.
--
-- Expect zero rows. One row means a window produced both a release and a capacity
-- decision, so the two WHERE clauses in sql/4.3_insert_*.sql have drifted apart.
-- Counting agents per window is the whole check; a window with one agent is correct
-- however many times it was retried.
SELECT
  service_name,
  window_end,
  COUNT(DISTINCT agent_name) AS agents,
  COUNT(*) AS decisions
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
GROUP BY service_name, window_end
HAVING COUNT(DISTINCT agent_name) > 1;
