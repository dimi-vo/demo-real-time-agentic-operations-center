-- Routing: each decision comes from the agent its leg is meant to call.
--
-- Expect zero rows. There are two ways to get one:
--
-- 1. `a window reached two agents`: a service window produced both a release and a
--    capacity decision, so the WHERE clauses in sql/4.3_insert_*.sql have drifted apart.
--    (The release agent's error-rate leg reads service_error_rate, where only web-gateway
--    has rows, and web-gateway never reaches the capacity agent, D-067.) Counting agents
--    per window is the whole check; a window with one agent is correct however many times
--    it was retried.
-- 2. `ticket routing`: a decision breaks the ticket leg's rules (D-068). HIGH_SEVERITY_TICKET
--    comes only from ticket_responder (sql/5.3_insert_ticket_responder.sql), always with a
--    ticket_id and never with a target; the metric agents never write a ticket_id, and the
--    responder never writes another trigger.
--
-- Ticket decisions are left out of the first part: their window_end is the ticket's
-- event_time, and two tickets created on the same tick share it.
SELECT
  'a window reached two agents' AS problem,
  service_name,
  window_end,
  CAST(NULL AS STRING) AS agent_name,
  CAST(NULL AS STRING) AS ticket_id,
  COUNT(*) AS decisions
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
WHERE ticket_id IS NULL
GROUP BY service_name, window_end
HAVING COUNT(DISTINCT agent_name) > 1
UNION ALL
SELECT
  'ticket routing' AS problem,
  service_name,
  window_end,
  agent_name,
  ticket_id,
  CAST(1 AS BIGINT) AS decisions
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
WHERE (`trigger` = 'HIGH_SEVERITY_TICKET'
       AND (agent_name <> 'ticket_responder' OR ticket_id IS NULL OR target IS NOT NULL))
   OR (`trigger` <> 'HIGH_SEVERITY_TICKET'
       AND (agent_name = 'ticket_responder' OR ticket_id IS NOT NULL));
