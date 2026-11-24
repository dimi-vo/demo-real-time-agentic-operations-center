-- 5.3 Every HIGH verdict to the ticket responder, and its recommendation into
-- remediation_decisions.
--
-- The filter on HIGH lives here, in a derived table below the LATERAL join, the way
-- 4.3_insert_release_agent_errors.sql reads error_status = 'HIGH'. Lab 5.2 writes every
-- verdict to ticket_triage, LOW and INVALID ones included, and there is no high-severity
-- topic. So the responder is called once per HIGH ticket, about 0.6 a minute, and a customer
-- shouting about a discount code stops at the classifier.
--
-- It reads ticket_triage from the latest offset, so starting it, or starting it again after a
-- stop, never sends old tickets to the model again. The HIGH verdicts written before it
-- started get no decision.
--
-- remediation_decisions is the same fan-in table the three 4.3 legs write, so ops-sim, the ops
-- wall and `uv run evaluate` read one table of decisions. A ticket decision is the one with a
-- ticket_id (the 4.3 legs write it as null): its window_end holds the ticket's event_time, its
-- agent is ticket_responder, its trigger HIGH_SEVERITY_TICKET, and its target is always null,
-- because there is never a version or a replica count to give.
--
-- The simulator takes no action on a ticket decision. A correct metric decision resolves its
-- incident (D-065), but a ticket is about checkout, which never has an incident, so nothing
-- the responder answers changes the simulation.
--
-- The answer is parsed as in the 4.3 legs (see 4.3_insert_release_agent.sql): action is the
-- Action line when the call succeeded and that line is one of the three actions, and INVALID
-- otherwise, so it is never null. raw_response always keeps the full answer.
-- The input ends with the classifier's reason, so the responder starts from why the ticket
-- was called HIGH. Its label, "Classifier said:", holds no "Reason:": the Reason regex takes
-- the first match, so if the model echoed its input, a label with "Reason:" in it would be
-- parsed instead of its own Reason line. That reason is null when the classifier's answer had
-- no Reason line, and CONCAT returns null when any argument is null, so it falls back to N/A,
-- as the subject and the description do in 5.2.
INSERT INTO `remediation_decisions`
SELECT
  tr.`event_time`,
  tr.`service_name`,
  'ticket_responder',
  'HIGH_SEVERITY_TICKET',
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('ROLLBACK', 'SCALE_OUT', 'INVESTIGATE_UPSTREAM')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  CAST(NULL AS STRING),
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`,
  tr.`ticket_id`
FROM (
  SELECT *
  FROM `ticket_triage` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
  WHERE `severity` = 'HIGH'
) tr,
LATERAL TABLE(AI_RUN_AGENT(
  'ticket_responder',
  CONCAT(
    'TICKET ', tr.`ticket_id`, ' from ', tr.`source`, '\n',
    'Priority: ', tr.`priority`, '\n',
    'Subject: ', COALESCE(tr.`subject`, 'N/A'), '\n',
    'Description: ', COALESCE(tr.`description`, 'N/A'), '\n',
    'Classifier said: ', COALESCE(tr.`reason`, 'N/A')
  )
)) AS a(`agent_status`, `agent_response`);
