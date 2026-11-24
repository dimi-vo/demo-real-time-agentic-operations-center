-- 5.2 Every P1 ticket to the ticket classifier, and its verdict into ticket_triage.
--
-- It reads p1_tickets, lab 5.1's output, from the latest offset, so starting it, or starting
-- it again after a stop, never re-classifies old tickets with fresh model calls. Start it once
-- 5.1 has caught up with its replay: the P1s that came in before this INSERT started get no
-- verdict. There is no filter here, because 5.1 already kept only the P1s: every row is one
-- agent call, about 1.2 a minute.
--
-- Every P1 is written, LOW ones included, so the ops wall and `uv run evaluate` see each
-- verdict, not only the tickets that go on. Lab 5.3 reads the HIGH rows and hands each one to
-- the responder. The seven ticket columns are copied as they came, so 5.3 reads one table
-- without a join.
--
-- The answer is parsed as in the 4.3 legs (see 4.3_insert_release_agent.sql): severity is
-- the Severity line when the call succeeded and that line says HIGH or LOW, and INVALID
-- otherwise, so it is never null. raw_response always keeps the full answer.
-- CONCAT returns null when any argument is null, and the agent would get no ticket to read,
-- so the subject and the description fall back to N/A, as Last Change does in 4.3. The
-- generator always sets both, and the id, the source and the priority (always P1) too.
INSERT INTO `ticket_triage`
SELECT
  t.`event_time`, t.`ticket_id`, t.`source`, t.`service_name`, t.`priority`, t.`subject`, t.`description`,
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Severity:\*{0,2}\s*([^\n]+)', 1)))
          IN ('HIGH', 'LOW')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Severity:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
  a.`agent_response`
FROM (
  SELECT *
  FROM `p1_tickets` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
) t,
LATERAL TABLE(AI_RUN_AGENT(
  'ticket_classifier',
  CONCAT(
    'TICKET ', t.`ticket_id`, ' from ', t.`source`, '\n',
    'Priority: ', t.`priority`, '\n',
    'Subject: ', COALESCE(t.`subject`, 'N/A'), '\n',
    'Description: ', COALESCE(t.`description`, 'N/A')
  )
)) AS a(`agent_status`, `agent_response`);
