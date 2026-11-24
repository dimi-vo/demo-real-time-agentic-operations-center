-- 5.1 Section 5, "A second architecture: support tickets": route the P1 tickets.
-- checkout produces support tickets rather than metrics: about 4 a minute, from Zendesk and
-- ServiceNow, into `support_tickets`. The reporter sets each ticket's priority, and Flink
-- routes on it: this INSERT copies the P1 tickets into `p1_tickets`, so only P1s reach the
-- agents of 5.2 and 5.3 and their model calls. P2 and P3 tickets stay in support_tickets,
-- and nothing further is done with them.
-- `p1_tickets` already exists: the platform team's Terraform created it with the same seven
-- columns (sql/sink_p1_tickets.sql), so this one statement is all you write. The columns are
-- named rather than `SELECT *`, to show that a ticket moves as it is (the INSERT is
-- positional). Reporters over-prioritise, so a P1 is not always a real outage: the
-- classifier (5.2) recovers the real severity.
-- Reads support_tickets from the earliest offset, the table's default, so it replays the
-- tickets ops-sim has written (up to 24 hours). The filter is cheap, and the agents downstream
-- read from the latest offset: start 5.2's INSERT once this one has caught up, and the replay
-- costs no model call.
-- Running it twice copies every P1 twice. To start clean, run `uv run reset-health`, then
-- run this INSERT again.
INSERT INTO `p1_tickets`
SELECT `event_time`, `ticket_id`, `source`, `service_name`, `priority`, `subject`, `description`
FROM `support_tickets`
WHERE `priority` = 'P1';
