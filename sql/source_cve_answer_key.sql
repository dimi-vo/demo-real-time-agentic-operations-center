-- Source table: cve_answer_key (simulator ground truth for CVE scan findings, append-only)
--
-- SINGLE SOURCE OF TRUTH for the cve_answer_key schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) writes to it and the dashboard (ops-wall) reads from it
-- (`uv run evaluate` ignores it for now). The .avsc copy in schemas/ is for --dry-run and
-- the schema-vs-DDL drift test only (tests/test_schema_vs_ddl.py), never for live
-- serialization.
--
-- FOR THE WALL AND EVALUATE ONLY. The simulator knows what each finding calls for; the
-- security agent must work it out from the scanner's sentence. This topic publishes the
-- truth so the wall can score the security agent, the way ticket_answer_key scores the
-- ticket agents (D-068, D-070). No lab and no agent reads it. One record per finding,
-- critical or not, on the same tick as the finding in cve_scans, tied to it by
-- finding_id. Append-only so the history can be replayed. No key columns: the generator
-- sends the raw finding_id string as the message key, and finding_id stays in the value.
-- (DISTRIBUTED BY HASH would make it a key column, which Confluent Cloud registers as an
-- Avro key the generator doesn't write; one bucket keeps the order anyway.)
--
-- delete retention (not compaction) with 24h matches service_metrics: an
-- append-only log is never compacted, and 24h caps storage per attendee.

CREATE TABLE IF NOT EXISTS `cve_answer_key` (
  `event_time` TIMESTAMP(3) COMMENT 'When the finding was reported (its cve_scans event_time); also the Kafka record timestamp',
  `finding_id` STRING COMMENT 'The finding this answers for (cve_scans.finding_id)',
  `expected_action` STRING COMMENT 'For a critical finding (CVSS 9.0 or more), what the security agent should answer: PATCH (shipped in a deployed service and the vulnerable code is used) or DISMISS; NULL when not critical',
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
)
DISTRIBUTED INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'kafka.message-timestamp-type' = 'create-time',
  'kafka.retention.time' = '24 h',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);
