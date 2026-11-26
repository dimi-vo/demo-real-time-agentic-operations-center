-- Source table: cve_scans (CVE scan findings in River Shop's repositories, append-only)
--
-- SINGLE SOURCE OF TRUTH for the cve_scans schema. The platform team's
-- Terraform (platform-team/modules/developer/statements.tf) creates this table;
-- the generator (opsgen) writes to it, lab 6.1 routes its critical findings, and the
-- dashboard (ops-wall) reads from it. The .avsc copy in schemas/ is for --dry-run and
-- the schema-vs-DDL drift test only (tests/test_schema_vs_ddl.py), never for live
-- serialization.
--
-- A scanner reports findings, each one CVE in one repository (D-070): River Shop's
-- three services, ci-tools and the archived storefront-legacy. About 4 a minute, one
-- record per finding, written when it is reported. The CVE ids are fictional
-- (CVE-2026-4xxxx) on real packages, so nobody takes a demo record for a real advisory.
-- The score says how bad the CVE is; the finding sentence says where the package sits
-- (shipped in a deployed service or not, the vulnerable code used or not), and that is
-- what decides whether a critical finding matters. Nothing here says what to do about
-- it; the simulator writes that to cve_answer_key, which the agents never read.
-- Append-only so the findings can be replayed. No key columns, exactly like
-- support_tickets: the generator sends the raw repository string as the message key, and
-- repository stays in the value. (DISTRIBUTED BY HASH would make repository a key column,
-- which Confluent Cloud requires first in the schema and registers as an Avro key the
-- generator doesn't write; one bucket keeps the order anyway.)
--
-- delete retention (not compaction) with 24h matches service_metrics: an
-- append-only log is never compacted, and 24h caps storage per attendee.

CREATE TABLE IF NOT EXISTS `cve_scans` (
  `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding; also the Kafka record timestamp',
  `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
  `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
  `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx. The same CVE can be found in several repositories',
  `package` STRING COMMENT 'The vulnerable package, e.g. urllib3',
  `installed_version` STRING COMMENT 'The version the repository has',
  `fixed_version` STRING COMMENT 'The first version without the vulnerability',
  `cvss_score` DOUBLE COMMENT 'CVSS score, 0.0 to 10.0 with one decimal; critical at 9.0 or more (lab 6.1)',
  `finding` STRING COMMENT 'What the scanner said: where the package sits and whether the vulnerable code is used',
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
