-- Output table: critical_cves (the scanner's critical findings, append-only)
--
-- SINGLE SOURCE OF TRUTH for the critical_cves schema. The platform team's Terraform
-- (platform-team/modules/developer/statements.tf) creates this table together with the
-- others, so it exists before section 6. Lab 6.1 writes it: an INSERT that copies the
-- critical findings (CVSS 9.0 or more) out of cve_scans, column for column. Lab 6.4 reads
-- it: the security agent gets one call per critical finding. ops-wall reads it too.
-- (D-050, D-070)
--
-- The same nine columns and options as cve_scans, 5 s watermark included, so the filter is
-- a plain WHERE on cvss_score and nothing else changes on the way. A finding scored under
-- 9.0 in here is a filter leak. A critical score is not always a real risk: the finding
-- sentence says where the package sits, and the security agent decides from that.
--
-- Read-uncommitted matters here: Flink commits its sink transactions about once a minute,
-- so a committed reader of this table would see each finding up to a minute late, and the
-- security agent would wait that long for every critical one (D-063). Delete cleanup (not
-- compaction) with 24h matches the source tables and is what lets `uv run reset` and
-- `uv run reset-health` empty the topic with delete-records. One bucket keeps the order.
-- IF NOT EXISTS never changes an existing table: a changed DDL needs a DROP TABLE first.

CREATE TABLE IF NOT EXISTS `critical_cves` (
  `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding, copied from cve_scans',
  `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
  `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
  `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx. The same CVE can be found in several repositories',
  `package` STRING COMMENT 'The vulnerable package, e.g. urllib3',
  `installed_version` STRING COMMENT 'The version the repository has',
  `fixed_version` STRING COMMENT 'The first version without the vulnerability',
  `cvss_score` DOUBLE COMMENT 'CVSS score, always 9.0 or more: lab 6.1 copies only the critical findings, so a lower score here is a filter leak',
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
