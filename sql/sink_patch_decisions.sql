-- Output table: patch_decisions (the security agent's decision on each critical finding, append-only)
--
-- SINGLE SOURCE OF TRUTH for the patch_decisions schema. The platform team's Terraform
-- (platform-team/modules/developer/statements.tf) creates this table together with the
-- others, so it exists before section 6, and turns the Real-Time Context Engine on for its
-- topic (confluent_rtce_topic), so a coding agent can ask about the decisions. Lab 6.4
-- writes it: the security agent's answer for each critical_cves row, one row per finding,
-- with what the patch agent on the participant's laptop answered when the security agent
-- called it (lab 6.2's tool). ops-wall reads it. (D-050, D-070)
--
-- The finding comes first, copied from critical_cves: enough to say which finding in which
-- repository, without a join. Then the decision: action is PATCH or DISMISS when the call
-- succeeded and the answer parsed to one of them, and INVALID otherwise (never null, like
-- ticket_triage.severity); reason is the security agent's explanation. Then the patch:
-- patch_status is PATCHED for a PATCH whose Patch: line holds a commit, FAILED for a PATCH
-- without one, and N/A otherwise (never null; lab 6.4 works it out, so the agent is never
-- asked to invent a commit). commit_sha and the patch's start and finish are parsed out of
-- the patch agent's reply, which the security agent copies into its Patch: line, and are
-- null unless they parse. raw_response keeps the full answer. Nothing here says what the
-- finding really called for: that is in cve_answer_key, which no agent reads.
--
-- The same options as critical_cves, 5 s watermark included. Read-uncommitted matters here
-- too: a committed reader would see each decision up to a minute late (D-063), and the
-- wall would draw it that much later. Delete cleanup (not compaction) with 24h is what lets
-- `uv run reset` and `uv run reset-health` empty the topic with delete-records. One bucket
-- keeps the order. IF NOT EXISTS never changes an existing table: a changed DDL needs a
-- DROP TABLE first.

CREATE TABLE IF NOT EXISTS `patch_decisions` (
  `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding, copied from critical_cves',
  `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
  `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
  `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx',
  `cvss_score` DOUBLE COMMENT 'CVSS score, 9.0 or more, as in critical_cves',
  `action` STRING NOT NULL COMMENT 'The security agent decision: PATCH or DISMISS, or INVALID when the call failed or the answer did not parse',
  `reason` STRING COMMENT 'The security agent explanation, one or two sentences',
  `patch_status` STRING NOT NULL COMMENT 'PATCHED for a PATCH that came back with a commit, FAILED for a PATCH without one, N/A otherwise; worked out by lab 6.4, not the agent',
  `commit_sha` STRING COMMENT 'The commit the patch agent answered with, 7 hex digits; null unless parsed',
  `patch_started_at` TIMESTAMP(3) COMMENT 'When the patch agent started, from its reply; null unless parsed',
  `patch_finished_at` TIMESTAMP(3) COMMENT 'When the patch agent finished, from its reply; null unless parsed',
  `raw_response` STRING COMMENT 'The full security agent answer, kept for debugging when parsing returns null',
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
