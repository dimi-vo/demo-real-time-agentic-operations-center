-- 6.1 Section 6, "A third architecture: code security incidents": route the critical findings.
-- A scanner reports CVEs in River Shop's repositories: about 4 findings a minute, each one CVE
-- in one repository with its CVSS score, into `cve_scans`. Flink keeps the critical ones, 9.0
-- and above (CVSS's own "Critical" band): this INSERT copies them into `critical_cves`, so only
-- they reach the security agent and its model calls. The other findings stay in cve_scans,
-- and nothing further is done with them.
-- A critical score is not always a real risk: the package may be a dev dependency only, the
-- repository archived, or the vulnerable code never called. The security agent (6.3) reads
-- the scanner's sentence about where the package sits, and only then, for a real risk, calls
-- the external patch agent of 6.2.
-- `critical_cves` already exists: the platform team's Terraform created it with the same nine
-- columns (sql/sink_critical_cves.sql), so this one statement is all you write. The columns
-- are named rather than `SELECT *`, to show that a finding moves as it is (the INSERT is
-- positional).
-- Reads cve_scans from the earliest offset, the table's default, so it replays the findings
-- ops-sim has written (up to 24 hours). The filter is cheap, and the security agent reads
-- critical_cves from the latest offset: start 6.4's INSERT once this one has caught up, and
-- the replay costs no model call and no patch.
-- Running it twice copies every finding twice. To start clean, run `uv run reset-health`,
-- then run this INSERT again.
INSERT INTO `critical_cves`
SELECT `event_time`, `finding_id`, `repository`, `cve_id`, `package`, `installed_version`,
  `fixed_version`, `cvss_score`, `finding`
FROM `cve_scans`
WHERE `cvss_score` >= 9.0;
