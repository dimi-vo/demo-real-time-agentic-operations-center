-- 6.4 Every critical finding to the security agent, and its decision into patch_decisions.
--
-- It reads critical_cves, lab 6.1's output, from the latest offset. Every row is one call to
-- the security agent, which calls the model, and for a PATCH the patch agent on your laptop,
-- so starting it, or starting it again after a stop, never sends old findings to either of
-- them. Start it once 6.1 has caught up with its replay, with `uv run patch-agent` and its
-- tunnel running: the findings that came in before this INSERT started get no decision. There
-- is no filter here, because 6.1 already kept only the critical findings: about 1.2 calls a
-- minute, half of them PATCH.
--
-- Every critical finding is written, DISMISS ones included, so the ops wall sees each
-- decision. The finding's time, id, repository, CVE and score are copied as they came.
--
-- The answer is parsed as in the 4.3 legs (see 4.3_insert_release_agent.sql): action is the
-- Action line when the call succeeded and that line says PATCH or DISMISS, and INVALID
-- otherwise, so it is never null. The Reason line is not the last line here, so it is read to
-- the end of its line. The commit and the patch's start and finish are read out of the Patch
-- line, where the agent copies the patch agent's reply ("... in commit 3f9c2ab, started
-- 2026-12-08T11:42:03.120Z, finished 2026-12-08T11:42:09.870Z"), each with the pattern that
-- scripts/patchagent/reply.py defines for the reply, and each is null when it does not parse.
-- raw_response always keeps the full answer.
--
-- patch_status is worked out here, not by the agent: PATCHED for a PATCH whose Patch line
-- holds a commit, FAILED for a PATCH without one (the agent wrote Patch: FAILED, or copied the
-- reply wrong), and N/A otherwise. So the agent is never asked whether the patch worked, and
-- never has to invent a commit. When patch-agent is stopped, the call fails at its agent card
-- and the answer is Flink's error, with no Action line: INVALID, and N/A.
--
-- Time zones: event_time is a TIMESTAMP(3), a date and time with no zone. ops-sim writes it as
-- epoch milliseconds, so it holds the UTC time, and the ops wall reads it as UTC. The patch
-- agent writes its times in UTC too (the Z), and TO_TIMESTAMP reads a string under the UTC+0
-- time zone into a TIMESTAMP(3), whatever time zone the workspace runs your statement in. So
-- patch_started_at and patch_finished_at hold UTC like event_time, and the wall compares them
-- as they are. (TO_TIMESTAMP_LTZ would make an instant, and casting it to the column's
-- TIMESTAMP(3) would use the workspace's time zone, D-054.) The format quotes T and Z as
-- letters to match, each quote doubled inside the SQL string.
--
-- CONCAT returns null when any argument is null, and the agent would get no finding to read,
-- so each text column falls back to N/A, as in 5.2. The score needs none: 6.1 copies only
-- scores of 9.0 and above, never a null one.
--
-- Running it twice calls the agent, and the patch agent, twice for every finding, and writes
-- every decision twice. To start clean, run `uv run reset-health`, then 6.1 and this INSERT
-- again.
INSERT INTO `patch_decisions`
SELECT
  f.`event_time`, f.`finding_id`, f.`repository`, f.`cve_id`, f.`cvss_score`,
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
          IN ('PATCH', 'DISMISS')
      THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
    ELSE 'INVALID'
  END,
  TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([^\n]+)', 1)),
  CASE
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) = 'PATCH'
      AND REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?in commit ([0-9a-f]{7})\b', 1) IS NOT NULL
      THEN 'PATCHED'
    WHEN a.`agent_status` = 'SUCCESS'
      AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) = 'PATCH'
      THEN 'FAILED'
    ELSE 'N/A'
  END,
  REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?in commit ([0-9a-f]{7})\b', 1),
  TO_TIMESTAMP(
    REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?started (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)', 1),
    'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
  ),
  TO_TIMESTAMP(
    REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?finished (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)', 1),
    'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
  ),
  a.`agent_response`
FROM (
  SELECT *
  FROM `critical_cves` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
) f,
LATERAL TABLE(AI_RUN_AGENT(
  'security_agent',
  CONCAT(
    'FINDING ', COALESCE(f.`finding_id`, 'N/A'), ' in ', COALESCE(f.`repository`, 'N/A'), '\n',
    'CVE: ', COALESCE(f.`cve_id`, 'N/A'), ' (CVSS ', CAST(f.`cvss_score` AS STRING), ')\n',
    'Package: ', COALESCE(f.`package`, 'N/A'), ' ', COALESCE(f.`installed_version`, 'N/A'),
    ', fixed in ', COALESCE(f.`fixed_version`, 'N/A'), '\n',
    'Scanner said: ', COALESCE(f.`finding`, 'N/A')
  )
)) AS a(`agent_status`, `agent_response`);
