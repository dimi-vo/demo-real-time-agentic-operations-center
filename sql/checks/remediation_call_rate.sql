-- Agent calls track anomalies, P1 tickets and critical findings, not windows (brief section 3,
-- improvement 4).
--
-- Running totals over everything the tables hold. Expect `metric agent decisions` to track
-- the two flagged counts added up, not the row counts: payments produces 60 service_health
-- rows and web-gateway 60 service_error_rate rows per 10 quiet minutes, and quiet windows
-- should cost no agent calls at all (D-067, D-068).
--
-- The ticket legs (D-068) cost one call per ticket that reaches them: the classifier one per
-- P1 ticket (`ticket_triage rows` tracks `p1_tickets rows`, about 1.2 a minute), the
-- responder one per HIGH verdict (`ticket decisions` tracks `HIGH ticket_triage rows`). Both
-- agent INSERTs read from the latest offset, so the tickets that came in before one was
-- started have no row: a 5.2 started after 5.1's replay leaves `ticket_triage rows` behind
-- `p1_tickets rows` by that replay, and only the gap that keeps growing is a problem. P2
-- and P3 tickets cost nothing; `p1_tickets` holds none (lab 5.1's filter).
--
-- Section 6 (D-070) works the same way: the security agent costs one call per critical
-- finding (`patch_decisions rows` tracks `critical_cves rows`, about 1.2 a minute), and the
-- patch agent on the laptop one call per PATCH decision (about half of them). Lab 6.4 reads
-- from the latest offset, like 5.2, so a gap left by 6.1's replay is expected. `PATCHED
-- decisions` short of `PATCH decisions` means a patch agent call that brought no commit back:
-- the patch agent or its tunnel was down, or the security agent did not copy the reply
-- exactly (patch_status FAILED).
--
-- Plain aggregates on purpose: `remediation_decisions` declares no watermark, so a
-- window TVF over it has no time attribute to use. The totals keep updating while the
-- query runs; read them once they settle, then stop it.
--
-- The flagged counts follow the routing rules: traffic anomalies go to the capacity
-- agent, latency anomalies to the release agent only when traffic is normal, and a HIGH
-- error rate to the release agent, so each flagged row is one expected call. A decision with
-- a ticket_id is the responder's (lab 5.3); the metric agents write it as null.
SELECT 'service_health rows' AS what, COUNT(*) AS n
FROM `service_health` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'service_error_rate rows', COUNT(*)
FROM `service_error_rate` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'anomalous service_health rows (one expected agent call each)', CAST(SUM(
  CASE
    WHEN traffic_status IN ('HIGH', 'LOW') THEN 1
    WHEN latency_status = 'HIGH' THEN 1
    ELSE 0
  END) AS BIGINT)
FROM `service_health` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'HIGH service_error_rate rows (one expected agent call each)', CAST(SUM(
  CASE WHEN error_status = 'HIGH' THEN 1 ELSE 0 END
) AS BIGINT)
FROM `service_error_rate` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'metric agent decisions', CAST(SUM(
  CASE WHEN ticket_id IS NULL THEN 1 ELSE 0 END
) AS BIGINT)
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'p1_tickets rows (one expected classifier call each)', COUNT(*)
FROM `p1_tickets` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'ticket_triage rows (classifier calls)', COUNT(*)
FROM `ticket_triage` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'HIGH ticket_triage rows (one expected responder call each)', CAST(SUM(
  CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END
) AS BIGINT)
FROM `ticket_triage` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'ticket decisions', CAST(SUM(
  CASE WHEN ticket_id IS NOT NULL THEN 1 ELSE 0 END
) AS BIGINT)
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'critical_cves rows (one expected security agent call each)', COUNT(*)
FROM `critical_cves` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'patch_decisions rows (security agent calls)', COUNT(*)
FROM `patch_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'PATCH decisions (one expected patch agent call each)', CAST(SUM(
  CASE WHEN `action` = 'PATCH' THEN 1 ELSE 0 END
) AS BIGINT)
FROM `patch_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'PATCHED decisions (a commit came back)', CAST(SUM(
  CASE WHEN patch_status = 'PATCHED' THEN 1 ELSE 0 END
) AS BIGINT)
FROM `patch_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
