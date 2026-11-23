-- Agent calls track anomalies, not windows (brief section 3, improvement 4).
--
-- Running totals over everything the three tables hold. Expect `agent decisions` to track
-- the two flagged counts added up, not the row counts: checkout and payments produce 120
-- service_health rows and web-gateway 60 service_error_rate rows per 10 quiet minutes, and
-- quiet windows should cost no agent calls at all (D-067).
--
-- Plain aggregates on purpose: `remediation_decisions` declares no watermark, so a
-- window TVF over it has no time attribute to use. The three totals keep updating while
-- the query runs; read them once they settle, then stop it.
--
-- The flagged counts follow the routing rules: traffic anomalies go to the capacity
-- agent, latency anomalies to the release agent only when traffic is normal, and a HIGH
-- error rate to the release agent, so each flagged row is one expected call.
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
SELECT 'agent decisions', COUNT(*)
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
