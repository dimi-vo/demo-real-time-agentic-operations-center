-- Agent calls track anomalies, not windows (brief section 3, improvement 4).
--
-- Three running totals over everything both tables hold. Expect `agent decisions` to
-- track `anomalous rows`, not `service_health rows`: six services produce 360 rows per
-- 10 quiet minutes and should cost no agent calls at all.
--
-- Plain aggregates on purpose: `remediation_decisions` declares no watermark, so a
-- window TVF over it has no time attribute to use. The three totals keep updating while
-- the query runs; read them once they settle, then stop it.
--
-- The anomaly count follows the routing rules: traffic anomalies go to the capacity
-- agent, and latency anomalies only when traffic is normal, so each anomalous row is
-- one expected call.
SELECT 'service_health rows' AS what, COUNT(*) AS n
FROM `service_health` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'anomalous rows (one expected agent call each)', CAST(SUM(
  CASE
    WHEN traffic_status IN ('HIGH', 'LOW') THEN 1
    WHEN latency_status = 'HIGH' THEN 1
    ELSE 0
  END) AS BIGINT)
FROM `service_health` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
UNION ALL
SELECT 'agent decisions', COUNT(*)
FROM `remediation_decisions` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;
