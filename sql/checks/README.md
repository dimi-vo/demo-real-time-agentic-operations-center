# Developer checks

Verification queries for building and tuning the workshop. They are **not** walkthrough
steps: the walkthrough drift test only uses the top-level `sql/*.sql` files. Run them in a
Flink SQL workspace (catalog = your environment, database = `RIVER-OOPS-DEMO-CLUSTER`) and
stop them when you're done.

| File | What it proves |
|---|---|
| `service_health_deploy_windows.sql` | Windows that contain a deploy carry the **new** version (`LAST_VALUE`, D-011). Compare with the `ops-sim` console. |
| `service_health_anomalies.sql` | Every anomalous `service_health` row, with actual versus expected values. |
| `service_health_describe.sql` | Column types of the table as declared in its DDL (`sql/sink_service_health.sql`); the status columns should be `STRING`, and the 19th and last column `detected_at` should be `TIMESTAMP_LTZ(3) NOT NULL` (D-054). The first `terraform apply` is the live check of the derived types (D-050). |
| `remediation_routing.sql` | Each decision comes from the right agent: no (service, window) reached both metric agents, and `HIGH_SEVERITY_TICKET` comes only from `ticket_responder`, with a `ticket_id` and no target, while the metric agents never write a `ticket_id` (D-068). Expect **zero rows**. |
| `remediation_call_rate.sql` | Agent calls track anomalies and P1 tickets, not windows: running totals of `service_health` and `service_error_rate` rows, their flagged rows and the metric agents' decisions; then the ticket legs (D-068): `p1_tickets` rows, `ticket_triage` rows and their HIGH verdicts, and the responder's decisions. |
