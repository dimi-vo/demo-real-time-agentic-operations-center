# Developer checks

Verification queries for building and tuning the workshop. They are **not** walkthrough
steps: the walkthrough drift test and `reset --with-labs` only use the top-level
`sql/*.sql` files. Run them in a Flink SQL workspace (catalog = your environment,
database = `RIVER-OOPS-DEMO-CLUSTER`) and stop them when you're done.

| File | What it proves |
|---|---|
| `temporal_join_attribution.sql` | Every request is joined to the version that actually served it: the version hash in `instance_id` (written by `ops-sim` from its own state) matches a hash of the version Flink joined, per service per 10 minutes. Expect `mismatches = 0` and `unmatched = 0`. (One mismatch at 09:58 on 2026-11-11 was a generator timestamp collision, fixed in D-028.) |
| `service_health_deploy_windows.sql` | Windows that contain a deploy carry the **new** version (`LAST_VALUE`, D-011). Compare with the `ops-sim` console. |
| `service_health_anomalies.sql` | Every anomalous `service_health` row, with actual versus expected values. |
| `service_health_describe.sql` | Column types of the CTAS output (the status columns should be `STRING`, not padded `CHAR`). |
