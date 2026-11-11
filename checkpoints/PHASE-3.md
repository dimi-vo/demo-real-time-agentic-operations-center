# Break 3: The pipeline up to `service_health`

## 1. Summary

Built and validated the detection pipeline in your environment, plus the detection half of
`uv run evaluate`.

- **`sql/` solution files** for walkthrough section 2, one statement grown step by step:
  - `2.1_regular_join.sql`, `2.1_regular_join_by_version.sql`: the anti-pattern, plus a
    per-version count that makes the re-attribution visible (D-022);
  - `2.1_temporal_join.sql`, `2.1_temporal_join_by_version.sql`: the fix, and the same count;
  - `2.2_window_metrics.sql`, `2.2_window_aggregation.sql`: 10-second metrics, then with
    deployment context through `LAST_VALUE`;
  - `2.3_anomaly_detection.sql`, `2.3_anomaly_status.sql`: two detectors in one statement,
    then the status and expected-value columns;
  - `2.4_service_health.sql`: the CTAS (used by `reset --with-labs`).
  `tests/test_sql_pipeline.py` keeps the shared CTEs, the detector config, the 10-second
  window, the earliest-offset hints and the column list identical across files.
- **Final detector config** (D-029): AR(1) (`p=1, d=0, q=0`), `minTrainingSize` 12,
  `maxTrainingSize` 360, `confidencePercentage` 99.9, `enableStl` false.
- **`sql/checks/`**: developer-only checks (D-026). Exact temporal-join attribution (the
  version hash in `instance_id` against `SHA1` of the joined version), deploy windows,
  anomalous rows, `DESCRIBE`.
- **`uv run evaluate`** (D-021): reads `service_health` over Kafka (read-only, no Flink
  cost) and scores it against `incidents.jsonl`:
  - pipeline checks;
  - detection window per incident, with a version check for bad deploys;
  - false positives per service-hour, labelled as echo, after-deploy or restart;
  - fresh-start noise.
  Options: `--dump`/`--rows` (offline re-scoring), `--topic` (score a variant), `--rescale
  FROM:TO` (re-derive another confidence level offline).
  The code lives in `scripts/common/topic_reader.py` (shared with the Phase 5 dashboard) and
  `scripts/evaluation/detection.py`.
- **Generator fixes found live** (D-028):
  - integer-millisecond tick timestamps (one old-version request had shared a deploy
    record's exact millisecond);
  - whole-second incident durations, so the logged `ended_at` is the real rollback tick;
  - incident ids continue across restarts.
- **Generator rates divided by ~3, outliers softened** (D-030, your call): ~29 msg/s instead
  of ~86.
- **Schemas** (D-027): the `.avsc` copies now match what Confluent registered
  (`local-timestamp-millis`; `service_name` only in the deployments key).
- **Tests:** 120 pass, ruff clean.

**Cumulative state.**
- **Phase 0:** plan and decisions log.
- **Phase 1:** source-table DDL as the single source of truth, plus the offline generator
  (`ops-sim --dry-run/--simulate`).
- **Phase 2a:** platform-team Terraform module.
- **Phase 2b:** developer tooling (`deploy`, `reset`, `destroy`, `setup-rtce`) and the
  generator's Kafka path.
- **Phase 3:** the detection pipeline to `service_health`, tuned live, and `evaluate`.

Commit: this commit.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-030 (open): request rates ÷3 and softer latency outliers.**
  - *Why:* new `service_health` statements replay the history from the earliest offset.
    At ~86 msg/s that held the pool at 10 CFU for several minutes per creation; steady
    state is about 1 CFU.
  - *Impact:* about 3× less replay work. **But checkout's drops are no longer detected:**
    2 of 2 missed. At ~30 requests per window, a drop to ~6 sits right at the model's
    lower bound.
  - *Proper fix (needs your decision, §6):* run the three drop candidates at 6 req/s
    (~41 msg/s in total).
- **DEVIATION D-006 (from the reference, not the brief): delete retention on the upsert
  `service_deployments`.** Now proven live. The temporal join replays history correctly:
  the attribution check found 0 Flink errors, and the one mismatch was a generator
  timestamp collision, now fixed. Can be considered closed.
- **Docs-vs-behavior gap (no workaround needed):** `ML_DETECT_ANOMALIES` with `p=0, d=0,
  q=0` is allowed by the docs. In Confluent Cloud it stalled at the first model fit while
  still reporting `RUNNING`, with no exception. We use AR(1) instead (D-029).
- **Open issue carried from Break 2b:** the `deploy` readiness check reports both source
  tables missing although they exist (D-020;
  `.scratch/deploy-readiness-check/issues/01-source-tables-reported-missing.md`). Fix before
  Phase 6.

No workarounds in code.

## 3. How to test it yourself

**Verified by me:**

*Offline:*
- `uv run pytest`: 120 passed. `uv run ruff check .`: clean.
- `uv run ops-sim --dry-run --simulate 60 --seed 42` at the new rates: same schedule
  (5 bad deploys, 2 surges, 3 drops). Signatures: payments drop ~20 → 3–4 requests per
  window; auth surge ~50 → ~150; cart bad deploy ~46 → ~180 ms with 8–15% errors.
- Across 40 seeds, traffic during bad deploys is unbiased (1.003 × λ).

*Read-only against your environment, with your permission:*
- **Schemas:** the registered schemas, which led to D-027.
- **Attribution replay over Kafka:** every request checked against the deployment history.
  Exactly 1 mismatch, a same-millisecond collision, which led to D-028.
- **Statement status:** the `mean` variants' stall.
- **Pool CFU history:** from the Metrics API.
- **`uv run evaluate` on every variant and the final run:**

| run | data | detected ≤ 3 windows | false positives | fresh start, worst |
|---|---|---|---|---|
| auto-ARIMA, 99.9 (baseline) | 2.1 h, old rates | 19/19 | 22 (12 echoes), worst 2.8/h | 1 |
| AR(1), 99.9 | same 2.1 h | 19/19 | 13 (3 echoes), worst 1.4/h | 2 |
| **final: AR(1), min 12, new rates** | 39 min, fresh | **4/6** (both checkout drops missed) | 3, no echoes | **0** |

**Verified by you (results you reported):**
- The regular join kept counting through a deploy (re-attribution), while the temporal
  join started the new version from zero.
- `DESCRIBE service_health` shows the status columns as `VARCHAR(10)`.
- The CTAS ran with `LAST_VALUE`, both detectors in one statement and `PARTITION BY`.

**Not verified yet:**
- The rewritten, windowed attribution check at the new rates (expect 0 everywhere).
- A drop-and-recreate of `service_health` at the new rates, to measure the replay cost.
- An hour of `evaluate` at the final rates.
- The drop fix, if you choose it.

**To reproduce the final run (about 40–60 minutes):**
1. Stop `ops-sim`. Run `uv run reset`. Expect `service_health` dropped, the topics
   cleared, and the generator state and `incidents.jsonl` deleted.
2. Run `uv run ops-sim` and leave it running. Then create `service_health` from
   `sql/2.4_service_health.sql` right away.
3. Run the two `sql/2.1_*_by_version.sql` queries side by side through a `DEPLOY` line,
   then stop them.
4. After about 10 minutes, run `sql/checks/temporal_join_attribution.sql`. Expect
   `mismatches = 0` and `unmatched = 0`. Stop it.
5. After 40–60 minutes, run `uv run evaluate`. Expect bad deploys and surges detected in
   window 1, zero fresh-start noise, and no echoes. Drops on checkout will be missed until
   the D-030 fix is applied.

Failure looks like:
- `WARMING_UP` after the first minutes (shown under "warm-up later");
- duplicate windows;
- echoes reappearing;
- a statement stuck at its warm-up rows (check with `--topic` and the CFU graph).

## 4. Commands you need to run

None for infrastructure. If you choose the drop fix in §6, it needs a fresh `uv run reset`
and `uv run ops-sim` run, plus `service_health`, to re-validate. That can overlap with
Phase 4, since the agents read `service_health`.

## 5. Decisions and assumptions

**Needs your confirmation first:**
- **D-030 follow-up:** the drop-detection fix (§6).
- **D-022:** the two per-version count queries in walkthrough 2.1 (user-visible flow).

**Confirmed or recorded this phase:**
- **D-009, D-010, D-011:** verified live (`PARTITION BY`, two detectors in one statement,
  `LAST_VALUE`).
- **D-021:** how `evaluate` reads and scores data. False positives are counted per signal.
- **D-023:** the step structure, and shared CTEs with earliest-offset hints on both sides.
- **D-024:** the metric expressions and types.
- **D-025:** confirmed. `VARCHAR(10)` statuses; `WARMING_UP` = `is_anomaly IS NULL`, first
  windows only.
- **D-026:** the `sql/checks/` folder.
- **D-027:** the `.avsc` copies aligned with the registered schemas.
- **D-028:** the generator timing fixes and incident ids.
- **D-029:** the tuning history and the final AR(1) config (your choice).
- **D-030:** the ÷3 rates and softer outliers (your choice).

## 6. Questions for me

1. **Drop detection.** At ~29 msg/s, checkout's drops are missed (0/2).
   - *Recommended:* cart, checkout and payments at 6 req/s, web-gateway 10, catalog 8,
     auth 5. That's ~41 msg/s, about half the brief's volume and 1.4× today's, with
     web-gateway still the busiest.
   - *Alternatives:* the brief's original rates (~86 msg/s), or accept missed drops on
     low-volume services.
2. **D-022.** Keep the two per-version count queries in walkthrough 2.1?

## 7. Next phase

**Phase 4: agents and serving layer.**
- `CREATE AGENT` for `release_agent` and `capacity_agent`.
- `remediation_decisions` with the two routed `INSERT INTO ... AI_RUN_AGENT` statements.
- The decision half of `uv run evaluate`.
- RTCE and Lightning verification. I'll give you the exact Console steps for RTCE.

What I need from you:
- your answers to §6;
- your OK to keep using your card read-only (for `evaluate` and statement status);
- you to run the agent statements in the workspace, as in this phase.

One heads-up: AR(1) keeps an incident flagged for most of its duration instead of only at
onset. Each incident will therefore produce several agent calls, one per anomalous window.

---

# Addendum to Break 3: drop detection fixed offline (D-031)

## 1. Summary

You answered §6 question 1. The fix was chosen with an offline tool instead of 60-minute live replays.
- **`scripts/tuning/`** (developer-only):
  - `arima.py`: a replica of the AR(1) detector;
  - `calibrate.py`: checks the replica against a recorded `service_health` dump;
  - `sweep.py`: runs the real generator through the replica, scored with `evaluate`'s own
    code.
- **Generator:**
  - rates are now web-gateway 10, catalog 8, auth 5, and cart, checkout and payments 6
    requests/s (~41 msg/s);
  - `ops-sim` starts on the next 10-second boundary (it waits at most 10 s), so the first
    window is never half empty.
- **Detector config unchanged** (D-029).
- **Tests:** 136 pass, ruff clean.

Commit: this commit.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-031** replaces D-030's rates; D-030's softer outliers stay. Volume and replay
  work are about 1.4× the D-030 rates and still about half the brief's.

## 3. How to test it yourself

**Verified by me (offline):**
- **Replica against the recorded live run:** statuses agree on 97–100% of windows. On the
  drop services' traffic, the bands are within 0–12% of Confluent's and statuses agree on
  99.7–100%.
- **Sweep, 24 simulated hours:**
  - drops detected: 61/61 (aligned start) and 60/61 (unaligned);
  - false positives: about 0.6 per service-hour on average, about 1.0 for the worst service.
- **Narrower bounds or AR(2) on the old data:** they catch more drops but multiply false
  positives (full table in D-031).
- **Seeded schedule:** `--simulate 60 --seed 42` still has its 10 incidents.

**Not verified yet (deferred at your request):** one live run at the new rates. Steps:
1. `uv run reset`, then `uv run ops-sim`. It prints `Starting on the next 10-second
   boundary...`.
2. Create `service_health` right away.
3. After 45–60 minutes, I run `evaluate` and `calibrate` read-only.

This can run alongside Phase 4.

## 6. Questions for me

- Still open: **D-022**, whether to keep the two per-version count queries in walkthrough
  2.1.
