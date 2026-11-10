# Break 0 — Plan

## 1. Summary

Phase 0 (plan) is complete. I studied the reference F1 workshop's pre-provisioned
track end to end, classified every reference file by whether that track touches it,
and verified the Confluent features the brief relies on against the current docs
(2026-11). The output is three files in this repo:

- **`PLAN.md`** — the file-by-file mapping from the reference (reuse / adapt / drop /
  new), the proposed repo layout and Python package split, the conflicts I found
  between the brief, the reference and the docs, and open questions.
- **`DECISIONS.md`** — 16 entries (`D-001`…`D-016`): decisions, assumptions, one
  deviation and the SQL-feature items to verify in your environment.
- **`README.md`** (stub), plus the Apache-2.0 `LICENSE`, `.gitignore` and
  `.gitleaks.toml` carried over from the reference.

**Cumulative state.** Phase 0 only — no code, no infrastructure. Commit hash: see the
commit this report is committed in.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-006** — `service_deployments` will use delete retention
  (`cleanup-policy = delete`, 24 h) instead of compaction. The reference's upsert
  `race_standings` is compacted. **Why:** the temporal join and the detector replay
  this topic from the earliest offset and must attribute each historical request to
  the version live at that request's event time; compaction keeps only the latest
  version per service and would mis-attribute replayed requests. **Impact:** depends
  on Confluent Cloud Flink accepting an `upsert` table on a `delete`-cleanup topic
  (D-016, verified in Phase 2). **Proper fix if rejected:** `delete-compact` with a
  long compaction lag, or accepting replay only within the compaction window — either
  would be raised before I proceed.

No workarounds yet.

## 3. How to test it yourself

Nothing to run at Break 0. To review:

- Read `PLAN.md` (mapping, layout, conflicts, questions) and `DECISIONS.md`.
- The reference is at `../demo-confluent-intelligence-f1` if you want to compare.

**Verified by me:** the Confluent-doc findings in `PLAN.md` §1 come from four
research passes over the current docs; the reference-file classification comes from
reading the files. **Not verified (needs your environment, Phases 2–3):** the SQL
feature assumptions D-009 (`PARTITION BY` in the detector), D-010 (two detectors in
one statement), D-011 (`LAST_VALUE` in a window aggregation), D-016 (upsert + delete
cleanup).

## 4. Commands you need to run

None.

## 5. Decisions and assumptions

**Assumptions to confirm (listed first):**
- D-003 resource naming prefix `RIVER-SHOP-DEMO` (attendee-visible).
- D-004 CSP Hosted model default `anthropic.claude-sonnet-4-6`.
- D-005 region default `us-east-1`.
- D-009/D-010/D-011/D-016 — SQL feature support (verifiable only in your environment).

**New decisions:** D-001 (layout), D-002 (`opsgen` package), D-007 (`AI_RUN_AGENT`
`ROW<status,response>` + `handle_exception=continue`), D-008 (Lightning `/query/v1`),
D-012 (drop `driver_race_history`), D-013 (hand-authored docs site), D-014 (ARIMA
config), D-015 (single partition). **Deviation:** D-006 (see §2). Full text in
`DECISIONS.md`.

## 6. Questions for me

See `PLAN.md` §5. The four that would change attendee-visible things or Phase 6:
1. Resource naming prefix `RIVER-SHOP-DEMO` — OK?
2. CSP Hosted model default — keep `anthropic.claude-sonnet-4-6`?
3. Docs site — hand-author parallel HTML (matches the brief) or add a markdown→HTML
   generator (single source)?
4. `runs/pre-provisioned/` as the home for `incidents.jsonl` and persisted state — OK?

None of these block Phase 1's generator internals.

## 7. Next phase

**Phase 1 (offline).** Source-table DDL (`service_requests`, `service_deployments`)
first, then the `.avsc` copies + schema-vs-DDL test, then `ops-sim` (all CLI options,
incident state machine, persisted state, ground-truth log) and the pytest suite. No
Kafka. I need only your answers to the questions above to start; they don't block the
generator's internals.
