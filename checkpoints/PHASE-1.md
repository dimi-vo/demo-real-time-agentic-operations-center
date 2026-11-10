# Break 1 — Source tables and generator (offline)

## 1. Summary

**This phase** built the two source-table schemas and the whole generator, offline.

- **`sql/source_service_requests.sql`, `sql/source_service_deployments.sql`** — the
  standalone `CREATE TABLE` statements, written first, as the single source of truth
  for both schemas. Phase 2's Terraform will use them verbatim.
- **`schemas/*.avsc`** — the Avro value-schema copies for `--dry-run`/tests, guarded
  against DDL drift by `tests/test_schema_vs_ddl.py`.
- **`opsgen/`** — the generator: service catalog (`services.py`), sampling
  (`distributions.py`), instance naming (`instances.py`), version/rollback semantics
  (`deployments.py`), the incident + benign-deploy state machine and scheduler
  (`engine.py`), the per-tick simulation that ties them together (`simulation.py`),
  output sinks (`sinks.py`), the drivers (`driver.py` — real-time `--dry-run` and the
  fast `--simulate`), the CLI (`cli.py`), plus state persistence (`state.py`) and
  ground-truth logging (`groundtruth.py`) ready for Phase 2, and a Kafka-path
  placeholder (`kafka_io.py`).
- **`pyproject.toml`** — `uv` project with the `ops-sim` entry point (others are
  declared for later phases).
- **55 pytest tests** across `opsgen/tests/` and `tests/`.

**Cumulative state.**
- *Phase 0:* planned the conversion; `PLAN.md` + `DECISIONS.md`.
- *Phase 1:* the source-table DDL and the offline generator (this phase). No Kafka,
  no infrastructure yet.

Commit: see the commit this report is committed in.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-006 (still open, from Phase 0)** — `service_deployments` will use
  delete retention instead of compaction. Verified in Phase 2. (The DDL is written;
  the live `CREATE TABLE` runs in Phase 2.)
- **DEVIATION D-018 (new)** — the generator uses **two independent RNG streams**
  (scheduler vs. traffic) rather than one. **Why:** with a shared stream the schedule
  shifted with traffic volume, and seed 42 produced zero `UPSTREAM_DROP` incidents
  over 60 min, which would fail the brief's own acceptance command. Independent
  streams make the schedule a pure function of seed+config. **Impact:** positive —
  `--seed` reproducibility is now robust; seed 42 shows all three signatures. No
  proper-fix debt.

Workarounds: none.

## 3. How to test it yourself

Preconditions: `uv` installed; run from the repo root. First run downloads CPython
3.12 and the deps (~1 min).

**a) Tests + lint (~5s after deps).**
```
uv run pytest
uv run ruff check opsgen tests
```
Expect `55 passed` and `All checks passed!`. Failure looks like a pytest `F`/`ruff`
error naming the file.

**b) Raw events (section 4.2).**
```
uv run ops-sim --dry-run --seed 42        # Ctrl-C to stop; it runs continuously
```
Each line is a JSON object. `service_deployments` records come first (one per
service, `last_change=INITIAL`, correct replicas/tier/team). `service_requests` lines
carry exactly: `event_time, service_name, instance_id, http_method, route,
status_code, latency_ms, response_bytes, trace_id`; `instance_id` looks like
`checkout-7d9f8c-x2k4p`; `trace_id` is 32 hex. (Schedule events also appear as
`{"_event": ...}` lines on stdout and as human lines on stderr.)

**c) Every scheduling rule in one view (section 4.5).**
```
uv run ops-sim --dry-run --simulate 60 --seed 42 > /tmp/sim60.txt
```
Runs an hour of simulated time in <1s, one summary line per service per 10-second
window plus the deploy/incident log. Specific lines that show each rule (seed 42):

- **Quiet period (no incidents for 3 min):** first `INCIDENT START` is at `[03:00]`.
  `grep "INCIDENT START" /tmp/sim60.txt | head -1`
- **Benign deploys, and before any incident:** `[01:00] DEPLOY payments 2.9.4 -> 2.9.5 (benign)`.
- **All three incident types present:**
  `grep -oE "BAD_DEPLOY|TRAFFIC_SURGE|UPSTREAM_DROP" /tmp/sim60.txt | sort | uniq -c`
  → 5 BAD_DEPLOY, 2 TRAFFIC_SURGE, 3 UPSTREAM_DROP (counts double because of
  start+end lines).
- **BAD_DEPLOY signature (latency ×4, ~12% errors, 10s onset, version hash flips and
  restores on rollback):** `grep -E "^\[t=(09|10):" /tmp/sim60.txt | grep catalog` —
  version flips `5.0.3→5.0.4` (hash `c5c22f→7f12c3`) at `09:20`, latency climbs to
  ~250 ms with ~12–17% errors from `09:30`, then **rollback** `5.0.4→5.0.3` at
  `[10:30]` returns to baseline.
- **TRAFFIC_SURGE (λ×3, latency×1.5):** web-gateway `235 → ~780` reqs/window,
  latency `~38 → ~55 ms` at `[t=03:10]` onward.
- **UPSTREAM_DROP (λ×0.2, never zero, latency unchanged):** payments `~63 → 5/6/6/9`
  reqs/window (measured ratio is exactly 0.20 over 300s), latency steady ~230 ms.
- **Cooldown ≥ 15 min per service:** every service with two incidents is >900s apart
  (auth 1347s, payments 1367s, cart 1342s, checkout 1425s).
- **Determinism:** rerun the command; the output is byte-identical.

**d) Benign-only mode.**
```
uv run ops-sim --dry-run --simulate 20 --seed 3 --no-incidents
```
Deploys appear, `INCIDENT START` never does.

**Verified by me:** all of the above (ran them; 55 tests pass, ruff clean, the drop
ratio measured at 0.2016, cooldown/quiet/spacing confirmed on the seed-42 run).
**Needs you:** nothing — this phase is fully offline.

## 4. Commands you need to run

None (offline phase).

## 5. Decisions and assumptions

**Assumptions still awaiting your confirmation** (from Phase 0, verifiable only in a
live environment in Phases 2–3): D-009 `PARTITION BY` in the detector, D-010 two
detectors in one statement, D-011 `LAST_VALUE` in a window aggregation, D-016 upsert
+ delete cleanup.

**New this phase:** D-017 (Python `>=3.10,<3.13` for wheel availability), D-018 (split
scheduler/traffic RNG — see §2), D-019 (`--dry-run` writes nothing; persistence is a
live-mode concern). Full text in `DECISIONS.md`.

## 6. Questions for me

None blocking. One heads-up for Phase 2: the generator serializes against the schema
Flink registers from the DDL. The `service_deployments` value `.avsc` currently
includes the primary-key column `service_name` (Confluent's `value.fields-include=all`
default, inferred from how the reference simulator serializes `race_standings`). I'll
reconfirm against the actually-registered schema in Phase 2 and adjust the `.avsc` +
drift test if Confluent excludes the key from the value.

## 7. Next phase

**Phase 2 — infrastructure and developer tooling.** The platform-team Terraform module
(using the Phase 1 DDL) and its README; `deploy` / `reset` / `destroy` / `setup-rtce`;
and the generator's Kafka path (`opsgen/kafka_io.py`: Schema Registry lookup and
key/value serialization mirroring the reference simulator — raw string key for
`service_requests`, Avro-encoded key for `service_deployments`). From you I'll need: a
confirmed region (default `us-east-1`), and then to run `terraform plan`/`apply` for
one test developer and `uv run deploy --track pre-provisioned` yourself (I never run
Terraform or anything that creates cloud resources).
