# Break 2b — Developer tooling and the generator's Kafka path

## 1. Summary

Built the developer tooling and the generator's live Kafka output, all offline-tested.

- **`deploy.py` + `scripts/preprovisioned/{provision,lookup,keys}.py`** — `uv run deploy
  --track pre-provisioned`: CLI login check → mint a Global key (CLI) → look up the user
  and environment → find the platform team's cluster/pool/SR → mint Kafka + SR keys over
  REST → write the credential card → verify both source tables (via registered SR
  subjects) and `llm_textgen_model` (via `SHOW MODELS`). Key IDs are recorded as they're
  minted; a run that stops before the card is written rolls them back.
- **`scripts/common/`** — `paths`, `deployment_meta` (runs/pre-provisioned/deployment.env),
  `credentials` (card resolution + `AIOPS_CARD` pointer), `card` (the `DEMO.env`/`DEMO.md`
  generator), `ui`, `login_checks`, `flink` (a lean `FlinkSession`, `model_exists`,
  `create_lab_objects`), `destroy`.
- **`scripts/reset.py`** — refuses while a local `ops-sim` is producing; stops running
  statements; drops `service_health` / `remediation_decisions` / both agents and their SR
  subjects; clears the source topics (both are delete-cleanup, so `delete_records` works);
  deletes the generator's persisted state and `incidents.jsonl`; `--with-labs` rebuilds
  from `sql/`.
- **`scripts/common/destroy.py`** — `uv run destroy`: deletes the three keys (Global last)
  and the card; the environment stays.
- **`scripts/setup_rtce.py`** — registers RTCE as an MCP server (Claude Code / Codex / VS
  Code / Copilot) and `--lightning` prints a `/query/v1` curl against `service_health`.
- **`opsgen/kafka_io.py` + `opsgen/cli.py` live mode** — `uv run ops-sim` now produces to
  Kafka: SR schema lookup, raw string key for `service_requests`, Avro-encoded key for
  `service_deployments` (primitive-or-record, inspected once), `event_time` as the Kafka
  record timestamp, continuing from persisted state and writing `incidents.jsonl`.
- **`scripts/evaluate.py`** — stub (full tool lands in Phases 3-4).
- **Tests** — `test_preprovisioned_lookup`, `test_card_and_meta`, `test_kafka_io_and_rtce`,
  `test_destroy`. **81 pytest pass; ruff clean.**

**Cumulative state.** Phase 0 (plan) → Phase 1 (DDL + offline generator) → Phase 2a
(Terraform module) → Phase 2b (tooling + Kafka path). Commit: this commit.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-006 (open)** — `service_deployments` delete retention. Confirmed at Break
  1; proven when you apply (2a) and when `ops-sim` produces to it (2b).

No workarounds. New decision D-020 (readiness check via SR subjects + `SHOW MODELS`).

## 3. How to test it yourself

**Verified by me (offline):** `uv run pytest` → **81 passed**; `uv run ruff check .` →
clean; every tooling module imports; `uv run deploy --help`, `uv run reset --help`,
`uv run evaluate`, and `uv run setup-rtce --lightning` (without a card) behave. I did
**not** run `uv run deploy` or `uv run ops-sim` live — they mint keys / create resources,
which is yours to run.

**Needs you (against the environment you applied in 2a):**

1. **Deploy** (from the repo root, with `confluent login` done as the test developer):
   ```bash
   uv run deploy --track pre-provisioned
   ```
   Expect: it mints a Global, Kafka and SR key, writes `runs/pre-provisioned/credentials/DEMO.env`,
   and prints `✓ table service_requests`, `✓ table service_deployments`, `✓ model
   llm_textgen_model`, then the Flink workspace URL. (Re-running reuses the card while its
   keys still exist.)

2. **Generate** (leave running):
   ```bash
   uv run ops-sim
   ```
   Expect one console line per deploy/incident; both topics filling.

3. **Verify in a Flink SQL workspace** (catalog = your env, database = `RIVER-OOPS-DEMO-CLUSTER`):
   ```sql
   SELECT * FROM service_requests LIMIT 20;      -- typed rows: status_code INT, latency_ms DOUBLE, event_time TIMESTAMP(3)
   SELECT COUNT(*) FROM service_requests;        -- grows ~90/sec
   SELECT service_name, version, last_change, ready_replicas, max_replicas, event_time
     FROM service_deployments;                    -- 6 rows; event_time advances every ~5s (the re-emit),
                                                   -- and version/last_change change when ops-sim logs a DEPLOY
   ```
   Watch the `ops-sim` console for a `DEPLOY`/`BAD DEPLOY` line, then re-query
   `service_deployments` — that service's `version` and `last_change` should match.

4. **Reset** (stop `ops-sim` first with Ctrl-C):
   ```bash
   uv run reset
   ```
   Expect: it stops statements, reports the source topics cleared, and deletes the local
   state/`incidents.jsonl`. (There are no lab objects yet — those come in Phases 3-4 — so
   the drops report "already gone", which is fine.) `uv run reset --with-labs` will say the
   `sql/` solution files don't exist yet; that's expected until Phase 3-4.

Failure looks like: deploy stopping with a named error (bad login, no environment, missing
table/model), or `ops-sim` raising on a missing card field.

## 4. Commands you need to run

The four above (deploy, ops-sim, the Flink queries, reset). `destroy` is tested at Phase 6.
Please paste back: the deploy's `✓`/`✗` lines, a few `service_requests` rows, the
`service_deployments` rows before and after a `DEPLOY`, and the `reset` output.

## 5. Decisions and assumptions

New: **D-020** (readiness check uses SR subjects + `SHOW MODELS`). D-006 (delete retention)
is exercised once `ops-sim` produces to `service_deployments`.

## Results of your live run (2026-11-11)

- **Terraform apply (2a):** succeeded; topics and model present.
- **`service_deployments-key`** is registered as a **record**
  (`service_deployments_key { service_name: string }`, namespace
  `org.apache.flink.avro.generated.record`). `opsgen/kafka_io.key_payload` takes the
  record path for this, and `ops-sim` produced to the topic fine.
- **Deploy:** keys minted and card written; `✓ model llm_textgen_model`, but
  **`✗` for both source tables although they exist.** Known issue, not investigated by
  request: `.scratch/deploy-readiness-check/issues/01-source-tables-reported-missing.md`
  (local, gitignored). It makes `deploy` exit 1 and skip the start-ops-sim offer; fix
  before Phase 6.
- **Still open:** whether the `service_deployments` **value** schema includes
  `service_name`. Doesn't block anything (fastavro ignores extra dict keys); I'll read
  it from the registered schema in Phase 3 and align the `.avsc` copy if needed.

## 6. Questions for me

- When you deploy, does the readiness check pass (all three `✓`)? If `service_deployments`'
  key schema is a **record** vs a **primitive** string, the generator handles both, but
  please tell me which the Console shows for `service_deployments-key` — it confirms D-020
  and the key-shape path. Also: does the `service_deployments` **value** schema include
  `service_name` (the PK)? That confirms the `.avsc` note from Break 1.

## 7. Next phase

**Phase 3 — the pipeline up to `service_health`** (interactive, in your environment): the
`sql/` solution files for the temporal join (with the regular-join anti-pattern), the
window aggregation, the two-detector anomaly statement, and the `service_health` CTAS,
plus the detection half of `uv run evaluate`. We tune the ARIMA config together against
real data.
