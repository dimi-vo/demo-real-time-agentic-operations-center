# Decisions

Every decision this brief didn't make, and every place I departed from it, the
reference repo, or the current Confluent docs, is recorded here. Types:

- `DECISION` — a choice the brief left open.
- `ASSUMPTION` — something I believe but couldn't confirm (confirm at the next break).
- `DEVIATION` — I did something differently from the brief.
- `WORKAROUND` — a temporary fix; the proper fix is described.

Each entry states **what**, the **options**, the **choice**, **why**, and the **impact**.
Workarounds are also marked in code with `# WORKAROUND(D-0NN): ...`.

Assumptions awaiting confirmation are listed first in each break report.

---

## Phase 0

### D-001 · DECISION · Repository layout and Python package split
**What.** How to organize the new repo's Python code.
**Options.** (a) One flat package; (b) mirror the reference's split of a generator
package (`datagen/`) plus a tooling package (`scripts/`).
**Choice.** (b). A generator package `opsgen/` (the equivalent of the reference's
`datagen/`), a tooling package `scripts/` (deploy helpers, reset, setup-rtce,
dashboard, evaluate), `deploy.py` at the root, `platform-team/` for Terraform,
`sql/` for canonical SQL, `tests/` for cross-cutting tests. See PLAN.md §3.
**Why.** The brief says to mirror the reference layout; the split keeps the
offline-testable generator independent of the Confluent tooling.
**Impact.** Import paths and `pyproject.toml` entry points follow this layout.

### D-002 · DECISION · Generator package named `opsgen`
**What.** Name of the generator package (reference: `datagen`).
**Options.** `datagen`, `opsgen`, `simgen`, `generator`.
**Choice.** `opsgen`.
**Why.** "ops" matches the AIOps story and the `ops-sim` / `ops-wall` command names;
short and unambiguous.
**Impact.** `ops-sim = opsgen.cli:main`; tests live in `opsgen/tests/`.

### D-003 · DECISION · Resource naming prefix `RIVER-OOPS-DEMO` (confirmed at Break 0)
**What.** The fixed names the platform team gives the cluster and Flink pool, which
the deploy finds by name (reference: `RIVER-RACING-DEMO-CLUSTER` / `-pool`).
**Options.** `RIVER-SHOP-DEMO`, `RIVERSHOP-DEMO`, `AIOPS-DEMO`, `RIVER-OOPS-DEMO`.
**Choice (confirmed by the user at Break 0).** `RIVER-OOPS-DEMO-CLUSTER` and
`RIVER-OOPS-DEMO-pool`; environment named `<sanitized local part>-<4 hex>_env`;
credential-card prefix `DEMO` (`runs/pre-provisioned/credentials/DEMO.env`).
**The store/narrative brand stays "River Shop"** (the services, data, dashboard and
walkthrough copy, per brief §1) — only the Confluent cluster/pool resource names use
`RIVER-OOPS-DEMO` (a nod to the self-healing-IT "oops" story).
**Why.** User's choice at Break 0; "River Oops" fits the incident-and-remediation
narrative while keeping the brief's "River Shop" store.
**Impact.** `RIVER-OOPS-DEMO` appears in the platform module, `lookup.py`, the deploy
and the card; "River Shop" appears in the generator data and all attendee-facing
prose. If you actually want the store renamed to "River Oops" too, say so and I'll
change the narrative.

### D-004 · DECISION · CSP Hosted model default `anthropic.claude-sonnet-4-6` (confirmed at Break 0)
**What.** Default model behind `llm_textgen_model`.
**Options.** Keep the reference default `anthropic.claude-sonnet-4-6`, or a newer id.
**Choice.** Keep `anthropic.claude-sonnet-4-6` as the Terraform default
(`csp_hosted_model`, overridable).
**Why.** It's the reference's validated default; no reason to change blind.
**Impact.** Platform-team `variables.tf` default; the model must be offered in the
chosen region (confirmed with the Confluent account team, as in the reference).

### D-005 · ASSUMPTION · Region default `us-east-1`
**What.** Default AWS region for every developer's cluster and pool.
**Options.** Any of the ~11 AWS regions where RTCE/Lightning are available.
**Choice.** `us-east-1`.
**Why.** Reference default; it is on the RTCE/Lightning supported-region list
(docs, 2026-11): us-east-1/2, us-west-2, ap-northeast-1/2, ap-southeast-1/2,
ap-south-1, eu-central-1, eu-west-1/2.
**Impact.** `region` default in the platform module; README flags the AWS-only,
~11-region constraint for RTCE/Lightning (§5–§7 of the walkthrough).

### D-006 · DEVIATION · `service_deployments` uses delete retention, not compaction
**What.** Cleanup policy on the upsert `service_deployments` topic.
**Options.** (a) Compaction (the Confluent default for an upsert/keyed table, and
what the reference's `race_standings` uses); (b) delete retention.
**Choice.** (b): `'kafka.cleanup-policy' = 'delete'`, `'kafka.retention.time' = '24 h'`.
**Why.** The brief (§4.3) requires it, and it's correct: the temporal join and the
detector replay `service_deployments` from the earliest offset and must attribute
each historical request to the version that was live *at that request's event time*.
Compaction keeps only the latest record per `service_name`, so replayed old requests
would be mis-attributed to the current version. The reference tolerates compaction
only because each race overwrites all keys at lap 0 and never replays old versions;
this continuous workshop does replay them.
**Impact.** Diverges from the reference `race_standings`. Depends on Confluent Cloud
Flink accepting an `upsert` changelog table on a `delete`-cleanup topic — see D-016.

### D-007 · DECISION · `AI_RUN_AGENT` consumed as `ROW<status, response>`; agents continue on failure
**What.** How to call `AI_RUN_AGENT` and handle model failures over a day-long run.
**Options.** (a) Copy the reference, which reads only `response` and leaves the
agent at the default `handle_exception = FAIL`; (b) alias the full
`ROW<status, response>`, record `status`, and set `handle_exception = 'continue'`.
**Choice.** (b). `... LATERAL TABLE(AI_RUN_AGENT(...)) AS a(status, response)`, keep
`raw_response`, and create both agents `WITH ('handle_exception' = 'continue')`.
A row whose `status = 'FAILED'`, or whose parsed `action` is outside the agent's
allowed set, maps `action` to `INVALID`.
**Why.** The current docs show `AI_RUN_AGENT` returns `ROW<String status, String
response>`, and that the default `handle_exception = FAIL` stops the whole statement
on one bad call. The workshop's INSERT statements run all day, so one failed LLM
call must not kill them.
**Impact.** Improves robustness over the reference; `status` feeds the ≥95%-valid-action
acceptance check (§11).

### D-008 · DECISION · Lightning Query REST path `/query/v1`
**What.** The Lightning Query endpoint path.
**Options.** `/query/v1alpha1` (reference code) or `/query/v1` (current docs).
**Choice.** `/query/v1`, host `https://sql.<region>.aws.confluent.cloud`.
**Why.** Current Confluent docs (2026-11) use `/query/v1`; the reference's
`v1alpha1` predates GA. Direct-API Lightning queries are still EAP (the MCP/RTCE
path is GA), so `setup-rtce --lightning` keeps the reference's `api_not_enabled`
handling.
**Impact.** `setup-rtce --lightning` and `tests/test_lightning_queries.py` equivalent
use `/query/v1`.

### D-009 · CONFIRMED · `PARTITION BY service_name` inside `ML_DETECT_ANOMALIES` OVER works
**Status.** Confirmed by the user at Break 1: partition per service in one statement,
not separate statements per service.

**What.** One ARIMA model per service, via `PARTITION BY service_name` in the OVER clause.
**Options.** (a) Partition in one statement; (b) a separate statement per service.
**Choice.** (a), per the brief (§5.3).
**Why.** `PARTITION BY` in the `ML_DETECT_ANOMALIES` OVER clause is undocumented, but
the reference's live `enrichment_anomaly.sql` uses exactly `OVER (PARTITION BY
car_number ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
**Impact.** Verify in Phase 3. If unsupported, fall back to per-service statements
(a significant redesign) and raise it.

### D-010 · CONFIRMED · Two `ML_DETECT_ANOMALIES` calls in one statement
**Status.** Confirmed by the user at Break 1.
**What.** Latency and traffic detectors in a single statement (§5.3).
**Why.** Docs confirm multiple `OVER` aggregates are allowed when their windows are
identical; both detectors share the exact same `PARTITION BY service_name ORDER BY
window_time RANGE ...` window. Two `ML_DETECT_ANOMALIES` specifically coexisting is
not documented.
**Impact.** Verify in Phase 3; if rejected, split into two statements feeding
`service_health` via a join, and raise it.

### D-011 · CONFIRMED · `LAST_VALUE` for deployment context in the window aggregation
**Status.** Confirmed by the user at Break 1: use `LAST_VALUE`.
**What.** Carrying `version`, `last_change`, etc. "as of window end" (§5.2).
**Options.** `LAST_VALUE` (brief) or `MAX` (lexicographically wrong on version strings).
**Choice.** `LAST_VALUE`, verify in Phase 3; `MAX` fallback with a documented caveat.
**Why.** `LAST_VALUE` is a supported aggregate, but its use inside a window-TVF
aggregation and its ordering semantics there are undocumented. The risk only bites in
a window that contains a mid-window deploy (rare; deploys are infrequent).
**Impact.** Verify in Phase 3 against a known deploy window.

### D-012 · DECISION · Drop `driver_race_history` entirely
**What.** The static historical table.
**Choice.** No static table in this workshop (per the F1→AIOps mapping). Removes the
seed file, the Postgres/CDC path, `scripts/selfservice/seed.py`, and the deploy's
198-row check.
**Why.** The brief's mapping marks it "Dropped."
**Impact.** The deploy's readiness check becomes "both source tables and
`llm_textgen_model` exist" (§7.2). `test_platform_team_sync` loses the seed check and
gains the two source-table DDL drift checks.

### D-013 · DECISION · Docs site is hand-authored HTML, matching the reference (confirmed at Break 0)
**What.** The GitHub Pages site tooling (delegated to me: "match the reference repo").
**Options.** (a) Hand-author a single interactive HTML like the reference's
`pre-provisioned.html`; (b) add a markdown→HTML generator so the `.md` is the single
source.
**Choice.** (a), matching the reference's approach (self-contained HTML + `pages.yml`
copying it to `index.html`).
**Why.** The brief delegates docs-site tooling with "match the reference repo," and
the reference hand-wrote the HTML (no generator exists). A generator would be a
cleaner single-source design but is an explicit deviation; raising it as a Phase 6
option in PLAN.md rather than deciding silently.
**Impact.** Phase 6 effort; the `.md` and `.html` can drift (as in the reference).
Flagged for the user in PLAN.md §5.

### D-014 · DECISION · Initial ARIMA config per the brief
**What.** `ML_DETECT_ANOMALIES` config.
**Choice.** `minTrainingSize` 6, `maxTrainingSize` 360, `confidencePercentage` 99.9,
`enableStl` false, for both detectors; auto-ARIMA (no fixed p/d/q) initially.
**Why.** Brief §5.3. All values pass the documented validation (minTrainingSize > 1
and < maxTrainingSize; maxTrainingSize < 10000; 0 < confidence ≤ 100).
**Impact.** Tuned in Phase 3 with `uv run evaluate`; raise `minTrainingSize` to 12 or
fix p/d/q if the acceptance criteria (§11) aren't met. Tuning history recorded here.

### D-015 · DECISION · Single partition for both source tables
**What.** `DISTRIBUTED ... INTO 1 BUCKETS` on `service_requests` and `service_deployments`.
**Why.** Brief §4.4. Total volume is ~90 msg/s. Docs confirm a single partition avoids
any idle-partition watermark stall, and progressive idleness would protect a
multi-partition topic only while at least one partition keeps receiving data.
**Impact.** No throughput concern at this volume.

### D-016 · CONFIRMED · CC Flink accepts an `upsert` table on a `delete`-cleanup topic
**Status.** Confirmed by the user at Break 1 ("of course possible"). I'll still see it
succeed when the platform module's `CREATE TABLE` runs in Phase 2.
**What.** `'changelog.mode' = 'upsert'` + `'kafka.cleanup-policy' = 'delete'` together
(needed by D-006).
**Note for accuracy.** The reference `race_standings` (the only upsert table in the F1
repo, in `terraform/modules/topics/main.tf` and `platform-team/.../statements.tf`) sets
*no* `cleanup-policy`, so it defaults to `compact` — i.e. the reference is not itself an
example of upsert+delete, which is why this was raised. The combination is supported;
proceeding with delete retention per D-006.

---

## Phase 1

### D-017 · DECISION · `requires-python = ">=3.10,<3.13"`
**What.** The Python version range for the project.
**Options.** Match the reference's `>=3.10` (uncapped), or cap below a version.
**Choice.** `>=3.10,<3.13`.
**Why.** `uv` picks the newest allowed interpreter; capping below 3.13 guarantees
`confluent-kafka[avro]` and `fastavro` resolve to published binary wheels on every
platform (3.13/3.14 wheels lag). The generator itself is pure stdlib. uv selected
CPython 3.12.15 locally and the suite passes.
**Impact.** CI and developers run on 3.10–3.12. Lift the cap once the Kafka wheels
for newer Pythons are universally available.

### D-018 · DECISION · Separate RNG streams for the scheduler and for traffic
**What.** How `--seed` maps to randomness in the generator.
**Options.** (a) One rng shared by the incident/deploy scheduler and request
generation; (b) two independent streams derived from the seed.
**Choice.** (b): `sched_rng = Random(seed)`, `traffic_rng = Random(seed + 1)`.
**Why.** With a shared stream the schedule depended on how many requests were drawn,
so it shifted whenever traffic volume changed — fragile, and with seed 42 it starved
`UPSTREAM_DROP` to zero over 60 minutes, which would fail the brief's acceptance
command (`--simulate 60 --seed 42` must show all three signatures). Independent
streams make the schedule a pure function of seed + config; seed 42 now yields
5 BAD_DEPLOY / 2 TRAFFIC_SURGE / 3 UPSTREAM_DROP.
**Impact.** "The whole schedule is reproducible with `--seed`" (brief §4.5) holds
independent of traffic. The schedule stream equals `Random(seed)`, so the engine's
own unit tests (which seed `Engine` directly) match production scheduling.

### D-019 · DECISION · `--dry-run` writes nothing; persistence is a live-mode concern
**What.** Whether `--dry-run` reads/writes the persisted state and `incidents.jsonl`
under `runs/`.
**Choice.** `--dry-run` starts from a fresh in-memory state and writes nothing, so a
seeded dry run is byte-for-byte reproducible and never pollutes `runs/`. Persisted
state and the ground-truth log are written by live mode (Phase 2); `state.py` and
`groundtruth.py` are unit-tested directly now.
**Why.** Reproducibility of `--dry-run --seed N`, and not surprising a developer by
writing files during an offline run.
**Impact.** The brief's "persist state / append incidents.jsonl" responsibilities are
exercised by live mode in Phase 2 and by unit tests now.

## Phase 2b

### D-020 · DECISION · Deploy readiness check uses Schema Registry subjects + SHOW MODELS
**Status (Break 2b, live run).** The model check works; the table check does **not**: it
reported both source tables missing although they exist. Logged as a local issue,
`.scratch/deploy-readiness-check/issues/01-source-tables-reported-missing.md` (status
`ready-for-agent`, fix before Phase 6). Not investigated yet, at the user's request.
**What.** How `uv run deploy` checks "both source tables and `llm_textgen_model` exist"
(brief §7.2, replacing the reference's `driver_race_history` row count).
**Options.** (a) List Kafka topics via the cluster's Kafka REST endpoint; (b) check the
registered Schema Registry subjects; plus a Flink `SHOW MODELS` for the model.
**Choice.** (b) for the tables — a `<topic>-value` subject exists iff the `CREATE TABLE`
ran — and `SHOW MODELS` (one short Flink statement) for the model.
**Why.** The credential card doesn't carry the cluster's Kafka REST endpoint, and the SR
subject is a faithful, cheap proof the table's schema was registered. The model isn't a
REST resource, so a Flink statement is the natural check (as the reference used a Flink
`COUNT(*)` for its check).
**Impact.** The deploy needs the SR key (it has it) and one Flink call. Verified against a
real environment at Break 2b.
