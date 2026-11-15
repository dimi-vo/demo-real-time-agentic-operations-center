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
**Status.** Confirmed against the docs in Phase 4: `handle_exception` exists, takes
`CONTINUE` or `FAIL`, defaults to `FAIL`, and is set in the `WITH` clause of
`CREATE AGENT`. Not on the `CREATE AGENT` reference page; documented in the Agent
Runtime Guide. See D-033 for what else that page corrected.

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
not separate statements per service. **Verified live in Phase 3:** the `service_health`
CTAS runs, and each service gets its own model (separate warm-up, expected values and
bounds per service).

**What.** One ARIMA model per service, via `PARTITION BY service_name` in the OVER clause.
**Options.** (a) Partition in one statement; (b) a separate statement per service.
**Choice.** (a), per the brief (§5.3).
**Why.** `PARTITION BY` in the `ML_DETECT_ANOMALIES` OVER clause is undocumented, but
the reference's live `enrichment_anomaly.sql` uses exactly `OVER (PARTITION BY
car_number ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
**Impact.** Verify in Phase 3. If unsupported, fall back to per-service statements
(a significant redesign) and raise it.

### D-010 · CONFIRMED · Two `ML_DETECT_ANOMALIES` calls in one statement
**Status.** Confirmed by the user at Break 1. **Verified live in Phase 3:** accepted by
Confluent Cloud; both result structs are populated in every row after warm-up.
**What.** Latency and traffic detectors in a single statement (§5.3).
**Why.** Docs confirm multiple `OVER` aggregates are allowed when their windows are
identical; both detectors share the exact same `PARTITION BY service_name ORDER BY
window_time RANGE ...` window. Two `ML_DETECT_ANOMALIES` specifically coexisting is
not documented.
**Impact.** Verify in Phase 3; if rejected, split into two statements feeding
`service_health` via a join, and raise it.

### D-011 · CONFIRMED · `LAST_VALUE` for deployment context in the window aggregation
**Status.** Confirmed by the user at Break 1: use `LAST_VALUE`. **Verified live in Phase 3:**
accepted inside the window-TVF aggregation, and correct around deploys: auth's
`2.1.9 → 2.1.10` deploy at 09:48:29.32 shows `2.1.10`, `ssd = 0` in the window ending
09:48:30, and the window holding the rollback shows `2.1.9 ROLLBACK`. `evaluate` checks
this for every `BAD_DEPLOY` against the logged bad and good versions. Edge case by
construction: the version is that of the last request in the window, so a deploy in a
window with no later request for that service would not show (vanishingly rare).
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
**Impact.** Tuned in Phase 3 with `uv run evaluate`. **Superseded by D-029:** final config
is AR(1) (p=1, d=0, q=0), minTrainingSize 12, maxTrainingSize 360, confidence 99.9.

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

### D-020 · DECISION · Deploy readiness check uses Flink `SHOW TABLES` + `SHOW MODELS`
**Status (post-Phase-5 cleanup).** Live-reported false negative fixed; three source
tables (including `service_incidents` added in Phase 5) and the model are checked
through one Flink session.
**What.** How `uv run deploy` checks "all source tables and `llm_textgen_model` exist"
(brief §7.2, replacing the reference's `driver_race_history` row count).
**Options.** (a) List Kafka topics via the cluster's Kafka REST endpoint; (b) check
the registered Schema Registry subjects; (c) one Flink `SHOW TABLES` + `SHOW MODELS`
through the same `FlinkSession` the model check already used.
**Choice.** (c). One `SHOW TABLES` returns the set of table names in the developer's
catalog.database; `SHOW MODELS` returns the model name.
**Why.** The first attempt (b, SR subjects) was live-reported as broken at Break 2b:
`/subjects` returned an empty list and the whole call was caught, so the check printed
"both tables missing" for an environment that was fine
(`.scratch/deploy-readiness-check/issues/01-source-tables-reported-missing.md`). The
underlying cause was unclear (SR subject contexts, RBAC propagation, or a filter) but
Flink `SHOW TABLES` sidesteps all of it: one mechanism for tables and model, the same
auth path, and real errors surface as `?` with the HTTP status instead of being
silently reshaped into a false "missing".
**Impact.** The deploy's SR key is still used for Avro I/O; the readiness check no
longer touches it. One extra Flink statement per `deploy` run (negligible, same
pool/catalog as the existing model check). Six new tests
(`tests/test_preprovisioned_readiness.py`) cover the four shapes: all present, one
missing, table-check error (`?` not `✗`), model-check error.

## Phase 3
**Amended by D-050.** The two lab output tables (`service_health`, `remediation_decisions`)
are the platform team's now, so the check lists five tables: a missing output table says
"ask your platform team to re-run their apply" like a missing source table
(`provision.PLATFORM_TABLES`).

### D-021 · DECISION · How `uv run evaluate` reads and scores `service_health`
**What.** The data path and the scoring definitions for the detection half (brief §7.2).
**Options.** (a) Read `service_health` with a Flink `SELECT` over the statement API;
(b) consume the topic directly over Kafka with the card's Kafka key and the registered
Avro schema.
**Choice.** (b). The reader is `scripts/common/topic_reader.py`: manual assignment from
the beginning, no committed offsets, `read_committed`, stop at end of every partition. It
reads the whole topic (about 2,200 rows an hour), which also gives the topic's head for
the fresh-start measurement. `--dump`/`--rows` save and re-score a run offline;
`--topic` scores a tuning variant. Scoring definitions (in
`scripts/evaluation/detection.py`):
- *signal*: `LATENCY_HIGH` / `TRAFFIC_HIGH` / `TRAFFIC_LOW` from the status columns
  (the incidents' `expected_trigger` names); `WARMING_UP` rows carry none;
- *incident span*: the service's windows overlapping `[started_at, ended_at)`; the window
  containing `started_at` is window 1; *detected in time* = expected trigger in window
  1–3, *late* after that, else *missed*; an opposite traffic signal is flagged as wrong
  direction; for `BAD_DEPLOY`, rows are also checked to carry `bad_version` (and the
  rollback window `good_version`) — check removed by D-045;
- *false positive*: any signal outside every incident span of its service, tagged *echo*
  (within 60 s after an incident ended) or *after deploy* (deploy < 60 s earlier);
- *fresh start*: signals in the first 5 minutes after each service's first window, outside
  incident spans, counted separately from the FP rate;
- false positives are counted per *signal* (a row with two anomalous statuses counts
  twice); rows with two signals outside incidents were rare in practice;
- the default range ends 100 s before the newest row, because `ops-sim` logs an incident
  only when it ends, so the newest rows may belong to one not yet in `incidents.jsonl`.
- *restart artifacts* (added during tuning): signals in the last window before a gap in the
  data and in the minute after it are counted as `restart`, not as false positives. A
  stopped `ops-sim` really does drop every service's traffic in those windows;
- incident timing uses the *effective end*, `started_at + ceil(ended_at − started_at)`
  (D-028).
**Why.** Reading Kafka is read-only, starts no statement and costs nothing; the dashboard
(Phase 5) reuses the reader. Explicit definitions make the acceptance numbers reproducible.
**Impact.** `evaluate` needs the Kafka + SR keys from the card (it has them). Rows lag
by up to one Flink checkpoint because of `read_committed`; the 100 s tail covers that.

**Amended by D-057.** The *after deploy* tag is gone, and so is its column in `evaluate`'s
false-positive table. With no benign deploys, every deploy is a bad deploy (inside its
incident's span) or its rollback (at the incident's end, where the *echo* minute starts), so
the tag could no longer fire.

### D-022 · DECISION (raise at Break 3) · 2.1 adds per-version count queries; the anti-pattern reads new requests only
**What.** How the walkthrough makes the regular join's re-attribution visible (brief §5.1).
**Options.** (a) Only the plain per-request regular join, as in F1; (b) additionally a
`COUNT(*) … GROUP BY service_name, version` over each join
(`sql/2.1_regular_join_by_version.sql`, `sql/2.1_temporal_join_by_version.sql`).
**Choice.** (b). With the regular join, a deploy makes the service's old-version row
vanish and the new version take over every request counted so far (its
`first_request` predates the deploy). With the temporal join, the old row freezes with
`last_request` just before the deploy and a new row starts at the deploy time. Both
anti-pattern queries read `service_requests` from the **latest** offset.
**Why.** In a per-request result panel the re-attribution is hard to spot; one row per
version shows it at a glance and compares directly with the `ops-sim` console. The
latest offset matters because a regular join keeps every request in state, and every
5-second re-emit of `service_deployments` is an update that makes it retract and re-emit
all of that service's joined rows. From the earliest offset, with hours of history,
that's millions of rows per re-emit on the attendee's pool.
**Impact.** Two more short queries in 2.1 (user-visible flow; confirm at Break 3).

**Amended by D-058.** A record is now one second of one service, so the per-version
queries add up `SUM(traffic_count)` instead of counting rows; their columns are
`traffic_count`, `first_sample` and `last_sample`. Both anti-pattern queries read
`service_metrics` from the latest offset. The lesson is unchanged.

### D-023 · DECISION · Step structure and shared CTEs for 2.2–2.4
**What.** How the 2.x solution files are cut.
**Choice.** As in F1, each step has two parts: 2.2 = metrics only, then with deployment
context (`2.2_window_metrics.sql`, `2.2_window_aggregation.sql`); 2.3 = the two raw
detector results, then the status columns (`2.3_anomaly_detection.sql`,
`2.3_anomaly_status.sql`); 2.4 = the 2.3 status query wrapped in the CTAS. The `enriched`
CTE (the temporal join) is the same text in every file and hints **both** sources to the
earliest offset, including the 2.1 solution (F1 hinted only the versioned side; the brief
asks for explicit hints on both from 2.3 on, and one identical CTE is easier to copy).
`tests/test_sql_pipeline.py` enforces that the CTEs, the detector config and window, and
the CTAS body stay identical across files.
**Why.** F1 pedagogy; attendees grow one statement step by step.
**Impact.** `reset --with-labs` submits only `2.4_service_health.sql` (from Phase 2) (removed by D-051).
**Amended by D-050.** 2.4 is now `INSERT INTO service_health` in front of the unchanged 2.3
status query, not a CTAS: the table already exists, created by the platform team. The
test is `test_insert_body_is_the_23_status_query`. `reset --with-labs` is gone (D-051).
**Amended by D-054.** The 2.3 status query ends with `detected_at` (`CURRENT_TIMESTAMP` cast to
`TIMESTAMP(3)`), so attendees see the real-time delay before they write the INSERT and 2.4
stays the 2.3 query behind one line.

### D-024 · DECISION · Metric expressions and types in the window aggregation
**What.** Expressions the brief left open.
**Choice.** `error_count = SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)` (INT);
`error_rate = CAST(error_count AS DOUBLE) / COUNT(*)`; `avg_response_bytes =
AVG(CAST(response_bytes AS DOUBLE))` (DOUBLE; `AVG` of an INT would truncate to INT);
`request_count = COUNT(*)` (BIGINT), cast to DOUBLE only inside the traffic detector.
`window_time` is kept in the `windowed` CTE (the detectors' `ORDER BY`) but not written
to `service_health`. `owner_team` is not carried (not in the brief's column list).
**Why.** Plain SQL attendees can read; no integer-division surprises.
**Impact.** Column types of `service_health` follow from these.
**Amended by D-050.** The types of `service_health` are no longer left to the query: they
are declared in `sql/sink_service_health.sql`. The ones above are still what the DDL was
derived from, and the first `terraform apply` is the live check (only the two status
columns were ever observed).

**Amended by D-058.** No error expressions (`error_count`, `error_rate`) and no
`MAX(latency_ms)`. `traffic_count = SUM(traffic_count)` is an INT (nullable, like its source
column), cast to DOUBLE only inside the traffic detector; `avg_latency_ms = AVG(latency_ms)`
is the unweighted mean of the window's ten per-second samples.

### D-025 · CONFIRMED · Warm-up is detected as `is_anomaly IS NULL`; status columns come out as STRING
**Status (Phase 3, live).** `DESCRIBE service_health` shows `latency_status` as
`VARCHAR(10)` (variable length, no blank padding). `WARMING_UP` appears in each service's
first **7** windows (the first is a partial window at the generator's start), never later.
**What.** Two details of the status `CASE` expressions.
**Choice.** `WARMING_UP` when `<result>.is_anomaly IS NULL` (the docs and the F1 notes say
the result fields are NULL before `minTrainingSize` rows; a NULL ROW also yields NULL
fields). The `CASE` returns string literals of different lengths; I expect Flink to type
it as `STRING`, not a blank-padded `CHAR(10)`.
**Why.** Docs/F1 for the first; Flink's type system converts ragged CHAR unions to
VARCHAR for the second, but I haven't seen it in Confluent Cloud.
**Impact.** Verify with `sql/checks/service_health_describe.sql` and by `WARMING_UP`
appearing exactly in each service's first 6 windows. `evaluate` trims statuses anyway.
If they come out as `CHAR(10)`, wrap each `CASE` in `CAST(... AS STRING)`.
**Amended by D-050.** The status columns are declared `STRING` in the table's DDL, so the
"wrap each `CASE` in `CAST(... AS STRING)`" fallback is moot: the INSERT converts the
query's `VARCHAR(10)` implicitly, and a declared `VARCHAR(n)` is avoided on purpose
because Confluent Cloud truncates to it.

### D-026 · DECISION · Developer check queries live in `sql/checks/`
**What.** Where verification-only SQL lives.
**Choice.** `sql/checks/` with a README: an exact temporal-join attribution check (the
version hash in `instance_id` vs `SHA1` of the joined version; `SHA1` and `LEFT` temporal
joins are both in the current Confluent docs), deploy windows (`LAST_VALUE`), anomalous
rows, and `DESCRIBE service_health`.
**Why.** They prove acceptance criteria but aren't walkthrough steps; the drift test (Phase
6) and `reset --with-labs` (removed by D-051) read only top-level `sql/*.sql`.
**Impact.** None for attendees.
**Amended by D-050 and D-051.** `service_health_describe.sql` now shows the types declared
in the DDL, not derived ones. `reset --with-labs` no longer exists.

**Amended by D-058.** The attribution check (`temporal_join_attribution.sql`) is deleted:
a metrics sample has no `instance_id` to prove the version with.

### D-027 · DECISION · `.avsc` copies aligned with the schemas Confluent registered
**What.** Two Phase 1 assumptions about the registered schemas were wrong; checked
read-only against the live Schema Registry at Break 3.
**Found.** (1) `TIMESTAMP(3)` is registered as `local-timestamp-millis`, not
`timestamp-millis`. (2) `service_deployments-value` does **not** contain `service_name`;
the primary key is only in `service_deployments-key` (record
`org.apache.flink.avro.generated.record.service_deployments_key`).
**Choice.** `schemas/*.avsc` now match: `local-timestamp-millis`, `service_name` removed
from the deployments value, new `schemas/service_deployments_key.avsc`. The drift test
splits the DDL into PRIMARY KEY columns (key `.avsc`) and the rest (value `.avsc`).
**Why.** The copies exist to mirror what's registered.
**Impact.** None at runtime: live serialization always used the registered schemas
(fastavro ignores the extra `service_name` in the value dict, and epoch-millis ints
encode the same for both logical types). `evaluate` treats naive timestamps as UTC,
which is what `local-timestamp-millis` decodes to.

### D-028 · DECISION · Generator timestamps on an integer-millisecond tick grid; whole-second incident durations
**What.** Two timing defects in `ops-sim`, found in the live Phase 3 run.
**Found.** (1) The attribution check (`sql/checks/temporal_join_attribution.sql`) found 1
mismatch in ~530,000 requests. A read-only replay over Kafka pinpointed it: a checkout
request served by the *old* instance (`checkout-8aa51a-…`, 4.2.2) carried exactly the
millisecond of the 4.2.3 deploy record (delta 0 ms). Request times were `to_millis(now +
uniform(0, 1))`, a truncation of float seconds, so a request in the last ~0.6 ms of a tick
could land on the next tick's millisecond, where a deploy record may sit. Flink was right;
the data was wrong. (2) Incident durations were fractional seconds, but the generator acts
on whole-second ticks, so an incident actually ended (and a `BAD_DEPLOY`'s `ROLLBACK`
fired) at the first tick after the logged `ended_at`, up to 1 s later.
**Choice.** Ticks are `start_ms + 1000·k` (integer arithmetic). Each request gets
`tick_ms + offset` with `offset` in 0–999 ms, and deploy records carry `tick_ms`, so a tick's
requests can never share the next tick's millisecond. Durations are rounded up to whole
seconds, so `ended_at` is the tick where the incident really ends. Same RNG draws, so `--seed`
schedules are unchanged apart from durations rounding up. `evaluate` computes the effective
end (`started_at + ceil(ended_at − started_at)`) so logs written before this fix score correctly.
(3) After a restart, incident ids started again at `INC0001`, so `incidents.jsonl` had
duplicate ids. Live mode now continues numbering after the highest id in the file
(`groundtruth.last_incident_number`). The duplicates already in the 2026-11-11 run's file
stay; `evaluate` doesn't key on ids.
**Why.** The attribution check should be exactly zero, and the ground truth should be exact
for "closing the loop".
**Impact.** Takes effect when `ops-sim` restarts (state persists, no reset needed). Tests:
`test_tick_timestamps_never_collide_with_the_next_tick`,
`test_deploy_record_and_its_requests_share_the_new_version`,
`test_incident_ends_on_a_tick_at_the_logged_ended_at`.

**Amended by D-058.** A `service_metrics` record carries exactly `tick_ms + 999`, the end of
the second it covers, instead of a random offset in 0–999 ms: still after the tick's deploy
record and before the next tick's. The attribution check that found defect (1) is deleted.
The tests are now `test_records_sit_at_the_end_of_their_tick` and
`test_a_record_follows_the_deploy_record_of_its_tick`.

### D-029 · DECISION · ARIMA tuning history and final config (AR(1), minTrainingSize 12)
**What.** Tuning `ML_DETECT_ANOMALIES` against the acceptance criteria (brief §5.3, §11),
measured with `uv run evaluate` on the live run that started from a clean reset at
2026-11-11 09:08:33 UTC.

**Round 1 — baseline (D-014): auto-ARIMA, minTrainingSize 6, maxTrainingSize 360,
confidence 99.9.** CTAS created at 10:39 UTC over ~90 minutes of history; caught up to
live within 7 minutes. Last hour (09:44–10:44, 9 incidents):
- Detection: **9/9 within 3 windows** (BAD_DEPLOY 5/5, TRAFFIC_SURGE 2/2, UPSTREAM_DROP
  2/2), all on the right service and in the right direction. Bad deploys flag in window 1,
  drops in window 2, surges in window 1–2.
- Fresh start: **at most 1 per service** in the first 5 minutes (cart, checkout and
  web-gateway 1 each) → **meets target; minTrainingSize stays 6.**
- False positives: **8 in 6 service-hours, worst service 2/hour → misses the target (≤ 1).**
  Two sources:
  1. *Echoes* (3 of 8, plus the end windows of both surges): auto-ARIMA follows a level
     change within 2–3 windows (on auth's surge the forecast went 153 → 319 → 376 → 461
     while traffic sat at ~460). When the surge ends, the return to baseline is flagged
     `TRAFFIC_LOW` (138 vs lower bound 347), then the forecast overshoots downward (78) and
     baseline is flagged `TRAFFIC_HIGH`. After a rollback, the latency forecast
     extrapolated the drop to 1 ms and flagged normal latency as `HIGH`. In Phase 4 these
     would send `INVESTIGATE_UPSTREAM`/`ROLLBACK` recommendations for incidents that
     already ended.
  2. *Marginal latency exceedances* (5 of 8): normal windows just over the upper bound
     (for example payments 271 ms vs upper 270). The window mean is right-skewed (0.5% of
     requests are 4× outliers, and payments has only ~60 requests per window), so the
     upper tail is heavier than the bound assumes.
- Diagnosis matches the brief's hint: auto-ARIMA "follows level changes too quickly".

**Round 2 — wider bounds and a mean model (side by side on the same history).**
- `service_health_c9999` (auto-ARIMA, 99.99): barely helps. Echoes sit far outside any
  reasonable bound (auth: 138 requests vs lower bound 347), and one drop became late. Live
  comparison showed the bounds are symmetric, forecast ± z·σ: the 99.99/99.9 half-widths
  differ by about z(99.99)/z(99.9) = 1.18, so other confidence levels can be re-derived
  offline (`evaluate --rescale FROM:TO`).
- `service_health_mean`, `service_health_mean9999` (fixed **p=0, d=0, q=0**): **don't work
  in Confluent Cloud.** Both stalled at the first model fit (each service's 7th window):
  committed output stopped after the first window, and a `read_uncommitted` read showed the
  warm-up rows replayed again and again (246 rows for 42 windows). The statements kept
  reporting `RUNNING` with no exceptions. The docs allow p, d, q ≥ 0, so this is a
  **docs-vs-behavior gap**.
- Unrelated noise found while scoring: an `ops-sim` restart (10:58) makes every service's
  traffic drop in the windows around the gap, which the detectors rightly flag. `evaluate`
  now labels signals next to a data gap as `restart` and leaves them out of the rate.

**Round 3 — AR(1) (p=1, d=0, q=0, 99.9).** Works (caught up within ~5 minutes). Over
the full 2.1 hours since the clean start (19 incidents; restart artifacts excluded):

| config | detected ≤ 3 windows | false positives | per service-hour, avg / worst | echoes | fresh start, worst |
|---|---|---|---|---|---|
| auto-ARIMA, 99.9 (baseline) | 19/19 | 22 | 1.7 / 2.8 | 12 | 1 |
| AR(1), 99.9 (live) | 19/19 | 13 | 1.0 / 1.4 | 3 | 2 |
| AR(1), 99.95 (offline) | 19/19 | 8 | 0.6 / 1.4 | 2 | 2 |
| AR(1), 99.97 (offline) | 19/19 | 6 | 0.5 / 1.4 | 2 | 1 |
| AR(1), 99.99 (offline) | 18/19 | 4 | 0.3 / 1.4 | 1 | 1 |

Without differencing, the forecast stays at the service's normal level instead of chasing
the incident, so the return to normal is no longer an anomaly. Checkout stays at 1.4/h in
every config: its remaining false positives are mostly latency windows pushed up by the
generator's 4× outliers (0.5% of requests) at only ~80 requests per window, which suggests
a distribution question for the generator rather than a detector question.

**Final (chosen by the user at Break 3): AR(1), confidencePercentage 99.9,
minTrainingSize 12, maxTrainingSize 360, enableStl false**, in `sql/2.3_*.sql` and
`sql/2.4_service_health.sql`. Only brief-sanctioned changes: fixed p/d/q because
auto-ARIMA "follows level changes too quickly" (§5.3), and `minTrainingSize` 12 because
AR(1)'s fresh start gave checkout 2 spurious anomalies (> ~1; §5.3 says raise to 12). With
12 the detector's output starts after about 2 minutes, still before the first incident at
~3:10. ARMA(1,1) was skipped to save cost (user's call).
**Impact.** Incidents stay flagged for most of their duration instead of only at onset.
That means more agent calls per incident in Phase 4 (still proportional to incidents). Final
acceptance numbers come from re-creating `service_health` with this config (it replays
the history from the earliest offset).

**Round 4: final config on fresh data at the D-030 rates (~29 msg/s).** Clean `reset`, then
`ops-sim` from 16:07:46 UTC, `service_health` created right away. Scored at 16:47 (39
minutes, 6 incidents), from the real start (a few seconds of stray 14:04 data at the
topic head excluded):
- Pipeline: 236 rows per service, no duplicates, no gaps; `WARMING_UP` only in the first
  windows (until 16:09:20), never later.
- Fresh start: **0** spurious anomalies in every service's first 5 minutes (meets ≤ 1).
- Detection: BAD_DEPLOY 2/2 and TRAFFIC_SURGE 1/1 in window 1; **UPSTREAM_DROP 1/3**: cart
  caught in window 1, **checkout missed twice** (see D-030).
- False positives: 3 in 3.2 service-hours (payments 2, cart 1), **no echoes**. Too short a
  run to judge the per-hour rate (payments' 2 in 0.54 h read as 3.7/h).
- Surges now flag `LATENCY_HIGH` in every window too (latency ×1.5 is sustained and AR(1)
  doesn't absorb it). Routing (Phase 4) sends those rows to the capacity agent anyway.

**Amended by D-059.** `confidencePercentage` is 99.95 (tuned offline for the simplified
demo's 20 s spikes and ~60 incidents/h). AR(1), 12 and 360 stay. Surges no longer touch
latency, so the `LATENCY_HIGH` flags on surges above are gone.

### D-030 · DEVIATION · Request rates divided by ~3; slow outliers softened (chosen by the user at Break 3)
**What.** `ops-sim` request rates and the latency outlier rule (brief §4.1, §4.2).
**Why it came up.** The user saw the `service_health` statement hold the pool at 10 CFU
"for quite some time". The pool's CFU history (Metrics API, read-only) showed steady state
at about **1 CFU** (11:12–11:16, `service_health` alone). Every 10-CFU period was a newly
created statement **replaying history** from the earliest offset (~500–730k requests after
1.5–2.2 hours at ~86 msg/s), followed by several minutes of autoscaler scale-down. Replay
work scales with message rate × time since `ops-sim` started, so a later drop-and-recreate
costs more.
**Options (simulated offline with the generator's distributions and a mean ± z·σ detector
approximation that reproduces the live false-positive rates).**
- ÷10 (as first asked, ~9 msg/s): drops on cart/checkout/payments become undetectable (0–9%
  of windows), up to 29% of drop windows are empty (no row at all: "absence of data is
  invisible"), and latency false positives roughly triple (2.7–3.9 per service-hour).
- ÷10 with rebalanced services: drop services would need more traffic than web-gateway,
  which breaks the story's ranking.
- ÷4: payments drops caught within 3 windows only ~67% of the time.
- ÷3 + softer outliers: drops 95–100% within 3 windows; worst latency false positives
  ~0.9 per service-hour (better than the brief's rates with 0.5% × 4 outliers, ~1.5).
- ÷2 + softer outliers: more margin, only 2× cheaper replays.
**Choice (user).** ÷3, rounded: web-gateway 8, auth 5, catalog 7, cart 4, checkout 3,
payments 2 requests/s (~29 msg/s; the ranking is unchanged). Slow outliers 0.2% of requests
at 3× (was 0.5% at 4×).
**Impact.** About 3× less replay work on every CTAS creation and drop-and-recreate. Topic
volume and storage drop by about 3×. Incident signatures are unchanged in relative terms
(seeded `--simulate 60`: drops 19–20 → 3–4 per window, surges ~50 → ~150, bad deploys
~46 → ~180 ms with 8–15% errors). The schedule logic is untouched. The brief's "roughly 90
messages per second" becomes ~29 in the walkthrough. Phase 3 validation is re-run on fresh
data at the new rates (the old history at the old rates would distort the detector's
first hour).
**Superseded for the rates by D-031** (the softened outliers stay).

**Status after the live run (Break 3): drop detection on checkout is not good enough.**
Checkout's two drops were both missed. Its traffic bounds are ±22 around ~30 requests (the
model's spread is ~25% wider than pure Poisson noise, the same factor as at the old rates),
and AR(1)'s forecast moves ~20% toward a drop after its first window, so a drop to ~6
requests sits right at the lower bound (INC0005: 9, 4, 7, 6 against lower bounds 8, 4, 2, 2).
My offline simulation assumed ideal Poisson bounds and was too optimistic. Calibrated with
the live factors, a drop is caught reliably only with about 60 requests per window
(≈ 6 requests/s) on the drop candidates (cart, checkout, payments), which is what the brief's
original rates gave payments. **Proposed fix (needs the user's decision):** cart, checkout and
payments at 6 requests/s, web-gateway 10, catalog 8, auth 5 (~41 msg/s, about half the
brief's volume, web-gateway still the busiest). Alternatives: the brief's original rates, or
accept missed drops on low-volume services.

**Amended by D-056.** `auth`, `catalog` and `cart` are gone. The rates of the three kept
services are unchanged (web-gateway 10, checkout 6, payments 6 since D-031).

**Amended by D-058.** The rates are now the means of a per-second `traffic_count`. The
topic carries three messages per second (one per service) whatever the rates, so the
replay cost that motivated this decision no longer grows with them. The softened outliers
apply to the per-request draws a sample's `latency_ms` averages.

### D-031 · DEVIATION · Drop services at 6 requests/s, aligned generator start, chosen with an offline tuning harness
**Status.** **Confirmed live on 2026-11-12.** After a fresh `reset` at 09:31, the
`UPSTREAM_DROP` on `checkout` was detected in window 2 (13 requests against an expected
59, lower bound 30). That is the same service and incident type that was missed 0/2 at
the D-030 rates.
**What.** How we fixed drop detection after the live Break 3 run (checkout's drops 0/2 at the
D-030 rates), and the tool used to choose.
**The tool (`scripts/tuning/`, developer-only).** Instead of a 60-minute live replay per idea:
- `arima.py`: a dependency-free replica of the AR(1) detector. It is OLS on a sliding
  window of up to 360 points, re-fitted on every row (Confluent's spread and forecast change
  on every row), with the first output after `minTrainingSize + 1` points, bounds at
  forecast ± z·σ, and forecast and bounds rounded to whole numbers for whole-number series
  (Confluent does this for `request_count`).
- `calibrate.py`: feeds the recorded per-window inputs from a `service_health` dump through
  the replica and compares it with what Confluent stored. On the final run: forecasts
  within 0.2% (latency) and 0–1.5% (traffic); statuses agree on 97–100% of windows per
  service and metric; **on the drop services' traffic (cart, checkout, payments) the bands
  are within 0–12% and statuses agree 99.7–100%.** Documented limitation: after a huge jump
  on a series (a bad deploy's ×4 latency, a surge's ×3 traffic), Confluent's spread recovers
  faster than a residual standard deviation does (its estimator isn't documented), so there
  the replica's bands stay 10–50% wider for a while and it slightly under-predicts
  post-incident false positives.
- `sweep.py`: runs the real generator (`opsgen.Simulation`, rates and multipliers overridden)
  for hours of simulated time per seed, aggregates the requests into Flink's 10-second
  windows, runs the replica, and scores with `scripts/evaluation/detection.score`, the same
  code as `evaluate`. 24 simulated hours (8 seeds × 3 h, 61 drops) take ~10 s on 8 cores.
**Findings (24 simulated hours per row).**

| variant | msg/s | drops detected ≤ 3 windows | false positives per service-hour, avg / worst |
|---|---|---|---|
| brief rates | 87 | 61/61 | 0.70 / 0.95 |
| D-030 rates, live-like start | 29 | 47/61 (payments 10/21) | 0.74 / 0.87 |
| drop services 5/s, aligned start | 36 | 59/61 | 0.71 / 1.00 |
| **drop services 6/s, live-like start** | **41** | **60/61** | **0.63 / 0.91** |
| **drop services 6/s, aligned start** | **41** | **61/61** | **0.58 / 1.04** |
| D-030 data, confidence 99 | 29 | 49/49* | 3.50 / 4.73 |
| D-030 data, confidence 99.5 | 29 | 47/49* | 2.13 / 2.60 |
| D-030 data, maxTrainingSize 180 | 29 | 39/49* | 0.99 / 1.27 |
| D-030 data, AR(2) | 29 | 41/49* | 1.26 / 1.50 (69 echoes) |

\* first sweep, 18 simulated hours. Detector-side changes either don't help or multiply false
positives, so the fix is on the data side. A drop is reliably visible only with ~60 requests
per window on the drop services.
**Also found.** `ops-sim` started at an arbitrary moment, so its first 10-second window was only
partly filled (live: 3.5 s of traffic) and looked like a drop. It sits inside warm-up (no
status) but stays in training for up to an hour and widens the bounds. A few seconds of stray
data at the head of the live topic made it worse.
**Choice (user, Break 3).** Rates: web-gateway 10, catalog 8, auth 5, **cart 6, checkout 6,
payments 6** requests/s (~41 msg/s; web-gateway stays the busiest). `ops-sim` (live and
`--dry-run`) now starts on the next 10-second boundary (`opsgen.driver.aligned_start`, waits
at most 10 s), as the F1 race aligned its laps to 20-second boundaries. Outliers stay at
0.2% × 3 (D-030). The detector config is unchanged (D-029).
**Impact.** Volume and replay work ~1.4× the D-030 rates, still about half the brief's. To be
confirmed in one live run (fresh `reset` → `ops-sim` → `service_health`, then `evaluate`).

**Amended by D-056.** Three services: the drop services are now two (`checkout` and
`payments`, both still at 6 requests/s) and the total is ~22 requests/s instead of ~41. The
tables above were measured on six services and are not re-run. `sweep.py`'s rate profiles
keep only the kept services (brief ~39, D-030 ~13, 5/s ~19) and its drop columns are
`chkout` and `paymt`.

**Amended by D-058.** `sweep.py` builds each window from the per-second samples
(`SUM(traffic_count)`, unweighted `AVG(latency_ms)`), and its `msg/s` column is `req/s`
(messages are always three per second). The windows have the same statistics as the
per-request ones (`.scratch/simplify-demo/research/ps_compare.out`), so the findings above
carry over.

**Amended by D-059.** The traffic means are 50/30/30 (the drop services well above the
~6 requests/s floor found here). `sweep.py`'s current profile is ~110 requests/s and its
`CURRENT` detector uses confidence 99.95.

---

### D-032 · DECISION · What `uv run evaluate` counts as a good decision
**What.** How the decision half of `evaluate` scores `remediation_decisions` (brief §7.2
and §11). The detection half is D-021.
**Options.** (a) Count matching actions per decision row; (b) score each incident by the
decision on its first anomalous window, and report the rest as context.
**Choice.** (b), plus four checks the brief asks for by name:
- **Valid action**: the parsed `action` is one the agent is allowed to take. The SQL maps
  a failed call or an out-of-set answer to `INVALID` (D-007), so everything else is valid.
  Target ≥ 95%.
- **Routing**: `LATENCY_HIGH` belongs to the release agent, `TRAFFIC_HIGH`/`TRAFFIC_LOW`
  to the capacity agent; and no (service, window) may appear under both agents.
- **Coverage**: every anomalous `service_health` row in range should have exactly one
  decision and no other row any, which is the brief's "agent calls track anomalies, not
  windows" check. Rows in the last minute of the range are counted as in flight, not
  missing, because the model call takes a few seconds.
- **Per incident**: the **headline** decision is the one on the earliest window of the
  span — the first recommendation the on-call team would see — and it matches when its
  action equals the incident's `expected_action`. A `ROLLBACK` must name the incident's
  `good_version` (since D-045: the health row's `previous_version`); a `SCALE_OUT` must
  carry the replica count the SQL computed (recomputed in Python from the row's stored
  context). Later windows are reported as an action mix,
  and a late match is marked, not counted.
**Why.** An incident spans several windows and AR(1) keeps it flagged for most of them
(D-029), so counting rows would weight long incidents more heavily and hide what matters:
what the team is told first. Scoring the headline keeps one number per incident.
**Impact.** `scripts/evaluation/decisions.py`, rendered by `evaluate`; new flags
`--decision-rows`, `--dump-decisions`, `--decisions-topic`, `--no-decisions`. The decision
half is skipped silently when `remediation_decisions` doesn't exist yet.
**Amended by D-050.** The table always exists (Terraform creates it), so the decision half is
skipped, with a message, when `remediation_decisions` exists but is empty, not only when it
is missing.

### D-033 · DEVIATION · Agent SQL follows the docs, not the reference, on three points
**Status.** **Confirmed live on 2026-11-12**: both agents and both INSERT statements were
accepted and ran, so the quoted agent name and the three `WITH` options are right.
Points 1-3 stand. The prompt-level choices below were partly undone by
D-034: no `request_id`, no `TRY_CAST`, and targets are stored as the agent wrote them.

**What.** Three places where the live F1 reference and the current Confluent docs
(checked 2026-11-11) disagree about `CREATE AGENT` / `AI_RUN_AGENT`.
**Findings and choices.**
1. **Agent name in `AI_RUN_AGENT`**: the reference passes a backticked identifier
   (`` AI_RUN_AGENT(`pit_strategy_agent`, …) ``); the docs type the first argument as
   `STRING` and quote it in all 16 examples. → **Quoted string**, `'release_agent'`. The
   name stays an identifier in `CREATE AGENT`, as the docs show.
2. **Failure handling**: the reference sets only `max_iterations` and takes the default
   `handle_exception = FAIL`, which ends the statement on one bad model call. → Both
   agents set `'handle_exception' = 'continue'` (D-007). `max_consecutive_failures` is
   set explicitly to 3 because the two doc pages contradict each other on its default
   (1 in the Agent Runtime Guide, 3 in the `CREATE AGENT` reference), and
   `'request_timeout' = '60'` so a hung call fails fast rather than after the default
   600 s.
3. **`max_iterations` is not set**: it bounds tool-calling iterations, and these agents
   have no tools, so it has no effect (documented for reflection sub-agents, and it
   follows from its definition).
**Also decided here** (the brief delegates the prompt wording):
- The prompts contain **no apostrophe**. No escaping rule for a literal `'` inside
  `USING PROMPT '…'` is documented, and not one of the ~37 example prompts in the docs
  contains one. A test enforces this.
- Nullable context (`previous_version`, `last_change`) is wrapped in `COALESCE(…, 'N/A')`
  before `CONCAT`, so one null cannot blank the whole agent input.
- `TRY_CAST` parses `Target Replicas`: a non-numeric answer yields null instead of
  failing the statement.
- `target_version` is kept only for `ROLLBACK` and `target_replicas` only for
  `SCALE_OUT`, so the enum and its targets stay consistent for "closing the loop"
  (brief §9). The unparsed answer is always in `raw_response`.
- Each call passes a **request_id** (`service@window_end`), which the docs use to
  correlate a call with its row in the agent logs.
**Why.** The brief (§10) says to follow the current docs where they disagree and to say
so. The reference is live-proven on point 1, so the identifier form probably also works;
the string form is what is documented.
**Impact.** `sql/4.2_*.sql`, `sql/4.3_*.sql`, guarded by `tests/test_agent_sql.py`. To be
confirmed in your environment when the statements run.

### D-034 · DEVIATION · `remediation_decisions` cut to 8 columns and the agent answer to 3 lines
**What.** The brief (§5.5) fixes a 20-column `remediation_decisions` and a six-line agent
answer. Both are cut down.
**Options.** (a) Build the brief's shape; (b) keep only what a decision *is* — what
happened, who answered, what to do — and read everything else from `service_health`.
**Choice.** (b), decided with the user in a design review at Break 4. Specifically:
- **Table: 8 columns.** `window_end`, `service_name`, `agent_name`, `trigger`, `action`,
  `target`, `reason`, `raw_response`. The nine context columns (`version`,
  `avg_latency_ms`, `request_count`, replicas, …) are gone: `evaluate` and the dashboard
  both already read `service_health` and join on `(service_name, window_end)`.
- **Answer: 3 lines.** `Action`, `Target`, `Reason`. `Target Version` and
  `Target Replicas` collapse into one free-text `target`; `Summary` and `Reasoning`
  collapse into one or two sentences; `Confidence` is dropped.
- **One shared action vocabulary**: `ROLLBACK`, `SCALE_OUT`, `INVESTIGATE_UPSTREAM`,
  `ESCALATE`, `OBSERVE` (`OBSERVE` replaces the brief's `WATCH`), plus `INVALID` (`ESCALATE`
  later removed by D-043). Each
  prompt still offers only its own agent's subset, and each INSERT validates against that
  subset, so the two statements differ only in the agent, the filter and the list.
- **The target is stored as the agent wrote it** (with `N/A` mapped to null). The SQL no
  longer blanks a target that doesn't match the action; `evaluate` flags the
  contradiction instead, so a misbehaving model is visible rather than tidied away.
- **Dropped from the INSERTs**: the `request_id` argument, `TRY_CAST` (the target is a
  string now), the two-level parse, the nulling `CASE`s, and the explicit column list in
  the capacity leg. **Kept**: the filter below the `LATERAL` join, the latest-offset
  hint, `handle_exception = continue` with the `INVALID` mapping, `COALESCE` on nullable
  prompt context, and `suggested_replicas` computed in SQL and handed to the agent.
- **Prompt inputs trimmed** to the fields the frameworks actually read.
**Why.** This is workshop code, and the brief's table stored the same numbers twice. Each
INSERT drops from about 100 lines to about 50, which matters because attendees read them.
The §11 acceptance checks survive: the action enum is still validated (so "≥95% valid" is
measurable), and §9's "closing the loop" still has a stable schema, a validated enum and
an explicit target — untyped rather than typed.
**Impact.** `sql/4.2_*.sql`, `sql/4.3_*.sql`, `scripts/evaluation/decisions.py`,
`scripts/evaluate.py`, the Lightning example in `scripts/setup_rtce.py`, and the tests.
`HealthRow` gained `ready_replicas` and `max_replicas`, which the decision half needs to
recompute the replica suggestion. The walkthrough (Phase 6) must describe the three-line
format, not the brief's six.
**Amended by D-050.** The 8-column table is created by the platform team's Terraform
(`sql/sink_remediation_decisions.sql`, with `IF NOT EXISTS`, delete cleanup and 24 h
retention); lab 4.3 keeps only the two INSERTs. Its columns are unchanged.

### D-035 · DEVIATION · Ops wall is dark-only
**What.** Brief §7.3 asks for "readable in light and dark mode"; the dashboard ships
dark-only.
**Options.** (a) Dual-theme: re-step every surface (truth purple, detect cyan, crit/warn/
serious/good, hatch, chart bed, pulse) against the light surface and validate each pair
against the light-mode gates; (b) dark-only now, light later if an attendee asks.
**Choice.** (b), explicit at Break 5 planning (Q35).
**Why.** The design the user picked (`.scratch/mockups/variant-e-ops-wall-dark.html`) is
a dark-first wall — cool `#0A1120` surface, pulsing crit stripe, hatched purple/cyan on
near-black — that trades its coherence for the dual-theme flex. Dual-theme is ~half a
day of design-system work; the method supports it, so reinstating it later is a pure
colour swap.
**Impact.** `scripts/opswall/static/opswall.css` carries no light-mode block. Record kept
so future me / the next contributor doesn't quietly add a `prefers-color-scheme: light`
and think they've finished the job.

### D-036 · DECISION · Ops wall chart is a per-metric truth-vs-detection mirror
**What.** Each service row has two inline charts, Latency and Traffic. In each chart:
up = the simulator-injected incident (purple, from `service_incidents`); down = the
pipeline detection span (cyan, from `service_health`); zero line = agent decisions as
shape-coded pins; detection-lag bracket in the down lane.
**Options.** (a) Brief §7.3's six tiles with a latency sparkline each, plus two separate
anomaly / AI panels; (b) one chart per service with the mirror by metric (my initial
Variant A–C sketches); (c) per-metric mirror by source (Variant D → E), one row per
service with two charts.
**Choice.** (c), locked at the end of grilling (Q31 + Variant E file supplied by the user).
**Why.** The brief's sparkline encodes the same thing the detection bar does, so the
tile keeps text only. The mirror by metric collapses "did we catch this" into a single
chart per service, losing the per-metric comparison the user asked for in Q27. The mirror
by source (truth up, detection down) makes the detection lag a *visible horizontal
offset* in a single chart per metric, which is the thing the dashboard exists to show.
**Impact.** `scripts/opswall/static/{opswall.css,opswall.js}` implement this shape;
`scripts/opswall/state.py` emits axis-relative seconds for every span so the renderer
doesn't do arithmetic with absolute timestamps.
**Amended by D-054.** The lower half is no longer the flagged windows in data time: it is one
block per anomaly run, from the `detected_at` of its first window to the write time of the
agent's first decision for it. The decision pins are gone from the chart (the block's tooltip
names the action), so is the warm-up band, and the state computes the verdicts and the summary
the page used to count.

### D-037 · DECISION · New `service_incidents` topic, two records per incident
**What.** Simulator ground truth is published to a Kafka topic (`service_incidents`),
not just the `incidents.jsonl` file. One record at `degradation_start` (`ended_at` null)
and one at `degradation_end` (both timestamps set), tied by `incident_id`. Append-only,
keyed by `service_name`, 9 columns: `event_time`, `incident_id`, `service_name`,
`incident_type`, `started_at`, `ended_at`, `expected_trigger`, `expected_action`,
`good_version` (8 columns since D-045 removed `good_version`).
**Options.** (a) Platform-team Terraform resource, same shape as `service_requests` /
`service_deployments`; (b) attendee creates the topic in the workshop; (c) generator
creates the topic over the admin API.
**Choice.** (a), decided at Break 5 planning (Q2, Q14). Two records (not one) per
incident, decided at Q3 so the dashboard can draw an "in progress" bar as soon as the
incident starts, not only after it ends.
**Why.** (a) keeps the schema story identical to the other two source tables. Two records
unlock live rendering; one record would make the dashboard an archaeologist, not a wall.
**Impact.** `sql/source_service_incidents.sql` (source of truth), `schemas/
service_incidents.avsc`, `platform-team/modules/developer/statements.tf` (third
CREATE TABLE heredoc, model statement's `depends_on` moves to it so the chain stays
linear). Generator emits via `opsgen/kafka_io.py::emit_incident`. `reset.py` purges it on
every reset. **Pending: `terraform apply` on your environment to register the subject.**

### D-038 · DECISION · `evaluate` keeps reading `incidents.jsonl` (superseded by D-046)
**What.** The decision scorer in `scripts/evaluate.py` stays on the file, not the topic.
**Why.** The generator writes both from the same `Incident` object so they can't disagree;
`evaluate` was live-validated on 2026-11-12 against the file; rewriting a working loader
buys nothing this phase. If and when the dashboard becomes the canonical source, flipping
`evaluate` to the topic is a one-function loader swap.
**Impact.** None to the evaluator. The ground-truth file keeps being deleted on
`uv run reset` step 5.

### D-039 · DECISION · Dashboard reads 10 min on start, not 15
**What.** Brief §7.3 says "15 minutes ago by timestamp". The dashboard reads 10 min.
**Why.** The chart is 10 min wide; a 15-min startup read fills five minutes of history
that will never be shown, for a quarter more Kafka reads per launch. One constant
(`STARTUP_BACKREAD_SECONDS` in `consumer.py`) used by every topic consumer.
**Impact.** `scripts/opswall/consumer.py`. The Phase 6 walkthrough must say "last 10 min".

### D-040 · DECISION · IBM Plex Sans vendored, no CDN
**What.** Three WOFF2 files (latin subset, weights 400/500/600, ~60 KB total) sit under
`scripts/opswall/static/fonts/` and are declared via local `@font-face` rules.
**Options.** (a) Load from Google Fonts at render time; (b) fall back to `system-ui`
everywhere; (c) vendor the files locally and serve them from the dashboard itself.
**Choice.** (c), decided at Q36.
**Why.** Conference wifi blocks CDNs regularly; `system-ui` is a different metric than
Plex (proportions shift, numeral tables land differently). Vendoring means every
attendee's screen matches the mockups and the dashboard has no third-party dependency at
render time.
**Impact.** `scripts/opswall/static/fonts/` carries the WOFF2s and a `LICENSE.md` (OFL).
`opswall.css` starts with three `@font-face` blocks pointing at `/static/fonts/*.woff2`.

### D-041 · DECISION · No separate "progressive unlock" panels
**What.** Brief §7.3 describes an anomaly panel that unlocks when `service_health` has
rows and an AI panel that unlocks when `remediation_decisions` has rows.
**Choice.** The chart is drawn from the first render; empty topics just leave empty
lanes. No separate panels, no reveal pattern.
**Why.** Decided during grilling (Q29). In a design where every chart is a per-metric
mirror, the "unlock" happens inside the chart (the detection lane fills in) rather than
by revealing whole panels. One fewer piece of state and one fewer thing the walkthrough
has to explain.
**Impact.** `scripts/opswall/static/` has no unlock logic. The Phase 6 walkthrough must
describe the chart's lane-by-lane fill-in, not the brief's progressive-reveal pattern.

### D-042 · DEVIATION · Incidents start at once, last 40 s, sit on window boundaries; 7-min cooldown
**What.** The brief (§4.5) has a BAD_DEPLOY degrade 10 s after its deploy, incidents last
40–70 s (uniform) and start at any second, and a service waits 15 min between incidents.
All four change.
**Options.** (a) Keep the brief's timing; (b) make the cause and the anomaly coincide and
make every incident the same, whole-window shape.
**Choice.** (b), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`):
- **No onset delay.** A BAD_DEPLOY's DEPLOY record and its degradation start on the same
  tick. The "onset" requests (new version, baseline metrics) no longer exist.
- **A fixed 40 s duration.** `Config.duration` replaces `duration_min`/`duration_max`;
  `--duration` replaces `--duration-min`/`--duration-max`.
- **Window-aligned starts.** The end of the quiet period and every spacing are rounded up
  to the next 10-second boundary (epoch seconds, like Flink's tumbling windows), so an
  incident covers exactly 4 whole windows and ends — and a BAD_DEPLOY rolls back — on a
  boundary. Benign deploys stay unaligned: they don't move the metrics.
- **Cooldown 900 → 420 s.**
**Why.** This is a workshop, and the point it makes is "anomaly → agent reacts → anomaly
gone". With the delay, attendees saw a deploy, then 10 s of nothing, then the anomaly.
With unaligned starts the first and last windows were half degraded and flagged or not
by chance — the fuzzy edges on the charts. Now an anomaly starts on the same window as
its cause, and every incident has the same 4-window shape. The shorter cooldown keeps
repeats possible on the 3-service surge/drop lists now that incidents are short.
**Impact.** `opsgen/engine.py` (`WINDOW_SECONDS`, `next_window_boundary`, `Config`; the
BAD_DEPLOY deploy and the start events fire in `_maybe_start_incident`; `Incident.t0` and
the `_deploy_fired`/`_start_fired` flags are gone), `opsgen/cli.py`, `opsgen/driver.py`
(`aligned_start` reuses `next_window_boundary`), and the tests. Dropping the duration
draw changes the RNG sequence, so `--seed` schedules differ from earlier runs. The
detector, request rates (D-030/D-031) and spacing are unchanged.

**Amended by D-056.** The surge and drop lists are now one service (`web-gateway`) and two
(`checkout`, `payments`). Under this schedule surges fall from about 26% to 17% of
incidents, because `web-gateway`'s cooldown often blocks one.

**Amended by D-057.** There are no benign deploys any more, so "Benign deploys stay
unaligned" no longer applies: every deploy and rollback sits on a window boundary.

**Amended by D-059.** The fixed duration is 20 s: an incident covers 2 whole windows. The
detector flags a spike in its first window; more windows only add after-spike false alarms.

**Amended by D-060.** The single slot is gone: every service runs its own schedule, so
incidents on different services overlap (at most one per service). The spacing and the
420 s cooldown are replaced by a 20 s gap per service and per-type rests (`BAD_DEPLOY` 390 s,
`TRAFFIC_SURGE` 300 s, `UPSTREAM_DROP` 300 s); the first three incidents are one of each
type; the quiet period is 150 s, tied to the detector's warm-up. Starts still sit on window
boundaries. `--duration` and the other timing flags are removed: timing lives in `Config`.

### D-043 · DEVIATION · One answer per incident type: `ESCALATE` and `max_replicas` removed
**What.** The brief puts `auth` at its replica ceiling (ready = max = 4) so a surge on
`auth` expects `ESCALATE`, and gives the release agent `ESCALATE` for "latency is high but
no recent deploy explains it". Both go, along with `max_replicas`.
**Options.** (a) Keep `ESCALATE` and the ceiling; (b) raise `auth`'s ceiling so it never
binds but keep the field and the rule; (c) remove the ceiling and `ESCALATE` everywhere.
**Choice.** (c), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`). Every incident type now has exactly one correct
answer, whatever the service:
- `BAD_DEPLOY` → `ROLLBACK` (Target = the previous version) on any of the six services;
- `TRAFFIC_SURGE` → `SCALE_OUT` (Target = Suggested Replicas) on `web-gateway`, `catalog`
  and `auth`;
- `UPSTREAM_DROP` → `INVESTIGATE_UPSTREAM` on `cart`, `checkout` and `payments`.
The vocabulary is `ROLLBACK`, `SCALE_OUT`, `INVESTIGATE_UPSTREAM`, `OBSERVE`, plus
`INVALID`. The release agent's framework is rule 1 `ROLLBACK` (unchanged conditions),
rule 2 `OBSERVE` for everything else; its "never OBSERVE when Error Rate Breach is true"
rule goes, since nothing else is left to pick. The capacity agent loses its `ESCALATE`
rule and the "Ready Replicas is less than Max Replicas" condition on `SCALE_OUT`; its other
rules keep their order and wording. Suggested Replicas is
`GREATEST(ready + 1, CEIL(ready × count / expected))`, with no `LEAST(max_replicas, …)`.
`OBSERVE` stays as the answer to a false alarm (detection on normal traffic).
**Why.** The workshop's point is "anomaly → agent reacts → anomaly gone", and it is easier
to follow when each incident has one concrete action. After D-042 a bad deploy degrades on
its own tick, so the release agent's `ESCALATE` could only ever answer a false alarm; and
the capacity agent's `ESCALATE` existed only for `auth`'s artificial ceiling. With no
environment running, removing the field costs nothing to roll out.
**Impact.** `max_replicas` is gone from `opsgen/` (`Service`, `DeploymentState`, the
deployment record), `schemas/service_deployments.avsc`, `sql/source_service_deployments.sql`
and its copy in `platform-team/modules/developer/statements.tf`, the 2.2–2.4 statements and
`service_health`, `evaluate` (`HealthRow`, `suggested_replicas`) and the ops wall (state,
Replicas tile). `ESCALATE` is gone from both prompts, both INSERTs, the
`remediation_decisions` comment, `evaluate`, the ops wall (closer set, pin, legend, status
label) and the mock (the `auth` scene; issue 06 rebuilds the rest). Environments must be
recreated: `service_deployments` changes shape.

**Amended by D-056.** Three services: `BAD_DEPLOY` → `ROLLBACK` on all three,
`TRAFFIC_SURGE` → `SCALE_OUT` on `web-gateway` only, `UPSTREAM_DROP` →
`INVESTIGATE_UPSTREAM` on `checkout` and `payments`.

### D-044 · DECISION · `avg_response_bytes` and `desired_replicas` no longer carried through the pipeline
**What.** The 2.2–2.4 statements computed `avg_response_bytes` and carried
`desired_replicas` from the deployment record into `service_health`. Nothing downstream
reads either: not the agents, not `evaluate`, not the ops wall.
**Options.** (a) Keep them for completeness; (b) drop them from the SQL and keep the raw
message fields; (c) drop the raw fields too.
**Choice.** (b), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`). `response_bytes` stays in `service_requests` and
`desired_replicas` in `service_deployments` — in the simulator, the `.avsc` files and the
source DDL — because they make the messages look real and cost no SQL.
**Why.** Attendees read the 2.x statements line by line; every column there should matter
to something they will see later.
**Impact.** `sql/2.2_window_metrics.sql`, `sql/2.2_window_aggregation.sql`,
`sql/2.3_anomaly_detection.sql`, `sql/2.3_anomaly_status.sql`,
`sql/2.4_service_health.sql` (and so the `service_health` schema), and
`tests/test_sql_pipeline.py`, which now guards against either column coming back.

**Amended by D-058.** `response_bytes` is gone with the per-request records. The guard
test now keeps `error_count` (display only) and `desired_replicas` out of labs 2.2–2.4,
and checks that both stay in their source tables.

### D-045 · DECISION · No versions in the ground truth; `ROLLBACK` targets scored against `service_health`
**What.** The ground truth carried `good_version` (topic and file) and `bad_version`
(file only). Three things used them: `evaluate` checked a `ROLLBACK` target against
`good_version`; `evaluate`'s version-mismatch check compared each incident window's
`version` with `bad_version`/`good_version`; the ops wall stored `good_version` and never
showed it. The agent never sees either — it reads `Previous Version` from `service_health`.
**Options.** (a) Add `bad_version` to the topic so `evaluate` can switch to it (D-046);
(b) drop the version-mismatch check and judge `ROLLBACK` targets against the health row
the agent answered; (c) derive `bad_version` from `good_version` + 1 patch in `evaluate`.
**Choice.** (b), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`). A `ROLLBACK` is right when its target equals the
`previous_version` of the `service_health` row with the decision's `(service_name,
window_end)` — the same row `SCALE_OUT` targets are already checked against — and
unscorable when there is no such row or no previous version (a missing target is still
wrong). The ground truth keeps only: incident id, type, service, start, end, expected
trigger, expected action.
**Why.** Less to carry and nothing lost: the target check now asks "did the agent use the
Previous Version it was given", and `sql/checks/temporal_join_attribution.sql` already
checks version attribution per request, more strictly than the per-window check did.
**Impact.** `good_version` is gone from `schemas/service_incidents.avsc`,
`sql/source_service_incidents.sql` and its copy in `platform-team/…/statements.tf`
(environments must be recreated), `opsgen/groundtruth.py`, `opsgen/engine.Incident` (the
console log already uses `Event.old_version`/`new_version`), the ops-wall state and the
mock. `bad_version` is gone from `incidents.jsonl`. `evaluate`: `HealthRow` gains
`previous_version`; `check_target` loses its incident argument; `Incident` loses both
versions; `IncidentResult.version_mismatches` and its report column are gone.
`scripts/tuning/sweep.py` builds `Incident` without them. Amends D-021, D-032 and D-037.

**Amended by D-058.** `temporal_join_attribution.sql` is deleted (a metrics sample has no
`instance_id`). Version attribution is now checked per window only
(`sql/checks/service_health_deploy_windows.sql`).

### D-046 · DECISION · `service_incidents` is the only answer key; `incidents.jsonl` removed
**What.** The generator wrote the ground truth twice: the `service_incidents` topic
(ops wall) and `runs/pre-provisioned/incidents.jsonl` (`evaluate`, and the generator's
own incident numbering across restarts). D-038 kept the file only because `evaluate`
already worked against it.
**Options.** (a) Keep both; (b) make the topic the only answer key, and give the file's
two side jobs a new home.
**Choice.** (b), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`):
- **`evaluate` reads `service_incidents`** through the same `read_topic` path as
  `service_health` and `remediation_decisions`. `incidents_from_records` keeps each
  incident's end record (keyed by `incident_id` and `started_at`), in whatever order the
  two records arrive, and ignores incidents that haven't ended — the file only ever held
  finished incidents, so scoring is unchanged.
- **Offline**: `--dump-incidents FILE` saves the records read, and `--incidents FILE`
  scores against such a dump, like `--dump`/`--rows` and
  `--dump-decisions`/`--decision-rows`. `--rows` without `--incidents` exits with a
  message: offline there is no other ground truth.
- **Incident numbering** moves into the generator state file, which `ops-sim` already
  saves every 5 s and `reset` already deletes: `{"services": {...}, "incident_counter": n}`.
  Ids must not repeat because the ops wall pairs start and end records by `incident_id`.
  State files in the old shape aren't migrated: `reset` deletes them and environments are
  recreated.
**Why.** One answer key instead of two, and the same read path for all three tables.
`reset` already clears the topic (step 4), so a fresh run starts with a clean answer key
either way.
**Impact.** `opsgen/groundtruth.py` keeps only `start_record`/`end_record`;
`opsgen/driver.run_realtime` loses `write_groundtruth`; `opsgen/simulation.py`
(`state_snapshot`, `initial_state`), `opsgen/state.py` and `opsgen/cli.py` carry the
counter; `scripts/reset.py` step 5 deletes only the state file; `scripts/evaluate.py`
and `scripts/evaluation/detection.py` (`incidents_from_records` replaces
`load_incidents`; `Incident.from_record` reads the topic's `incident_type`). Supersedes
D-038.

### D-047 · DECISION · The ops-wall mock follows the simplified rules
**What.** `uv run ops-wall --mock` scripted 8 scenes in a 12-minute loop, with 60 s
incidents at arbitrary offsets, `OBSERVE` as a lead-in to real actions, an `ESCALATE`
scene, a "resolved without action" scene, surges on `cart` (which can't surge in ops-sim)
and starting versions shuffled between services (its `checkout` ran `payments`' 2.9.4).
**Options.** (a) Leave the mock as a free-standing showcase; (b) make it obey the same
rules as ops-sim.
**Choice.** (b), decided with the user in the simplify-simulator grilling session
(`.scratch/simplify-simulator/spec.md`). Six scenes, 80 s apart after the 120 s warm-up,
in a 10-minute loop: `checkout` bad deploy → `ROLLBACK` to 4.2.1; `catalog` surge →
`SCALE_OUT` to 9; `payments` drop → `INVESTIGATE_UPSTREAM`; `web-gateway` false alarm →
`OBSERVE`; `cart` drop → `INVALID`; `payments` bad deploy, missed. Every incident starts
on a 10-second boundary (the driver's start is aligned) and lasts 40 s; each detected
incident flags its 4 windows (the false alarm 2); the first decision lands 18 s after the
start, i.e. after the first window closes. Services start on their catalogue versions,
and a bad-deploy scene deploys the next patch at its start and rolls it back at its end.
The incident mapping comes from `opsgen.engine` (`TRIGGER`, `EXPECTED_ACTION`), not a copy.
**Why.** The mock is what most people will see first; if it shows things the simulator
never does (an `OBSERVE` before every fix, an anomaly that fades on its own), it teaches
the wrong story. Note that the mock now shows the open loop honestly: the wall closes a
detection at the agent's decision while the injected incident runs its full 40 s. That
gap is what closing the loop (later) removes.
**Impact.** `scripts/opswall/mock.py` (scenes, timing, versions, `aligned_start`; the
unused `POST_WINDOWS_AFTER_CLOSE` and `PRESEED_VERSIONS` are gone), `scripts/opswall/app.py`
(aligned start) and `tests/test_opswall_mock.py`, which now checks the mapping, the
timing, `OBSERVE`-only-on-false-alarms and the deploy/rollback on the tile.
**Replaced by D-055.** The mock is removed; the wall needs a live feed.

### D-048 · DECISION · The live ops wall decodes records the way Kafka delivers them
**What.** Read in the simplify-simulator session and reproduced offline: the live ops
wall would drop most of what it reads. (1) The Avro decoder turns Flink's `TIMESTAMP(3)`
columns (registered as `local-timestamp-millis`, D-027) into naive `datetime`s, and
`OpsWallState._to_seconds` returned `None` for them — so every `service_health` row was
discarded (no sparklines, statuses or detection spans) and incident start times fell back
to 0.0. (2) `service_deployments` values carry no `service_name` (it is only in the key,
D-027), the consumer decoded only the value, and `apply_deployment` returned at once — so
tiles never showed the live version or a rollback. The mock and the tests fed the state
plain dicts with epoch milliseconds and a `service_name`, so neither path was exercised.
A third suspicion — decisions overwriting each other — was ruled out by running it.
**Evidence.** The Phase 4 dumps read from Confluent Cloud hold the timestamps as the ISO
text `jsonable` writes only for `datetime`s; D-027 checked the deployments schemas against
the live Schema Registry; an Avro round trip through the real serializer/deserializer
pair reproduced both drops.
**Options.** For (2): (a) decode the key in the consumer and merge it into the record;
(b) change the source DDL so the key is also written into the value.
**Choice.** (a), decided with the user: local to the ops wall, no schema or attendee-facing
change, testable offline. For (1), `_to_seconds` reads a naive `datetime` as UTC
wall-clock (the same rule as `topic_reader.epoch_seconds`).
**Impact.** `scripts/opswall/state.py` (`_to_seconds`), `scripts/opswall/consumer.py`
(`make_deserializer`, `decode_record`; only `service_deployments` decodes its key), and
`tests/test_opswall_ingest.py` with `tests/fixtures/opswall/` — real `service_health` and
`remediation_decisions` rows from the Phase 4 run (the payments bad deploy, minus the
columns D-043/D-044 dropped), plus deployments and incidents through the real Avro pair in
an in-memory Schema Registry. `.gitignore` now admits `tests/fixtures/` (credential
patterns still excluded). Still to confirm in a live run against a recreated environment.

### D-049 · FIX · `service_incidents` has no key columns (`DISTRIBUTED INTO 1 BUCKETS`)
**What.** D-037's DDL said `DISTRIBUTED BY HASH(service_name) INTO 1 BUCKETS` with
`service_name` as the third column. Phase 5 never applied it; the first live
`terraform apply` (2026-11-13) failed with "Key columns must appear at the beginning of
the table schema": `DISTRIBUTED BY` makes `service_name` a Flink key column, which
Confluent Cloud requires first and registers as an Avro key. That also contradicted the
generator, which writes a raw-string key and expects no key subject
(`opsgen/kafka_io.emit_incident`, "same shape as service_requests").
**Choice.** `DISTRIBUTED INTO 1 BUCKETS`, exactly like `service_requests` (proven live
since Phase 2): no key columns, `service_name` stays in the value where ops-wall and
`evaluate` read it. One bucket keeps per-service order anyway.
**Impact.** `sql/source_service_incidents.sql` and its copy in
`platform-team/modules/developer/statements.tf`. An environment where the old statement
already created the table must drop it before re-applying (`CREATE TABLE IF NOT EXISTS`
won't change an existing table).

### D-050 · DECISION · Terraform creates `service_health` and `remediation_decisions`; lab 2.4 is an INSERT
**What.** Attendees created the two lab output tables themselves: `service_health` with a
CTAS in lab 2.4, `remediation_decisions` with a `CREATE TABLE` in lab 4.3. That cost lab
time on table plumbing, left the ops wall with no topics to read until the labs were done,
and made improving the detection a drop-and-replay (a plain re-run of the INSERT appends a
second copy of the history).
**Options.** (a) The platform team's Terraform runs `CREATE TABLE IF NOT EXISTS` for both,
the same pattern as the source tables; (b) a `confluent_kafka_topic` plus a registered
schema, with Flink inferring the table.
**Choice.** (a), decided with the user in `.scratch/terraform-lab-tables/`. (b) needs new
per-developer Kafka and Schema Registry keys (Global keys do not cover Standard clusters),
gives an inferred table with an extra leading `key` column that breaks a positional INSERT,
and defaults to `read-committed`. Both tables: append-only, read-uncommitted, `delete`
cleanup (it is what lets the reset tools empty a topic), 24 h retention, earliest offset,
`avro-registry` values, `DISTRIBUTED INTO 1 BUCKETS`, no custom watermark. `service_health`
has the 22 columns of the 2.3 status query, with both status columns declared `STRING`;
`remediation_decisions` keeps its 8. Lab 2.4 is `INSERT INTO service_health` followed by
the 2.3 status query verbatim; lab 4.3 keeps only the two agent INSERTs. The two DDL files
are `sql/sink_service_health.sql` and `sql/sink_remediation_decisions.sql` (renamed from
`4.3_remediation_decisions.sql`), each the single source of truth for its copy in
`statements.tf`; the sync test now covers all five tables.
**Why.** The labs are about the queries. Pre-created tables also let the ops wall open at
the start of a session.
**Impact.** `platform-team/modules/developer/statements.tf` (two statements chained after
`service_incidents`, the model depends on the last), `outputs.tf`, the module tests, and
the README (it now lists 12 resources per person; its old count of 9 was already stale
since `service_incidents`). `tests/test_sql_pipeline.py` checks the DDL's column list
against `SERVICE_HEALTH_COLUMNS`. `evaluate` says "run the INSERT" instead of "create it".
The readiness check lists five tables. The sync test's helper now finds the statement by a
line that starts with `CREATE TABLE`, because a header comment that mentioned it had made a
copied heredoc start in the comment.
**Pending (live check).** The `service_health` types other than the status columns are
derived, not observed, and so is `INSERT INTO ... WITH ... SELECT` being accepted. The
first `terraform apply` is the check: run lab 2.4's INSERT (it must start RUNNING with
rows appearing), `DESCRIBE EXTENDED service_health`, and if a type must change `DROP TABLE
service_health`, fix `sql/` and `statements.tf` together, and apply again (`IF NOT EXISTS`
never changes an existing table). If the CC grammar rejects `INSERT ... WITH`, move the
shared CTEs into nested subqueries in 2.4.
**Amended by D-054.** `service_health` has 23 columns: `detected_at TIMESTAMP_LTZ(3) NOT NULL` is the
last. `remediation_decisions` keeps its 8.

### D-051 · DECISION · `reset` empties the output topics instead of dropping their tables; `--with-labs` is removed
**What.** `reset` dropped `service_health` and `remediation_decisions`, which Terraform now
owns: a dropped table would not come back (Terraform's statement stays COMPLETED), and
deleting their Schema Registry subjects would break them too.
**Choice.** `reset` keeps its guard against a running `ops-sim`, stops all running
statements (and now waits until they are gone), drops the two agents (`CREATE AGENT` has no
`IF NOT EXISTS`), and deletes the generator's state file. It empties five topics with
delete-records: the three source topics and the two output topics. `--keep-source` keeps
only the three source topics; the output topics are always emptied, because they would hold
duplicates once their INSERTs run again. A missing topic is reported as a problem. The
`--with-labs` option, `create_lab_objects`, `LAB_SQL`, `LAB_TABLES`, the subject deletion
and every comment that promised a rebuild are removed, at the user's request. Nothing
rebuilds the labs now: run the INSERTs and create the agents again. Whether a replacement
is needed is decided at the end of this work, not here.
**Why.** One tool must leave the platform team's tables in place, and fewer lines of code
mean less to maintain.
**Impact.** `scripts/reset.py`, `scripts/common/flink.py` (the wait helper replaces the
rebuild code), `scripts/common/topics.py` (the emptying code and the topic lists, shared
with `reset-health`), `tests/test_reset.py` against a fake Flink session and a fake Kafka
admin (the `install_fakes` fixture in `tests/conftest.py`). Wording that said the SQL files exist so
`--with-labs` can submit them is reworded.
**Addition not in the spec.** `reset` now waits (up to 60 s) for the statements it deleted to
be gone before it empties anything, like `reset-health`: it used to rely on the table drops
and the agent drops taking long enough, and a still-running INSERT would leave stray rows in
the topics it has just emptied. A statement that stays up is reported as a problem.

### D-052 · DECISION · `uv run reset-health`: a clean slate for the detection and the agents
**What.** An attendee who improves their `service_health` query and re-runs the INSERT
appends a second copy of the replayed history; there was no easy way back to a clean slate.
**Choice.** A new command, for attendees. It deletes every running statement whose SQL is
`INSERT INTO service_health ...` or `INSERT INTO remediation_decisions ...` (the second form
catches both agents however they are named), matched on the SQL text (leading `--` and
`/* */` comments, backticks, catalog and database prefixes, and letter case are ignored;
SELECTs and INSERTs into other tables are not targets). Statements are deleted, not paused,
with the same call as `reset`: attendees work in the Confluent Cloud workspace, where the SQL
stays and Run starts it again. It prints each statement's name and SQL (before deleting it),
waits until the targets are gone (a missing entry or a terminal phase counts; 60 s), and
only then empties the two topics. If a statement stays up it exits nonzero and empties
nothing; a missing topic exits nonzero too. With nothing running it just empties the topics.
It leaves the source topics, other statements, the agents' definitions and local files alone,
asks no confirmation and re-runs nothing. It ends with the restart order: the
`service_health` INSERT first, the agent INSERTs only once the wall's charts reach the
right-hand edge again, because a fresh `service_health` INSERT replays up to 24 h of history
and agents started meanwhile would call the model for every replayed anomaly; and a hint to
press "Refresh data" on the ops wall (D-053).
**Why.** One command, safe by construction: no INSERT can write into a topic while it is
being emptied, and the other statements an attendee has open keep running.
**Impact.** `scripts/reset_health.py` and its entry in `pyproject.toml`;
`tests/test_reset_health.py` (the matcher in both directions, the order of deletes and
empties, a stuck statement, nothing running, the printed guidance).
**Pending (live check).** Statement delete, delete-records and the matching against real
statement listings were only tested against fakes. Check in your environment: with the labs'
INSERTs running, `reset-health` deletes exactly those, empties both topics, leaves other
statements running, and Run on the INSERTs fills the tables again with no duplicates.

### D-053 · DECISION · "Refresh data" on the ops wall restarts the feed behind it
**What.** After `reset-health` empties the output topics (D-052), a running ops wall still
held up to 10 minutes of old charts and pins in memory, and a consumer stayed attached to a
topic that had been deleted and recreated. The only cure was restarting the wall.
**Choice.** A "Refresh data" button in the page header, no confirmation. It calls
`POST /refresh` (a POST: it changes state), which asks the `Feed` — the new owner of the
feed threads — to restart: stop and join the running threads, `OpsWallState.clear()`
(six pre-seeded services, no history, pins or connection errors, `generation` + 1), then
start a new set. Live: every consumer is attached afresh from its usual start
(`service_deployments` from the beginning, the other three from about 10 minutes back).
Mock: a new `MockDriver` on a fresh aligned start, so the loop plays again from its first
scene. One lock serialises restarts, so overlapping clicks cannot start two sets of
consumers. Every snapshot carries `generation`; the button shows "Refreshing…" from the
click until a snapshot of the new generation arrives, never for less than 1 s, and ignores
clicks meanwhile.
**Options.** (a) Clear the state only: leaves the stale consumers attached to a recreated
topic. (b) Seek the existing consumers: more code, same result. (c) Restart the whole feed
(chosen): the same code path as startup, so a refresh is by construction what a fresh wall
would show.
**Impact.** `scripts/opswall/` (`feed.py` new; `state.py` `clear()`/`generation`;
`server.py` `POST /refresh`; `app.py`; `mock.py` `start_threads` replaces the unused
`run_until_interrupt`; `consumer.py` stops promptly on the stop event; the header button in
`static/`). Tests: `tests/test_opswall_{state,server,feed}.py`. No JS test setup exists, so
the button is a manual check. Refreshing loses nothing: all data comes back from Kafka.

### D-054 · DECISION · The ops wall draws each anomaly from detection to the agent's answer
**What.** The lower half of each chart drew the windows `service_health` flagged, in the
time of the data, until the anomaly faded or a fix landed. It hid the real-time delay (the
window has to close, the watermark has to pass), looked the same whether or not an agent ever
answered, said "missed" for every incident until a detection overlapped it, and did all its
counting in the browser, untested. Decided with the user in a grilling session on 2026-11-13;
spec in `.scratch/opswall-detected-answered/spec.md`.
**Choice.**
- `service_health` gains a 23rd and last column, `detected_at TIMESTAMP_LTZ(3) NOT NULL`:
  `CURRENT_TIMESTAMP` in the final SELECT, evaluated per row in a streaming query and filled
  on every row. It is the table's one `TIMESTAMP_LTZ`, an instant, on purpose. The first
  version cast it to `TIMESTAMP(3)` on the assumption that the session time zone is UTC (the
  documented default), but the Cloud Console workspace submits every statement with
  `sql.local-time-zone` set to the browser's zone: the first live run (2026-11-14, statement
  properties read over REST) had `GMT+01:00`, so the column held local wall-clock time, the
  wall read it as UTC, and every detection sat an hour in the future (a "+60m 15s" time to
  detect that kept growing, "answered 0s later", blocks never removed). The workspace shows
  `detected_at` in the attendee's zone while the window columns are UTC; the 2.3 comment says
  so, and `sql/checks/service_health_anomalies.sql` runs in UTC with a `SET`. `PROCTIME()` is
  not supported on Confluent Cloud. It is in the 2.3 status query too, so lab 2.4 stays the
  2.3 query behind one line. `remediation_decisions` keeps its 8 columns; the agent INSERTs
  read `service_health` by name and `evaluate` reads by name, so neither changes.
- The times the wall uses: a flagged window's `detected_at`; an answer's Kafka record
  timestamp (Confluent Cloud's sink sets it to the write time, measured live on
  `service_health` even during a replay); an incident's `started_at`/`ended_at`. A
  `CURRENT_TIMESTAMP` column in the agent INSERTs was rejected: whether it is evaluated before
  or after `AI_RUN_AGENT` returns is not documented.
- A **run** is consecutive flagged windows for one service and one trigger (latency high,
  traffic high, traffic low); one window without that flag ends it. Only windows whose
  `window_end` is on the chart count, which keeps a restarted INSERT's replay (every replayed
  row has `detected_at` = now) off the wall.
- Its **block** opens at the `detected_at` of the run's first window (no `detected_at`, no
  block), grows toward now, and closes at the write time of the first decision for any window
  of the run (matched on service, `window_end` and trigger), whatever the action: `OBSERVE`
  and `INVALID` close it too. The chart draws only the block, no icons: the spec's pin at the
  closing edge was dropped after the first live run, at the user's request ("we should just
  show the window"). The block's tooltip names the answer (action and target, the agent's
  reason) and counts all the run's decisions by action. Unanswered 30 s after the run's last
  `detected_at`, the block is removed.
- A block **belongs** to an incident on the same service and metric (bad deploy: latency;
  surge or drop: traffic) when the run's first window overlaps the incident, counting up to
  30 s after it ended; an incident can own several blocks; a block owned by none is a false
  alarm. An incident is **answered** once one of its blocks closed, **missed** when it ended
  more than 30 s ago and none closed, **pending** (plain purple) until then. Time to detect and
  time to answer are measured on its first block.
- The tile: "Anomaly detected: …, waiting for the agent" while a block is open; the fix
  ("Rolled back to vX", "Scaled out to N replicas", "Traced to upstream") for 120 s after a
  fix decision; "Healthy" otherwise. "Detector warming up" and the band texts are gone; Rate
  and Errors still come from the newest row.
- `OpsWallState.snapshot()` computes all of it (blocks, ownership, verdicts, the summary
  counts and medians, the tile); the page only draws. The detection spans, the warm-up band
  and the decision list are gone from the snapshot; `generation` stays.
**Why.** One chart then tells the whole real-time story: the gap from the purple box to the
block is the time to detect, the block's width is the agent's time to answer, and with the
agents not running yet, blocks appear and disappear, which says exactly that.
**Deferred.** Showing anomalies nobody answered for longer than the 30 s before their block is
removed. The mock (`ops-wall --mock`, D-047) is out of date: it writes no `detected_at`, so it
shows incident boxes but no blocks, and its scenes were tuned for the old rules. Its outdated
tests are skipped ("mock not updated for D-054") until
`.scratch/opswall-detected-answered/issues/01-mock-draws-blocks.md` is done.
**Impact.** `sql/2.3_anomaly_status.sql`, `sql/2.4_service_health.sql`,
`sql/sink_service_health.sql` and its copy in `platform-team/modules/developer/statements.tf`;
`sql/checks/service_health_anomalies.sql` shows `detected_at` and the delay (in UTC, with a
`SET`); the checks README. `scripts/opswall/state.py` (rewritten snapshot), `static/opswall.js`
(draws the snapshot, no counting, no pins; the legend loses the pin shapes),
`static/index.html` and `static/opswall.css` (warm-up hatch and styles gone). Tests: `tests/test_opswall_state.py` (rewritten around the scenarios above),
`tests/test_opswall_ingest.py` (a synthetic `detected_at` on the real Phase 4 rows, which
predate the column, and a `timestamp-millis` round trip through the real Avro pair), `tests/test_opswall_server.py`, `tests/test_opswall_mock.py` (six
skipped), `tests/test_sql_pipeline.py`, `tests/test_evaluate_detection.py` (a 23-column row
scores as before). Rolling out needs `DROP TABLE service_health`, a `terraform apply` (the
statement is `CREATE TABLE IF NOT EXISTS`), then the INSERT restarted. **Pending (live
check):** the real detection delay and answer time (the 6 s and 8 s in the spec are
estimates; the first live run read 15 s to detect once the zone offset is taken out), and
that `CURRENT_TIMESTAMP` is evaluated in the final projection (`EXPLAIN` on the live
statement shows it). The `TIMESTAMP_LTZ` column needs the table dropped and applied again.

### D-055 · DECISION · The ops-wall mock is removed
**What.** `uv run ops-wall --mock` drove the wall from a scripted scene loop with no Kafka
(D-047). It had drifted from the live wall: it writes no `detected_at`, so since D-054 it
drew incident boxes and no blocks, and six of its tests were skipped. Every wall change
had to read around it, and D-054 left a follow-up issue to bring it back in step.
**Options.** (a) Update the mock to the D-054 rules
(`.scratch/opswall-detected-answered/issues/01-mock-draws-blocks.md`) and keep it in step
with every later wall change; (b) remove it.
**Choice.** (b), decided with the user in the simplify-demo grilling session
(`.scratch/simplify-demo/spec.md`). The wall needs a live feed: `ops-sim` and the
attendee's environment. `OpsWallState._to_seconds` now reads only `datetime`s. Every time
the wall reads arrives that way through the Avro decoder, and the epoch-millisecond and
float paths existed only for the mock and for tests that fed the state in its shape.
**Why.** The mock showed a story the live wall no longer told, and keeping it in step costs
work on every wall change. The design drafts in `.scratch/mockups/` stay: they are HTML
mockups, not a feed.
**Impact.** `scripts/opswall/mock.py` and `tests/test_opswall_mock.py` are deleted.
`scripts/opswall/app.py` loses `--mock`, its starter and the `[MOCK FEED]` log suffix; the
consumer import is no longer lazy. "Refresh data" (D-053) re-attaches the Kafka consumers
and nothing else. Docstrings in `scripts/opswall/` (`__init__`, `feed`, `state`) and
`tests/test_opswall_ingest.py` no longer mention the mock. `tests/test_opswall_feed.py`
loses the mock refresh test. `tests/test_opswall_state.py` and `tests/test_opswall_server.py`
feed `datetime`s where they passed epoch milliseconds. The D-054 follow-up issue is
`wontfix`. Replaces D-047.

### D-056 · DECISION · Three services: `web-gateway`, `checkout`, `payments`
**What.** `ops-sim` ran six services, each with its own quirks: `auth`'s 401s, three
services that could only get a surge (`web-gateway`, `catalog`, `auth`) and three that could
only get a drop (`cart`, `checkout`, `payments`).
**Options.** (a) Keep the six; (b) keep three and remove `cart`, `catalog` and `auth`
completely (code, wall, tests, docs).
**Choice.** (b), decided with the user in the simplify-demo grilling session
(`.scratch/simplify-demo/spec.md`). The kept services keep every value they had: tier,
owner team, initial version, latency median, replicas, routes and rates (10/6/6 requests/s;
D-058 and D-059 change the data model and the means). Candidates:
- `BAD_DEPLOY` → all three;
- `TRAFFIC_SURGE` → `web-gateway` only;
- `UPSTREAM_DROP` → `checkout` and `payments`.
Client errors are all 404: `auth`'s 401s go with it.
**Why.** The workshop showcases streaming and streaming agents, not a realistic shop. Three
services are easier to follow on the wall and in the labs, and each removed service only
added a quirk to explain.
**What it changes in earlier decisions.**
- D-030/D-031: the drop services are now two, both still at 6 requests/s; the total is ~22
  requests/s instead of ~41.
- D-043: a surge (`SCALE_OUT`) happens on `web-gateway` only.
- D-042: its "3-service surge/drop lists" are now one and two services. Under its schedule
  (one incident at a time, 7-min cooldown per service) the rate stays ~10.5 incidents/h, but
  `web-gateway`'s cooldown often blocks a surge and the engine re-picks the type. Measured
  with the generator over 96 simulated hours (16 seeds × 6 h): `BAD_DEPLOY` 51 → 54%,
  `TRAFFIC_SURGE` 26 → 17%, `UPSTREAM_DROP` 24 → 29%. D-060 replaces this schedule with one
  per service.
**Impact.** `opsgen/services.py` (three entries, docstring), `opsgen/engine.py`
(`CANDIDATES`), `opsgen/distributions.py` and `opsgen/simulation.py` (no `is_auth`:
`status_code(success_code, rng, server_error_rate)`), the `opsgen/state.py` docstring. Fewer
services draw from the RNGs, so `--seed` runs differ from earlier ones. Agent prompts
(`sql/4.2_release_agent.sql`, `sql/4.2_capacity_agent.sql`): "three microservices", and the
examples use kept services (the `SCALE_OUT` example is `web-gateway` on 4 replicas, Target
12). `scripts/tuning/sweep.py` (rate profiles, drop columns). Comments in
`sql/source_service_requests.sql` (outside the `CREATE TABLE`, so the Terraform copy is
unchanged), `sql/checks/remediation_call_rate.sql` (180 `service_health` rows per 10 quiet
minutes), `scripts/opswall/state.py`, `static/opswall.js` and the README. The wall's rows
and `/healthz` `serviceCount` follow `SERVICES`; `evaluate` reads service names from the
data. No change to Terraform, the schemas or the DDL. Tests: `CANDIDATES` is pinned in
`opsgen/tests/test_engine.py`; the opsgen, tuning, wall, evaluate, agent-SQL and
`reset-health` tests use the kept services and count three. The remaining `401`s in
`scripts/preprovisioned/` and its tests are Confluent Cloud API answers, not `auth`.

### D-057 · DECISION · No benign deploys
**What.** `ops-sim` deployed a random healthy service every 3-5 minutes (first at 60 s): a
patch-version bump that changed no metric, so the temporal join had versions to follow
outside incidents. With frequent incidents, bad deploys and their rollbacks already change
versions all the time. The only effect benign deploys had left was wrong rollbacks: the
release agent answers `ROLLBACK` to `LATENCY_HIGH` when the last change is a `DEPLOY` less
than 600 s old, so a latency false alarm within ten minutes of a benign deploy rolled back
a healthy version.
**Options.** (a) Keep them; (b) remove them.
**Choice.** (b), decided with the user in the simplify-demo grilling session
(`.scratch/simplify-demo/spec.md`). The deployments stream keeps the bootstrap record, the
5-s heartbeat (it advances the temporal join's watermark), and each bad deploy's deploy and
rollback. A rollback sets the last change to `ROLLBACK`, so after an incident ends the
release agent answers `OBSERVE` to a latency false alarm, as it should.
**Why.** They were one more kind of version change to explain, and their only remaining
effect on the demo was a wrong answer.
**Impact.** `opsgen/engine.py`: `Config` loses `benign_first`, `benign_min`, `benign_max`
and `benign_min_gap`; `Engine` loses `next_benign_at`, `last_deploy_at` (only the benign
picker read it), `_maybe_benign_deploy` and `_choose_benign_service`; the `benign_deploy`
event kind and its console line (`opsgen/sinks.py`) are gone. `opsgen/cli.py` loses
`--benign-first`, `--benign-min` and `--benign-max`; `--no-incidents` now means baseline
traffic with no version change at all. Docstrings in `opsgen/__init__.py` and
`opsgen/deployments.py`. Benign deploys drew from the scheduler's RNG, so `--seed`
schedules differ from earlier runs; incident timing and the type mix are otherwise
unchanged (the incident chooser never looked at deploys). `evaluate` drops the *after
deploy* tag (D-021). `scripts/opswall/consumer.py` renames `BENIGN_ERROR_CODES` (Kafka
errors it skips, unrelated) to `IGNORED_ERROR_CODES`. The release agent's prompt, the SQL,
Terraform and the schemas are unchanged. Tests: an hour of `--no-incidents` sees only the
heartbeat after the bootstrap (`opsgen/tests/test_engine.py`); version changes come only
from bad deploys and their rollbacks; `--simulate` shows every deploy as a `BAD DEPLOY`;
the tests that relied on a benign deploy force a bad one. Amends D-021 and D-042.

### D-058 · DECISION · `service_metrics`: one record per service per second
**What.** `ops-sim` wrote one `service_requests` record per HTTP request: instance, route,
method, status code, latency, bytes and trace id. A traffic anomaly changed the number of
records, and the detector's input (`COUNT(*)` per window) was an aggregate attendees never
saw in the topic.
**Options.** (a) Keep per-request records; (b) one metrics sample per service per second
that sums up that second.
**Choice.** (b), decided with the user in the simplify-demo grilling session
(`.scratch/simplify-demo/spec.md`). The topic is `service_metrics`, and a record is:
- `event_time`: tick + 999 ms, the end of the second it covers. Deploy records stay at the
  tick (D-028), so on a deploy's tick the temporal join gives the record the new version;
- `service_name`;
- `traffic_count` (INT): Poisson(mean × the incident's traffic multiplier);
- `latency_ms` (DOUBLE): the mean of `traffic_count` log-normal draws (median × the
  incident's latency multiplier, sigma 0.3, the D-030 slow outliers); one draw when
  `traffic_count` is 0, so it is always set;
- `error_count` (INT): failed requests among `traffic_count` (0.5% normally, 12% in a bad
  deploy). Display only: nothing detects or decides on it.

Three records per second whatever the traffic: an incident changes the values, never the
number of records. `instance_id`, `route`, `http_method`, `status_code`, `response_bytes`
and `trace_id` are gone. The means (10/6/6) and spike sizes (surge ×3 traffic and ×1.5
latency, drop ×0.2, bad deploy ×4 latency) are unchanged here; D-059 tunes them.

Detection stays on 10-second windows: `SUM(traffic_count) AS traffic_count` and
`AVG(latency_ms)`, the unweighted mean of the window's ten samples. `service_health`
loses `error_count`, `error_rate`, `max_latency_ms` and `error_rate_breach`;
`request_count` and `expected_request_count` become `traffic_count` and
`expected_traffic_count` (19 columns, was 23). Lab 2.1 keeps its regular-vs-temporal join
lesson, with counts as `SUM(traffic_count)`. Its proof check
(`sql/checks/temporal_join_attribution.sql`) goes, because a sample has no `instance_id`
to carry the version.
**Why.** The workshop showcases streaming: attendees should see in the topic the values
the detector works on, one step before the window. Offline, 10-second windows built from
samples match the old per-request windows: the same means, the same count spread, and a
latency spread ×1.05–1.11 (`.scratch/simplify-demo/research/ps_compare.out`). Detection
behaves the same and earlier tuning numbers carry over. No detector or agent rule needed
the error columns. The capacity prompt used Error Rate Breach only in its rule 3 (never
`OBSERVE` on a breach), which rules 1 and 2 always pre-empt.
**What it changes in earlier decisions.**
- D-022: the per-version queries sum `traffic_count` (`first_sample`, `last_sample`).
- D-024: no error expressions, no `MAX(latency_ms)`; `traffic_count` is an INT.
- D-026, D-045: the attribution check is deleted.
- D-028: records sit at tick + 999 ms, not at a random offset within the tick.
- D-030, D-031: the rates are means of `traffic_count`; the topic carries three messages
  per second whatever the rates. `sweep.py` builds windows from samples.
- D-044: `response_bytes` is gone; the guard test keeps `error_count` out of labs 2.2–2.4.
**Impact.**
- `opsgen/`:
  - `simulation.py`: `_gen_metrics` replaces `_gen_requests`.
  - `distributions.py`: new `mean_latency_ms`, `binomial` and `ERROR_RATE`. Gone:
    `status_code`, `response_bytes`, `trace_id` and the status-code mixes.
  - `services.py` loses `Route`, `routes` and `pick_route`; `instances.py` is deleted.
  - `engine.py`: `Effect.error_rate`, `BAD_DEPLOY_ERROR_RATE`.
  - `driver.py`: the `--simulate` line reads `traffic= err= lat= ver=`, with no `inst=` hash.
  - `sinks.py`, `kafka_io.py`: `emit_metrics` and the `service_metrics` topic; `cli.py`.
- Schema and Terraform:
  - `schemas/service_metrics.avsc`; `sql/source_service_metrics.sql` (same connector
    options); `sql/sink_service_health.sql`.
  - Both copies in `platform-team/modules/developer/statements.tf`. The resource is
    renamed `create_service_metrics_table`, and its module output, `platform-team/outputs.tf`
    and the `.tftest.hcl` files follow.
  - `scripts/common/topics.py`, `scripts/preprovisioned/provision.py`.
  - Environments must be recreated: `service_requests` becomes `service_metrics` and
    `service_health` changes shape.
- Labs: 2.1–2.4 (the metrics side's alias is `m`); the three `service_health` checks and
  `sql/checks/README.md`.
- Agents:
  - The capacity INSERT sends `Traffic Count` and `Expected Traffic Count`, and computes
    Suggested Replicas from `traffic_count`. Neither INSERT sends Error Rate Breach.
  - The capacity prompt's rule 3 keeps `OBSERVE` for weak evidence, without the Error Rate
    Breach conditions, and its `OBSERVE` example loses "with no error rate breach".
  - The release prompt's `ROLLBACK` example loses "11 percent of requests failing".
- `evaluate`: `HealthRow` loses the error fields; the count fields become `traffic_count`
  and `expected_traffic_count`.
- `scripts/tuning/`: `sweep.py` windows from samples, a `req/s` column; `calibrate.py`
  column names.
- The ops wall: Rate is `traffic_count / 10` from `service_health`. Errors shows "—" until
  issue 07 reads `error_count` from `service_metrics`. The ingest fixture is the Phase 4
  live dump with the dropped columns removed and the count columns renamed.
- README and `platform-team/README.md` no longer call the topic a request log.
- The traffic RNG draws differently, so `--seed` runs differ from earlier ones.
- Tests:
  - One record per service per second through a surge, a drop and a bad deploy, with the
    values moving by `opsgen.engine`'s multipliers (so the test follows D-059's sizes).
  - A record at tick + 999 ms, after the deploy record of its tick.
  - Windows of samples against the old per-request windows (mean and spread).
  - The new `.avsc`, DDL and `statements.tf` stay in step; the 19-column order;
    `test_instances.py` is deleted.

Amends D-022, D-024, D-026, D-028, D-030, D-031, D-044 and D-045.

**Amended by D-059.** The means are 50/30/30 and the spike sizes surge ×5 traffic (no
latency effect), drop ×0.1 and bad deploy ×4 latency.

### D-059 · DECISION · Tuned data and reference detector: 20 s spikes, 50/30/30, confidence 99.95
**What.** Workshop preparation: the data settings and **our** reference detector settings
for the simplified demo (`.scratch/simplify-demo/spec.md`, issue 05), chosen offline with the
replica for the issue-06 schedule (one schedule per service, incidents may overlap across
services, 20 s between incidents on a service, per-type rests, first three one of each type,
50/25/25; D-060). Attendees tune their own detectors later; that competition is out of scope.
**Options.** Varied with `.scratch/simplify-demo/research/tune.py` (the per-second data
model, the issue-06 schedule, an AR(p) replica with any refit interval, scored with
`evaluate`'s code): spike length 20/30 s (10 and 40 s from the earlier search), traffic means,
spike sizes, `confidencePercentage`, `minTrainingSize`, `maxTrainingSize`, `p`, the refit
interval. A setup qualifies at **one grid step of margin** above its minimum rests (rest grid
0/60/120/180/240/300/420/600/900/1200 s; the minimum is the smallest rest per type with
≥ 99.5% recall) if that margin still allows **about 60 incidents/h**. Then the rests are raised
to land on ~60/h, and the qualifying setup with the fewest false alarms wins.
**Choice.**
- **Spikes 20 s** (2 windows; was 40 s, D-042).
- **Traffic means 50/30/30** requests/s (was 10/6/6).
- **Sizes:** surge ×5 traffic and **no latency effect** (was ×3 and ×1.5 latency); drop ×0.1
  (was ×0.2); bad deploy ×4 latency (unchanged). Each type now moves exactly one metric.
- **Detector:** AR(1) (`p` 1, `d` 0, `q` 0), `minTrainingSize` 12, `maxTrainingSize` 360,
  **`confidencePercentage` 99.95** (was 99.9), `enableStl` false.
- **Rests for D-060** (per service and type, the same on every service): `BAD_DEPLOY` 390 s,
  `TRAFFIC_SURGE` 300 s, `UPSTREAM_DROP` 300 s → **59 incidents/h**, mix about 44/19/37%.
  The quiet period stays tied to `minTrainingSize` 12 (150 s, D-060).

Candidates (replica; 200 seeds × 2 h each; 20 s spikes, 50/30/30, ×5/×0.1/×4, AR(1)
12/360/99.9 unless stated; recall was 100% at the ~60/h rests in every row; "fresh start" is
false alarms per service in the first 5 minutes of a run; "quiet data" is noise per
service-hour with no incidents at all):

| Setup | Min rests (BD/SURGE/DROP) | Margin rests | /h at margin | Rests at ~60/h | Echoes /svc-h | Fresh start /svc | Quiet data /svc-h |
|---|---|---|---|---|---|---|---|
| confidence 99.5 | 180/180/180 | 240/240/240 | 83 | 540/240/240 | 0.155 | 1.09 | 3.40 |
| confidence 99.9 (the start) | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.013 | 0.94 | 0.81 |
| **confidence 99.95** | 240/240/240 | 300/300/300 | **67** | **390/300/300** | **0.005** | **0.85** | **0.48** |
| confidence 99.97 | 240/240/300 | 300/300/420 | 61 | (58/h at 330/300/420) | 0.014 | 0.75 | 0.28 |
| confidence 99.99 | 240/300/300 | 300/420/420 | 58 | — | 0.001 | 0.60 | 0.11 |
| 30 s spikes | 240/240/240 | 300/300/300 | 65 | 330/300/300 | 1.49 | 1.01 | — |
| means 25/15/15 | 240/240/240 | 300/300/300 | 67 | 390/300/300 | 0.021 | 0.77 | 0.94 |
| means 100/60/60 | 180/180/180 | 240/240/240 | 83 | 540/240/240 | 0.006 | 1.05 | 0.79 |
| sizes ×3/×0.2/×3 | 180/240/240 | 240/300/300 | 75 | 390/300/300 | 0.013 | 0.89 | — |
| sizes ×8/×0.05/×6 | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.015 | 0.98 | — |
| `maxTrainingSize` 180 / 720 | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.014 / 0.013 | 0.94 | — |
| `minTrainingSize` 6 (quiet 80 s) | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.006 | 0.68 | 0.81 |
| `minTrainingSize` 6, 99.95 | 240/240/240 | 300/300/300 | 69 | 390/300/300 | 0.001 | 0.50 | 0.48 |
| `minTrainingSize` 24 | 180/180/240 | 240/240/300 | 78 | 420/240/300 | 0.28 | 0.59* | — |
| AR(2) | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.000 | 2.77 | 0.85 |
| refit every 5 rows | 180/180/240 | 240/240/300 | 77 | 420/240/300 | 0.008 | 1.30 | 0.84 |

\* not comparable: the later warm-up moves the first incidents' echoes past the 5 minutes.
Noise outside echoes was 0.000 per service-hour in every demo run: frequent incidents keep
the bands wide.

**Why.**
- **The band comes from the spike windows in the model's history**, not from the data's
  noise. Too few spike windows and the forecast after a spike is still pulled toward it (an
  echo); too many and the band outgrows the next spike (a miss). Both limits scale with the
  spike itself, so spike sizes and `maxTrainingSize` change almost nothing (table), and the
  rests are what sets recall and frequency.
- **20 s, not 10 or 30 s.** Detection is in the first window of every spike (100% of
  detections here). Each extra window adds a spike→spike pair to the AR(1) fit, which pulls
  the forecast after the spike toward the spike: 30 s spikes have ~100× the echoes of 20 s.
  A 10 s spike has no echoes but is flagged only after it has ended (detection lands ~15 s
  after the start), so 20 s is the shortest spike that is still on when it is flagged.
- **Raise the bad-deploy rest, not the traffic rests, to get down to 60/h.** A bad deploy
  can't echo (latency below the forecast is never flagged), while rarer traffic incidents
  mean narrower traffic bands and more echoes. Balancing the mix to 50/25/25 instead
  (300/300/450 at 99.95) gave 13× the echoes (0.066 per service-hour). The weights still decide
  whenever two types are allowed at once; at these rests the rests set the mix.
- **99.95 over 99.9:** it still qualifies with slack (67/h at margin against the 60/h target)
  and has fewer false alarms of every kind (echoes 0.005 vs 0.013, fresh start 0.85 vs 0.94,
  quiet-data noise 0.48 vs 0.81). The replica under-predicts post-incident false alarms
  (D-031: Confluent's spread recovers faster after a jump), so live echoes are likely higher
  than offline and live recall at least as good; a stricter band pays off live. 99.97 and 99.99
  have no slack left (61 and 58/h at margin).
- **Means 50/30/30:** calmer data (100/60/60) shortens the rests but raises fresh-start false
  alarms; rougher data (25/15/15) does the opposite and needs longer rests. 50/30/30 keeps
  visible noise (web-gateway's per-second traffic 50 ± 7) so the band can't collapse.
- **`minTrainingSize` stays 12.** At 6, the first-incident echoes halve only if the
  simulator's quiet period shrinks to 80 s, and the very first output window then flags noise
  in 12% of service starts (offline, at 99.9), the failure D-029 saw live at 6. With our 150 s quiet
  period, 6 gives 1.18 fresh-start false alarms per service against 0.84.

**Validation of the choice** (400 seeds × 3 h, fresh seeds): 59.0 incidents/h, **0 misses in
70,795** (31,200 bad deploys, 13,200 surges, 26,395 drops), all flagged in window 1, echoes
0.003 per service-hour, no other noise, 0.85 fresh-start false alarms per service.
**Margin:** the minimum rests are 240/240/240 (2 missed drops in 10,800); one grid step below
(180/180/180) recall falls to 91.8% / 88.7% / 58.7%. The chosen rests sit one step above the
minimum for the traffic types and 150 s above it for bad deploys.
**Generator check:** windows from `opsgen.Simulation` at 50/30/30 match the harness's (means
within 0.1%, spreads within 3%; `research/t_gen_vs_harness.out`).

**Fresh-start echoes (found here; accepted).** The first surge and the first drop on each
service echo almost every time: the model has seen a spike→spike pair but no spike→normal pair
yet, so its AR slope is near 1 and the forecast for the window after the spike stays at the
spike's level (`TRAFFIC_LOW` after a surge, `TRAFFIC_HIGH` after a drop). That is about 2.5
false alarms per `ops-sim` start, all in its first 5 minutes; `evaluate` counts them as
fresh-start noise, outside the false-positive rate. Nothing the replica varies removes them
except 10 s spikes; stricter confidence and rougher data reduce them. The spec's steady-state
"echoes near zero" holds (0.003 per service-hour).

**Competition sanity check** (our data and rests, our 150 s quiet period, different detectors;
200 seeds × 2 h):

| Detector | Recall per type | Echoes /svc-h | Fresh start /svc | Quiet data /svc-h |
|---|---|---|---|---|
| ours: AR(1) 12/360/99.95 | 100 / 100 / 100% | 0.004 | 0.84 | 0.47 |
| confidence 99 | 100% | 1.58 | 1.17 | 6.47 |
| confidence 99.5 | 100% | 0.40 | 1.13 | 3.34 |
| confidence 99.9 (D-029) | 100% | 0.014 | 0.94 | 0.85 |
| confidence 99.99 | 100 / 100 / 99.94% | 0.000 | 0.61 | 0.13 |
| Confluent's defaults as AR(1) (min 128, max 512, 99, refit 5) | 82 / 82 / 82% | 0.68 | — | 6.11 |
| `minTrainingSize` 6 | 100% | 0.004 | 1.18 | 0.47 |
| `maxTrainingSize` 60 | 100% | 0.31 | 0.84 | 0.91 |
| AR(2) / AR(3) | 100% | 0.000 | 2.61 / 2.83 | 0.48 / 0.50 |

Settings score visibly differently: false alarms span ×400, Confluent's default warm-up
(128 windows, 21 minutes) misses every incident in it, and higher AR orders are noisy at a
fresh start. Recall alone separates little, because the spikes are big: if the competition
should also rank on recall, smaller spikes are the knob (×3/×0.2/×3 cost only one more grid
step of surge rest). Confluent's real default is auto-ARIMA, which the replica can't model
(D-029: it chases level changes, so more echoes).

**Live shortlist (issue 08).**
1. The choice: AR(1), 12/360, **99.95**.
2. **99.99**, otherwise the same: the fewest false alarms offline (fresh start 0.61, quiet
   data 0.13), with 5 missed drops in 8,793 at the chosen rests. It shows how much recall
   margin the live model really has.
3. **ARMA(1,1)** (`p` 1, `d` 0, `q` 1) at 99.95: the replica can't model `q`. The MA term
   reacts to a spike's first residual, which might cancel the first-incident echo, the main
   false alarm left. Untested live (D-029 skipped it for cost). Never `p` = `d` = `q` = 0: it
   stalls live (D-029).
Fallback: 99.9 (D-029, live-proven; 77/h at margin, rests 420/240/300 for 60/h). Not
shortlisted: `d` 1 (follows level changes, D-029), STL (the data has no seasonality),
`minTrainingSize` 6 and AR(2) (above).
**What it changes in earlier decisions.**
- D-029: `confidencePercentage` 99.9 → 99.95; the rest of the config is unchanged.
- D-031: the traffic means are 50/30/30; `sweep.py`'s current profile and detector follow.
- D-042: the fixed duration is 20 s (2 windows). Spacing and cooldown stay until D-060.
- D-058: the spike sizes above; a surge no longer touches latency.
**Impact.**
- `opsgen/services.py` (means), `opsgen/engine.py` (`SURGE_LAMBDA_MULT` 5,
  `DROP_LAMBDA_MULT` 0.1, `SURGE_LATENCY_MULT` removed, `Config.duration` 20).
- `sql/2.3_anomaly_detection.sql`, `sql/2.3_anomaly_status.sql`,
  `sql/2.4_service_health.sql`: `confidencePercentage` 99.95. Running statements must be
  re-created to pick it up.
- `scripts/tuning/`: the replica's default confidence, `calibrate.py`'s default, `sweep.py`'s
  `CURRENT` and rate profiles.
- Tests: the detector config, the incident duration, the CLI defaults, the surge's
  untouched latency, the sweep's traffic level.
- Until D-060 lands, the old single-slot schedule (spacing 240–360 s, cooldown 420 s) runs
  with 20 s incidents; the rests above take effect with D-060.
- Evidence: `.scratch/simplify-demo/research/tune.py` and its `t_*.out` outputs.

Amends D-029, D-031, D-042 and D-058.

### D-060 · DECISION · Per-service schedules: overlap, per-type rests, first three one of each type
**What.** `ops-sim` had one global incident slot: one incident at a time, the next one
240–360 s after the last ended, and a 420 s cooldown per service. About 10 incidents an
hour, and the first surge could take up to an hour to appear. Its timings were CLI flags.
**Options.** (a) Keep the single slot with a shorter spacing: the slot caps the rate, and
the cooldown blocks the same services over and over; (b) one schedule per service, with
rests per service and type.
**Choice.** (b), decided with the user in the simplify-demo grilling session
(`.scratch/simplify-demo/spec.md`, issue 06):
- **One schedule per service.** Incidents on different services may overlap; a service has
  at most one at a time.
- **Gap and rests.** A service's next incident starts at the next 10-second boundary once
  **20 s** have passed since its last incident ended and at least one of its types is out of
  its **rest**. Rests are per service and type, measured from the end of that type's last
  incident on that service, the same on every service (D-059): `BAD_DEPLOY` **390 s**,
  `TRAFFIC_SURGE` **300 s**, `UPSTREAM_DROP` **300 s**. At a boundary, every service that can
  start one does.
- **First three one of each type.** Until all three types have happened, only types not
  seen yet are picked (order random). After that the type is drawn by `INCIDENT_WEIGHTS`
  (50/25/25) among the types allowed now, then a random service allowed for it.
- **Quiet period tied to the warm-up.** No incident before
  `(minTrainingSize + 1) × WINDOW_SECONDS + WINDOW_SECONDS` after start (the detector scores
  nothing for `minTrainingSize + 1` windows, plus one window of margin). That floor is 140 s
  with lab 2.3's `minTrainingSize` 12; the quiet period is **150 s**, as decided in the
  session, one more window of margin. A test reads `minTrainingSize` from
  `sql/2.3_anomaly_status.sql` and fails if the quiet period drops below the floor.
- **Spikes** are 20 s (D-059), start and end on window boundaries; a bad deploy deploys on
  its first tick and rolls back at its end (D-042), unchanged.
- **Timing lives in `Config` only.** `--quiet-period`, `--spacing-min`, `--spacing-max`,
  `--duration` and `--cooldown` are removed; `--seed`, `--no-incidents`, `--simulate`,
  `--dry-run` (and `--creds`) stay.
**Why.** Each service has its own detector model (`PARTITION BY service_name`, D-009), so
incidents on other services change nothing for it (`research/cross_service.py`); overlap
costs no accuracy. What limits the rate is same-service, same-metric repeats: the model
trains on its spike windows, and too many of them on one metric widen the band past the next
spike. Per-type rests control exactly that, and D-059 tuned them to about 60 incidents an
hour with every type caught. The first-three rule shows every incident type within the
first minute of a run, where it used to take up to an hour for a surge. Tying the quiet
period to the warm-up means every incident lands in windows the detector scores.
**Measured** (`Engine` alone, seeded): **60 incidents in the first hour for each of 500
seeds**, **58.5 an hour at steady state** (hours 2–3 of 200 three-hour runs), mix
44/19/37% (bad deploy, surge, drop), the same as the issue-05 harness (`research/tune.py`'s
`schedule`, D-059's 59/h). The harness loops over the free services in random order and draws
a type for each; the engine draws a type, then a service. The two give the same rate, mix and
per-service rates, and with the harness's 140 s quiet period the engine reproduces a
type-first copy of it incident for incident (300 seeds × 3 h). The harness's quiet period
was 140 s; at 150 s the numbers above are unchanged, so D-059's results carry over.
**Impact.**
- `opsgen/engine.py`: `RESTS`; `Config` loses `spacing_min`, `spacing_max` and `cooldown`,
  gains `gap` (20) and `rests`, and `quiet_period` is 150 (was 180). `Engine.active` is a dict
  by service; `effect_for` reads the service's own incident; `next_incident_at` and
  `_uniform` are gone; `last_type_end` (by service and type) and `seen` are new; `_eligible`
  excludes a service mid-incident and checks the gap and the type's rest; `_choose_incident`
  applies the first-three rule and is called until no service can start one. All
  scheduling randomness is still on the one scheduler rng, so `--seed` reproduces the
  schedule, but schedules differ from earlier runs.
- `opsgen/cli.py`: no timing flags. `opsgen/driver.py`: the `--simulate` banner prints
  `quiet`, `duration`, `gap` and the rests (or "no incidents").
- Ground truth and sinks are unchanged: records are per incident, and nothing assumed one
  open incident. `evaluate` and the wall already work per service.
- On a restart, incidents in flight lose their end record, now up to one per service.
  Accepted (spec): the wall keeps them open for 11 minutes; `evaluate` skips them.
- Tests (`opsgen/tests/`): overlap across services; at most one incident per service; ≥ 20 s
  between incidents on a service; no same-type repeat within its rest; the first three one
  of each type; starts on window boundaries; the warm-up floor; about 60 an hour; the chooser
  (rests, gap, weights, first three); the banner; `ops-sim --help` has no timing flags.

Amends D-042.
