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
**Amended by D-068.** Only a `ROLLBACK` carries a target. The capacity agent has no
`SCALE_OUT` and its Target line is always N/A, so the replica-count check and
`suggested_replicas` are gone; a target on any other action is still flagged.

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
**Amended by D-068.** `remediation_decisions` gets a ninth column, `ticket_id` (nullable, the
last), for the ticket responder's decisions; the metric INSERTs write `CAST(NULL AS STRING)`
there. Suggested Replicas is no longer computed in SQL: the capacity agent has no
`SCALE_OUT`.

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

**Amended by D-066.** The mirror is gone: one chart per metric, the incident a translucent box
behind the raw line, the detection and the answer marks under the plot.

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
**Amended by D-068.** Geist and Geist Mono, copied from the target design
(`inspiration/Ops wall redesign.html`) with their OFL licence, replace IBM Plex Sans. Still
vendored, no CDN.

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

**Amended by D-068.** `TRAFFIC_SURGE` is retired, so `SCALE_OUT` leaves the capacity agent:
it answers `INVESTIGATE_UPSTREAM` for `TRAFFIC_LOW` and `OBSERVE` for `TRAFFIC_HIGH`, and
Suggested Replicas is gone. Each remaining type keeps its one answer, on payments:
`BAD_DEPLOY` → `ROLLBACK`, `UPSTREAM_DROP` → `INVESTIGATE_UPSTREAM`. `SCALE_OUT` stays in the
vocabulary for the ticket responder.

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
**Amended by D-068.** A second answer key, `ticket_answer_key`: one record per support ticket
with its true severity and expected action, read only by the wall and `evaluate`.
`service_incidents` stays the only answer key for metric incidents. The generator state file
also keeps the two ticket-id counters.

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
**Amended by D-068.** Terraform creates four more tables: `support_tickets` and
`ticket_answer_key` (sources), `p1_tickets` and `ticket_triage` (the outputs of labs 5.1 and
5.2), chained after the existing ones in lab order; 17 resources per person.
`remediation_decisions` is recreated with `ticket_id`.

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

### D-053 · DECISION · "Refresh data" on the ops wall restarts the feed behind it (superseded by D-066)
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

**Superseded by D-066.** The button, `POST /refresh`, `Feed.restart()`, `clear()` and
`generation` are removed; a clean view means restarting the wall.

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

**Amended by D-061.** Above each lane sits a line chart of the raw `service_metrics` value,
one point per second, with a tick at each injected incident's start.

**Amended by D-062.** The page no longer rebuilds its DOM on every snapshot: rows, bars and
charts are kept and updated in place, keyed (block bars by the snapshot's new `key`).

**Amended by D-064.** The 15 s to detect was the 10 s window plus 5 s of watermark: the
deployments watermark only passed a window's end at the next 5-s heartbeat. With the
shorter watermarks a window closes on the tick at its end, ~10 s after an incident starts.

**Amended by D-066.** A block is no longer a bar: a cyan dot at its start, a line to its end
and an amber diamond at the answer, under the plot of its metric's chart. The summary is a
table.

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

**Amended by D-067.** `web-gateway` is no longer a candidate of any shared incident type:
`BAD_DEPLOY` goes to `checkout` and `payments`, `TRAFFIC_SURGE` to `checkout`, and
`UPSTREAM_DROP` to `payments` only (a traffic series with both surges and drops misses the
drops). web-gateway deploys on its own clock; its bad deploys are `BAD_DEPLOY_ERRORS`,
flagged by an error-rate threshold in lab 2.2.

**Amended by D-068.** checkout is no longer a metric service: it produces support tickets and
is never an incident candidate. `TRAFFIC_SURGE` is retired; payments keeps `BAD_DEPLOY` and
`UPSTREAM_DROP`.

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

**Amended by D-067.** Good deploys come back on web-gateway only, on its own deploy clock:
an error-rate threshold has no false alarm that could turn one into a wrong rollback.
checkout and payments still have none. A deploy now bumps past the highest version the
service has run, so a rolled-back version's number is never reused.

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

**Amended by D-061.** The wall's Rate and Errors come from the last 10 s of
`service_metrics` samples, not from `service_health`.

**Amended by D-067.** `error_count` is read again, by lab 2.2 only: web-gateway's error
rate over a 10-second window against a fixed 25 % threshold, written to
`service_error_rate`. `service_health` still has no error columns, and the ARIMA labs
(renumbered 2.3–2.5) still don't read it.

**Amended by D-068.** Two services write `service_metrics`, web-gateway and payments, so two
records per second: checkout produces support tickets instead. The surge's size is gone with
the surges.

### D-059 · DECISION · Tuned data and reference detector: 20 s spikes, 50/30/30, confidence 99.95
**What.** Workshop preparation: the data settings and **our** reference detector settings
for the simplified demo (`.scratch/simplify-demo/spec.md`, issue 05), chosen offline with the
replica for the issue-06 schedule (one schedule per service, incidents may overlap across
services, 20 s between incidents on a service, per-type rests, first three one of each type,
50/25/25; D-060). Attendees tune their own detectors later; that competition is out of scope.
**Options.** Varied with `scripts/tuning/research/tune.py` (the per-second data
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
- Evidence: `scripts/tuning/research/tune.py`; its `t_*.out` outputs are in
  `.scratch/simplify-demo/research/`.

Amends D-029, D-031, D-042 and D-058.

**Amended by D-064.** Detection now lands ~10 s after the start, not ~15 s: a 10 s spike is
still flagged only as it ends, so 20 s stays the shortest spike that is on when flagged.

**Amended by D-065.** Spikes are no longer a fixed 20 s: an incident lasts until an agent
resolves it, or a 35 s cap (`Config.max_duration`) that ends mid-window on purpose. Offline,
35 s has no echoes, where 30 s and 40 s (both on a boundary) have 80× and 670× those of 20 s.
The rests stay 390/300/300 s, counted from the scheduled end; the rate is ~56 an hour.

**Amended by D-068.** Surges are retired, and with them the ×5 surge size and the 300 s surge
rest. The rests are 390 s for bad deploys and 300 s for drops, on payments alone: about 19
incidents an hour, every one caught offline.

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
44/19/37% (bad deploy, surge, drop), the same as the issue-05 harness
(`scripts/tuning/research/tune.py`'s `schedule`, D-059's 59/h). The harness loops over the
free services in random order and draws
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

**Amended by D-065.** Incidents last up to 35 s (an agent's correct answer ends one early)
and so end mid-window. The gap and the rests count from each incident's scheduled end
(start + 35 s), not its actual end, so the schedule doesn't depend on what the agents do and
still follows `--seed`. About 56 incidents an hour; a service's shortest gap between
incidents is 25 s (T+35 to the boundary at T+60). The banner prints `max` for `duration`.

**Amended by D-067.** The shared scheduler serves `checkout` (bad deploys and surges) and
`payments` (bad deploys and drops) only: about 37 incidents an hour, 19 per service as
before. web-gateway has its own deploy clock with its own RNG (`seed + 2`): a deploy every 60–90 s,
bad with probability ½, outside the quiet period, the gap, the rests and the "one of each"
rule.

**Amended by D-068.** The shared scheduler serves payments alone, with two types, so the first
two incidents are one of each. About 19 incidents an hour (18.7 over 3-hour runs offline,
19.0 at steady state). Every seeded schedule shifts, because the scheduler draws differently.

### D-061 · DECISION · The wall charts raw `service_metrics` latency and traffic
**What.** The wall drew each metric as a lane: the injected incidents up, the detections
down. A viewer saw *that* an anomaly happened, never the data that made it one. The rail's
Rate was the newest `service_health` window's `traffic_count / 10`, and Errors had no source
since D-058 dropped the error columns.
**Options.** What to chart: (a) the `service_health` windows (the detector's 10-s input,
with its band and forecast), which need the attendee's labs and change with their detector
settings; (b) the raw `service_metrics` records, one point per second. The y scale:
(i) linear from 0, as issue 07 was written; (ii) logarithmic.
**Choice.** (b) with (ii). The scale was the user's call when reviewing the mockup
(`.scratch/mockups/wall-charts.html`, which keeps a Linear / Log toggle for comparison;
`.scratch/simplify-demo/issues/07-wall-charts.md`).
- **Consumer.** `service_metrics` is a fifth topic, read from 660 s back (the 10-minute axis
  plus the state's 60 s margin) with the existing seek by timestamp. `start_threads`
  attaches it with the others, so "Refresh data" (D-053) re-attaches it.
- **State.** `apply_metric` keeps each service's samples (`event_time`, `traffic_count`,
  `latency_ms`, `error_count`) in `event_time` order: a late one goes in its place, the same
  second twice keeps the newer. `trim()` drops them with the rest.
- **Trimming.** `OpsWallState.trim()` existed but nothing called it, so the state only grew,
  and an incident whose end record never came (a simulator restart, D-060) stayed on the
  wall for as long as it ran: a pending bar across the whole lane. The `Feed` now runs one
  more thread next to the consumers that calls `trim(time.time())` every 10 s; it stops and
  restarts with them ("Refresh data", D-053). The state keeps 11 minutes (the axis plus
  60 s) whatever the length of the workshop, and such an incident leaves the wall after
  11 minutes, as D-060 assumed.
- **Snapshot.** The `metrics` per service: the samples with `event_time` on the axis
  and not after `now`, as `{t, traffic, latency, errors}`, with `t` axis-relative and latency
  rounded to 0.1 ms. A sample stamped in the second still running (tick + 999 ms, D-058)
  waits for the next snapshot.
- **Rail.** Rate is the mean `traffic_count` of the samples of the last 10 s (one sample is
  one second, so requests per second); Errors is their `error_count` over their
  `traffic_count`, in %. No sample in the last 10 s → "—" for both; no requests → "—" for
  Errors. `HealthRow` loses `traffic_count`.
- **Page.** Above each Latency and Traffic lane, a 56 px hand-drawn SVG chart on the lane's
  `X()` axis: one point per sample, a plain line, no smoothing, broken over a gap of more
  than 1.5 s instead of bridging it. The y scale is log, from a round number just below the
  smallest visible value above 0 to one just above the largest (multiples of a fifth of the
  power of ten), at least ×10 apart; a second with 0 requests sits on the bottom edge. A band
  above the plot names the range (`28–280 rps · log`), so the label never covers data. A
  thin purple tick marks each injected incident's start, only on its metric's chart
  (`INCIDENT_METRIC`). No band, no forecast. Hover snaps a crosshair to the nearest second;
  the tooltip gives the value, how long ago, and the incident the second falls in. The
  hover is kept across the 4 Hz rebuilds of the DOM. The line is a new neutral token,
  `--metric` `#7F8CA8`: OKLab ΔE 15.5 from `--truth` (`#A3AEC6` was 11.1, under the 15
  floor). The legend gains "Value per second" and "Incident starts"; the subtitle and the
  footer mention the raw lines. Only latency and traffic get charts.
**Why.** Raw records show before any lab is done and the same whatever the attendee's
detector settings, which matters once attendees tune their own detectors. The incidents are
ratios (surge ×5, drop ×0.1, bad deploy ×4); on a log scale a ratio has the same height
wherever the service's normal level sits, so a drop on a 30 rps service reads as clearly as
a surge on a 50 rps one, and Poisson noise shrinks to a thin band. The ×10 minimum keeps a
chart with no incident on it from blowing plain noise up. Rate and Errors read the stream
the charts draw: only `service_metrics` has `error_count` (D-058), and the figures now move
with the line next to them.
**Measured.** Offline: the real `Simulation` (seed 7) fed into `OpsWallState` with
Avro-shaped timestamps, served by the real app, rendered in headless Chrome. At every
incident marker the first sample after it is changed and the last before it is not;
medians in the 20 s before against during: surges ×5.27–5.30, drops ×0.06–0.13, bad
deploys ×3.92–4.14. The snapshot carries about 1,800 samples (~95 KB, four times a second,
local). Through Kafka and a running `ops-sim`: issue 08.
**Impact.** `scripts/opswall/consumer.py` (`METRICS_TOPIC`, `METRICS_BACKREAD_SECONDS`, five
topics), `scripts/opswall/feed.py` (the trim thread, `TRIM_INTERVAL_SECONDS`),
`scripts/opswall/state.py` (`MetricSample`, `apply_metric`, `RATE_WINDOW_SECONDS`,
`HEALTH_RETAIN_MARGIN` renamed `RETAIN_MARGIN_SECONDS`, the snapshot's `metrics`, Rate and
Errors), `scripts/opswall/app.py` (docstring), `static/opswall.js` (line chart, log scale,
hover, legend), `static/opswall.css` (`--metric`, the chart's styles, the metric gap; the
dimmed label of a quiet metric is gone, since its line always has data),
`static/index.html` (subtitle, footer). `platform-team/README.md`: the wall reads five
topics. Each service row grows from about 200 px to about 310 px. Tests:
`tests/test_opswall_state.py` (samples on the axis, oldest first, late and repeated ones;
a future sample waits; `trim()` drops old samples and an incident that never ended; Rate
and Errors over 10 s, through a
bad deploy and a drop, and empty without samples or requests; health rows alone give no
rate; `clear()` drops samples), `tests/test_opswall_ingest.py` (real `ops-sim` records
through the Avro pair reach the charts and the rail; the consumer tails `service_metrics`
from 660 s back; the Phase 4 health rows give no rate), `tests/test_opswall_feed.py` (the
feed trims on the wall clock until stopped, and again after a restart).

Amends D-053, D-054 and D-058.

**Amended by D-062.** The page no longer rebuilds its DOM on every snapshot: rows, bars and
charts are kept and updated in place, keyed (block bars by the snapshot's new `key`).

**Amended by D-066.** The line and its lane are one chart; the incidents are boxes behind the
line instead of onset ticks. The rail's Rate and Errors are gone, and with them `rateRps`,
`errorRatePct` and the per-sample `errors`.

**Amended by D-068.** checkout has no charts: its row shows its tickets and agent runs.
web-gateway's and payments' charts keep their behaviour inside the target design's rows.

### D-062 · FIX · The ops wall updates its DOM in place instead of rebuilding it
**What.** On every snapshot (4 Hz, `PUSH_INTERVAL_SEC`) the page emptied `#rows` and built
every row again with empty chart hosts, which a ResizeObserver drew after the next layout.
For that one layout every host was 0 px tall, the page about 800 px shorter, and the browser
clamped the scroll position to it: the wall couldn't be scrolled, it flickered and jumped
back to the top. The rebuild also threw away the bar holding the keyboard focus or under the
pointer four times a second, so tabbing through bars and their tooltips didn't work.
**Options.** (a) Keep the rebuild and give each host its height up front (fixes the scroll
only); (b) update the DOM in place, keyed; (c) move the page to React or Preact, whose keyed
reconciliation does (b), at the price of a build step or a vendored runtime (D-040: no CDN).
**Choice.** (b), with (a)'s heights kept for a row's first draw. React was weighed and
declined: the page is ~550 lines, and stable keys for the bars are needed either way.
- **Keyed update.** `sync()` reuses a node by key, creates one for a new key and removes one
  whose key is gone. Rows are keyed by service name, incident bars by `incidentId`, block
  bars by a new snapshot field `key`, `"<trigger>@<first window_end in ms>"`: every other
  block time is axis-relative and clamped, so the page couldn't tell a block apart from one
  snapshot to the next.
- **Charts.** Each chart's `<svg>` and its layers are made once. A redraw empties the layers
  that only show things (grid, line, labels, time-to-detect marks) and updates the bars and
  the line chart's hit area in place.
- **Rail and summary.** Made once; a write is skipped when the text is unchanged.
- **Tooltip.** It follows the bar node: each snapshot refreshes its text (an answer arrived)
  or hides it when the bar has left the axis.
**Measured.** Headless Chrome, a 1400×700 window, real `OpsWallState` snapshots at 4 Hz:
before, scrolling stopped at 250 px (the shrunken page's maximum); after, it stayed at 400 px.
A focused open block stays the same node and keeps the focus as it is answered; its tooltip
turns from "Waiting for the agent" to "Answered…"; it hides when the block is gone; a line
hover follows the data. The same snapshot renders pixel-identical to before.
**Impact.** `static/opswall.js` (`sync`, `setText`/`setClass`/`setHTML`, `chartSvg`,
`createRow`/`updateRow` replace `serviceRow`, one module-level ResizeObserver, `refreshTip`),
`scripts/opswall/state.py` (the block's `key`). Tests: `tests/test_opswall_state.py` (a block
keeps its key as the wall moves; two blocks never share one).

Amends D-054 and D-061.

### D-063 · FIX · The ops wall reads uncommitted records
**What.** Health rows and agent decisions showed up on the wall up to about a minute after
the Confluent Cloud UI showed them in the topic. The wall's consumers read
`read_committed`. Flink's sinks are transactional and commit "approximately every minute"
([delivery guarantees](https://docs.confluent.io/cloud/current/flink/concepts/delivery-guarantees.html)),
so a committed reader waits up to one commit interval for each statement's rows. The UI
shows uncommitted records. `remediation_decisions` also has two writers on its one
partition: a committed reader can't get past the first record of either agent's open
transaction, so one agent's committed decision can wait behind the other agent's
transaction too.
**Options.** (a) Keep `read_committed` and live with the lag; (b) `read_uncommitted` on the
wall's consumers.
**Choice.** (b). The cost is that the wall can read records from a transaction Flink later
aborts and replays. Every applier already keeps one record per natural key (a health row
per window, a decision per window and agent, a sample per second, an incident per id), and
keeps the first detection and first write time, so a replayed row counts once. A replayed
agent call may answer differently; the newer answer wins, as for a re-run INSERT.
`uv run evaluate` (`scripts/common/topic_reader.py`) stays `read_committed`: it scores
after the fact and its 100 s tail covers the lag (D-021).
**Impact.** `scripts/opswall/consumer.py` (`_build_consumer`). Tests:
`tests/test_opswall_ingest.py` (the consumer reads uncommitted records).

### D-064 · DECISION · Shorter source watermarks: a window closes on the tick at its end
**What.** Every incident was detected 15 s after it started. Incidents start on a window
boundary and are flagged in their first window (D-059), so 10 s of that is the window. The
other 5 s were the watermarks: both sources trailed `event_time` by 5 s, and the temporal
join's watermark is the lower of the two. `service_metrics` (stamped tick + 999 ms, every
second) reached the window's last millisecond on the tick at 14 s; `service_deployments`
(stamped on the tick, written only on the 5-s heartbeat) not until the heartbeat at 15 s.
**Options.** (a) Keep 5 s; (b) 5-s windows and a 1-s watermark; (c) shorter watermarks,
10-s windows kept.
**Choice.** (c): `service_metrics` trails by 1 s, `service_deployments` by 1 ms. Both reach
a window's last millisecond on the tick at its end, so the window closes then: ~10 s from
an incident's start to its detection, plus processing.
**Why.**
- **(b) was measured offline and set aside.** Its detection would land at 10 s, not 6 s: a
  1-s deployments watermark still misses the window's end at the heartbeat on it (T+4 at
  T+5) and waits for the next one. And the detector gets worse. `sweep.py`'s generator and
  AR(1) replica (`scripts/tuning/research/window_*.py`; 6 seeds x 3 h, 1,062 incidents)
  caught every incident in its first window at both sizes, but a 20 s spike becomes 4
  windows, the fitted AR(1) coefficient rises from
  ~0.47 to ~0.73, and the first normal window after every traffic spike is flagged the
  other way: 0 -> 7.9 false alarms per service-hour, all echoes. `maxTrainingSize` 720 left
  7.5; 10 s spikes left 2.2. 5-s windows would need D-059's tuning again.
- **1 s and 1 ms are safe here.** Each topic has one partition and one producer writing in
  `event_time` order, so no row arrives late. 1 ms, not 0: the heartbeat stamps every service
  with the same tick, and Flink's strictly ascending strategy (`AS event_time`) would make
  all but the first of them late. A deploy is always read before the samples it applies
  to: the join emits a sample only once the deployments watermark has passed it, which
  takes a later record on the same partition.
- **The detector is untouched.** A watermark changes when a window is scored, not what is
  in it, so D-059's results stand.
**Impact.** `sql/source_service_metrics.sql`, `sql/source_service_deployments.sql` and their
copies in `platform-team/modules/developer/statements.tf`. `service_incidents` keeps 5 s: no
statement windows or joins it. Tests: `tests/test_sql_pipeline.py`
(`test_a_window_closes_on_the_tick_at_its_end` works out the closing tick from the two DDLs
and ops-sim's stamps and heartbeat), `tests/test_opswall_ingest.py` (the synthetic detection
delay is 1 s, was 6 s). Rolling out: the tables are `CREATE TABLE IF NOT EXISTS`, so drop
`service_metrics` and `service_deployments` and apply again, then `uv run reset-health` and
run the 2.4 INSERT and the agents again. **Pending (live check):** the wall's time to detect
drops from 15 s to ~10 s.

Amends D-054 and D-059.

### D-065 · DECISION · Close the loop: a correct agent answer resolves its incident
**What.** The demo's story is: an incident starts, the pipeline detects it, an agent decides
what to do. The ops wall showed all three, but the agent's decision changed nothing: the
generator kept degrading the service until the incident's fixed 20 s (D-059) ran out,
whatever the agent said. A `ROLLBACK` that rolled nothing back, or a `SCALE_OUT` after which
the surge carried on, made the last step of the story fake.
**Options.**
- Whether a decision acts at all: (a) keep agent decisions display-only; (b) ops-sim reads
  `remediation_decisions` and a correct answer ends its incident early.
- The length of an incident nobody fixes: a cap of 20 s (today), 30 s, 35 s or 40 s.
- Where an early end leaves the schedule: gap and rests dated from (i) the actual end;
  (ii) the scheduled end (start + cap).
- When a fix takes effect: on the next tick, mid-window; or snapped to a window boundary.
- How ops-sim reads the decisions: `read_committed` or `read_uncommitted`.
**Choice.** (b), decided with the user in a grilling session on 2026-11-15/16
(`.scratch/close-the-loop/spec.md`). An incident an agent ends is **resolved**; one that
reaches the cap **expires**. ("Anomaly" stays what the detector flags; the generator ends
*incidents*.)
- **Which decision resolves which incident.** A decision resolves the active incident on its
  `service_name` when its `action` is the incident's `expected_action` (`ROLLBACK`,
  `SCALE_OUT` or `INVESTIGATE_UPSTREAM`), its `trigger` is the incident's
  `expected_trigger`, its `window_end` is after the incident's `started_at` (so a stale
  decision for an earlier incident can't end a new one), and the incident is still active
  when the decision is read. The first match resolves it; later decisions for the same
  incident, decisions arriving after it ended, and decisions matching no active incident are
  ignored. `OBSERVE`, `INVALID` and a wrong action resolve nothing: the incident runs on, and
  a correct answer for a later window of it can still resolve it.
- **What resolving does.** The rule lives in the engine, `Engine.resolve(decision, now)`,
  pure, no Kafka; `Simulation.tick(now, decisions=())` applies the decisions before ticking
  the engine. A decision read before the tick at `now` makes that tick's metrics sample
  normal, and the incident ends on that tick (`ended_at` = `now`), mid-window, not snapped to
  a boundary. The end is an expiry, only sooner: a bad deploy fires its `ROLLBACK`
  deployment record on that tick, and the end record goes to `service_incidents` with the
  real `ended_at`. `ROLLBACK` always restores `previous_version`; the decision's `target` is
  ignored (`evaluate` scores targets, D-045). `SCALE_OUT` only ends the surge:
  `ready_replicas` and `desired_replicas` stay as they are (no ceiling exists, D-043, and
  `SCALE` is reserved for a later project). The console line for an end says how it ended:
  `resolved by release_agent ROLLBACK after 17 s` or `expired after 35 s`.
- **The cap: 35 s** (`Config.max_duration`, was `Config.duration` 20 s). It ends mid-window
  on purpose; a test pins it to a value that is not a multiple of 10.
- **The schedule is dated from the scheduled end** (`Incident.scheduled_end`, start + 35 s):
  the gap and the per-type rests count from there, not from the actual end. The rests stay
  390/300/300 s (D-059). Incidents still start on a 10 s window boundary.
- **Reading the decisions.** Live mode only. A consumer thread in `opsgen/decisions.py`
  reads `remediation_decisions` with the card's Kafka key, Avro through Schema Registry (the
  registered schema, as the wall does), a fresh group per launch, no offset commits,
  `read_uncommitted`, from the topic's latest offset when it is assigned, so old decisions
  are never replayed. It decodes each record onto a queue; the driver drains the queue
  before every tick. If the topic is missing or can't be read, ops-sim warns once, keeps
  retrying in the background, and keeps producing; meanwhile every incident expires at
  35 s. `--dry-run` and `--simulate` never connect to Kafka, so every incident runs its 35 s.
  There is no flag to turn the loop off: before lab 4.3 runs, there are no decisions to read.
**Why.**
- **35 s, not 30 or 40.** An incident that ends on a window boundary goes from a fully
  spiked window to a fully normal one in one step, and that first normal window gets the
  opposite traffic flag (an echo: the forecast is still pulled toward the spike, D-059). At
  35 s the return to baseline splits over two windows, in two steps small enough to stay
  inside the band. Measured
  offline (table below): 35 s has no echoes, where 30 s and 40 s, which end on a boundary,
  have 80× and 670× today's. It also removes almost all of D-059's fresh-start echoes (0.06
  false alarms per service against 0.91). 40 s also starts to miss drops (57 in 122,000).
- **Scheduled end, not actual end.** The schedule (which service, which type, when) is then
  the same whatever the agents do, and the live schedule still follows `--seed`. Dated from
  start + 35 s, the rate falls from ~59 to **~56 incidents an hour**; the mix stays
  44/19/37%. `Engine` alone, seeded: 54 in the first hour for each of 20 seeds, 57 an hour
  at steady state.
- **Starts stay on a boundary** so the first window is all spike and detection stays in the
  first window (100% in every scenario below).
- **The fix lands on the next tick, mid-window.** Snapped to a boundary, every fix would
  end on one and lose the mid-window benefit (row H below: a fix exactly at T+20).
- **`read_uncommitted`** because Flink commits about once a minute (D-063): a committed
  reader would see every decision after its incident had already expired. Accepted cost: a
  decision from a transaction Flink later aborts can already have resolved an incident; its
  replay finds the incident ended and is ignored.
- **The loop never stops the generator.** A missing or unreadable topic only means
  incidents expire, as they did before.
**Evidence.** `scripts/tuning/research/close_loop.py`, output in
`.scratch/close-the-loop/research/close_loop.out`: the real generator through the AR(1)
replica at the labs' settings (12/360/99.95), scored with `evaluate`'s code, 2000 seeds × 3 h
per scenario, the same seeds everywhere. A fix is modelled by ending the incident at the fix
tick while the gap and rests are dated from start + cap.

| Scenario | Incidents/h | Recall | Echoes /svc-h | Fresh-start false alarms /svc | Agent calls /incident |
|---|---|---|---|---|---|
| 20 s (today) | 59.0 | 100 % | 0.003 | 0.91 | 1.02 |
| 30 s | 57.9 | 100 % | 0.239 | 1.00 | 1.02 |
| **35 s, no fix** | **55.9** | **100 %** | **0.000** | **0.06** | **1.02** |
| 40 s | 55.9 | 99.98 % | 1.997 | 0.99 | 1.02 |
| 35 s cap, fixed at T+15 | 56.0 | 100 % | 0.000 | 0.07 | 1.02 |
| 35 s cap, fixed at T+18 | 56.0 | 100 % | 0.000 | 0.11 | 1.02 |
| 35 s cap, 80 % fixed at T+18 | 56.0 | 100 % | 0.002 | 0.10 | 1.02 |
| H: 35 s cap, fixed at T+20 | 56.0 | 100 % | 0.006 | 0.94 | 1.02 |

The harness reproduces D-059 (20 s at 99.9: 0.012 vs 0.013 echoes; 30 s: 1.71 vs 1.49).
**Caveats:** after a big jump the replica's band is up to 1.5× wider than Confluent's, so
live echo suppression may be weaker; a fix exactly on a boundary loses the benefit (row H,
still no worse than today); fixes at T+21 to T+29 were not tested.
**What does not change.**
- No schema, DDL or Terraform change; no Flink statement changes. `service_incidents` gets
  no new column.
- The ops wall: the incident box already ends at `ended_at`, and the raw per-second chart
  (D-061) shows the spike ending right after the block closes. D-054's 30 s ownership and
  missed limits work unchanged with 35 s incidents.
- `uv run evaluate`: only the comment on `UNLOGGED_TAIL_SECONDS` changes; 35 s fits its
  100 s tail. No "resolved vs expired" count: without a column it could only be guessed
  from durations. **Accepted scoring gap:** in the part-spike last window of a traffic
  incident, an opposite flag counts as neither a false alarm nor a wrong direction
  (`Incident.at_end`), though it is still an agent call that
  `scripts/evaluation/decisions.py` attributes to the incident. Measured on 0.2–0.4% of
  incidents (the fixed scenarios above).
- A restart still drops the incidents in flight (D-060).
**Impact.**
- `opsgen/engine.py`: `Config.duration` (20) becomes `Config.max_duration` (35);
  `Incident.scheduled_end` (start + `max_duration`, whole seconds); `degradation_end` starts
  equal to it; `_end_incidents` dates `last_incident_end` and `last_type_end` from
  `scheduled_end`; the module docstring and the `RESTS` comment (about 56 an hour).
- `opsgen/driver.py`: the `--simulate` banner prints `max=35s` (was `duration=20s`).
- `scripts/evaluation/detection.py`: the `UNLOGGED_TAIL_SECONDS` comment.
- `scripts/tuning/research/close_loop.py` and `window_echoes.py` set `max_duration`;
  `close_loop.py`'s self-check compares the stock engine with scenario C (35 s, no fix),
  since the stock engine is no longer 20 s. Research scripts that run the stock `Config`
  (`sweep.py`, `window_compare.py`, `window_phi.py`) now run 35 s incidents, so their
  earlier outputs no longer reproduce exactly.
- Tests (`opsgen/tests/test_engine.py`): every incident starts on a boundary and lasts
  35 s; the cap is not a multiple of 10; about 56 an hour (54 in the first hour, 57 at
  steady state); the shortest gap between incidents on a service is 25 s (T+35 to the
  boundary at T+60); the gap and the rest count from `scheduled_end`.
  `opsgen/tests/test_output.py`: the banner. Test helpers that build an `Incident` pass
  `scheduled_end`.
- `opsgen/engine.py`: `Decision` (`service_name`, `window_end` in epoch seconds,
  `agent_name`, `trigger`, `action`; no Kafka types, no `target`); `Incident.resolved_by`
  (`"<agent_name> <action>"`); `Engine.resolve(decision, now) -> bool` sets
  `degradation_end` to `now`, so `_end_incidents` ends the incident (and rolls a bad deploy
  back) on that tick. `tick` is unchanged. The module docstring states the resolve rule.
- `opsgen/simulation.py`: `Simulation.tick(now, decisions=())` applies the decisions, in
  order, before the engine ticks.
- `opsgen/sinks.py`: `format_event`'s `INCIDENT END` line appends how it ended, after the
  rollback for a bad deploy: `rollback 4.2.2 -> 4.2.1, resolved by release_agent ROLLBACK
  after 17 s`, or `expired after 35 s`.
- `opsgen/groundtruth.py`: unchanged; `end_record` already writes `ended_at` from
  `degradation_end`, and a test pins it.
- Tests: `opsgen/tests/test_engine.py` (a correct decision ends each type on the tick that
  reads it, a bad deploy rolled back to its previous version; `OBSERVE`, `INVALID` and the
  wrong capacity action leave it running to 35 s; a correct answer after an `OBSERVE`
  still resolves; a wrong trigger, a `window_end` at or before the start, another service:
  ignored; only the first correct decision counts, and none after the incident expired; the
  schedule is identical whether every incident is resolved at T+15 or none is),
  `opsgen/tests/test_simulation.py` (the end record's `ended_at` and a bad deploy's
  `ROLLBACK` deployment record land on the resolving tick; that tick samples normal metrics
  and the one before is still degraded), `opsgen/tests/test_output.py` (the resolved line;
  without decisions, every `--simulate` incident expires after 35 s).
- `opsgen/decisions.py` (new): `DecisionFeed(card)` tails `remediation_decisions` on one
  daemon thread. Its consumer (`_build_consumer`) uses the card's Kafka key over SASL_SSL, a
  fresh `ops-sim-decisions-<hex>` group per launch, `enable.auto.commit` off and
  `read_uncommitted`; once the topic is found, every partition is assigned at `OFFSET_END`.
  Values are decoded against the registered schema (`make_deserializer`, as the wall does)
  by `decode_decision`, with `window_end` through `epoch_seconds` (a naive datetime is UTC,
  D-048). `drain()` returns what was queued since the last call, in order, without
  blocking. A missing topic, a failed lookup, assign or poll, a broker error on a message,
  a Schema Registry failure and a record that doesn't decode (or lacks a field) are each
  warned once on stderr; the feed retries every 3 s and skips bad records. `stop()` waits
  about one poll (1.5 s) and no longer: a thread stuck in a metadata lookup to an
  unreachable broker is a daemon and ends with the process, so Ctrl-C stays quick.
- `opsgen/driver.py`: `tick_once(sim, sink, now, decisions, st_path)` is one second of live
  output (drain the feed, tick, emit records, events and ground truth, save the state);
  `run_realtime(..., decisions=None)` calls it every second and stops the feed on the way
  out.
- `opsgen/cli.py`: live mode builds `DecisionFeed(card)` after the `KafkaSink`, prints
  "Reading remediation_decisions: a correct agent answer resolves its incident.", and
  passes the feed to `run_realtime`. `--dry-run` and `--simulate` build none; no flag.
  Module docstring updated.
- Tests: `opsgen/tests/test_decisions.py` (a real Phase 4 decision round-trips through the
  Avro pair against the table's registered schema shape, whose columns are checked against
  `sql/sink_remediation_decisions.sql`; with a fake consumer, the feed queues every decision
  and `drain` hands them over once, in order; a missing topic and an unreachable broker are
  each warned once and retried; a record that doesn't decode is skipped with one warning; a
  poll error is warned once per kind, and the end of a partition is no error; the consumer
  built from the card reads uncommitted, never commits, gets a fresh group and is assigned
  at `OFFSET_END`; the driver's tick step hands the drained decisions to `sim.tick`, so a
  resolving tick emits the normal sample, the `ROLLBACK` deployment record and the end
  record; `--dry-run` and `--simulate` build no feed, and live mode builds one and says so).
  `opsgen/tests/test_incident_records.py`: its `sim.tick` wrapper passes the decisions on.
**Pending (live check).**
- The real time from an incident's start to its resolution. Detection lands when the first
  window closes (~T+10, D-064); the answer time was estimated at ~8 s and never measured.
- The live echo rate and fresh-start false alarms with 35 s incidents.

Amends D-059 and D-060.

**Amended by D-068.** A ticket decision (`ticket_id` set) resolves nothing: it is about
checkout, which never has an incident. There are no surges, so no `SCALE_OUT` ends one.

### D-066 · DECISION · The ops wall redesign: one chart per metric, floating cards, Confluent dark
**What.** The user asked for a calmer wall that says what to expect: the summary sentence
changed shape with the numbers, the legend was a run of inline items, and each service row
carried text nobody read (subtitle, footer, the tier pill, Rate, Errors, the "Detected 10s
after onset" note). Each metric had a line chart above a separate lane of bars. Agreed in a
grilling round (Q1–Q25); the spec is `.scratch/opswall-redesign/spec.md`. Design source: the
dark stage of `dimi-vo/demo-confluent-intelligence-f1/docs/site/pre-provisioned.html`.
**Options.** For the chart: (a) keep the line above the lane; (b) merge them: the incident
as a box behind the line, the detection and the answer as marks under the plot. For the
answer marker: (a) one shape for every action, the action in the tooltip; (b) a hollow shape
for observe / invalid. For Refresh: (a) hide the button only; (b) remove it end to end.
**Choice.** Chart (b), marker (a), Refresh (b).
- **Page.** Dark only (D-035), midnight `#040531` with a 20-px dot grid, text `#E8EEF5` and
  its mixes into midnight, Confluent cyan `#38CCED` for detection, purple `#B69CFF` for
  incidents, amber `#F2B33D` for answers, red for anomalies, green only for status and the
  LIVE lamp. IBM Plex Sans stays (D-040; Plex is IBM's face, and the logo reads "an IBM
  Company"). The header has the Confluent lockup (`static/confluent-logo.svg`) and "Ops
  wall"; the favicon is the mark (`static/confluent-mark.svg`), both vendored from the
  design source. The LIVE pill has a real lamp: green with a steady glow, red while
  reconnecting, grey before the first connection.
- **Tables.** A summary table, one row per field, Metric | Value (Injected · Answered ·
  Open · Missed · False alarms · median time to detect and to answer in seconds; whole
  numbers; an empty field reads 0; caption "Last 10 minutes"), beside a seven-row legend
  table, each symbol drawn like its mark.
- **Layout.** Inside side margins of 8% of the window (20–160 px), three columns: the
  summary (25%), the cards (50%) and the legend (25%), the cards starting right under the
  header and the two side panels staying in view while the cards scroll. The content is
  capped at 1680 px so an ultra-wide screen doesn't stretch it. Below 1200 px the two
  tables sit side by side above the cards; below 760 px everything stacks.
- **Cards.** One floating card per service: name and status in its header; the
  Latency and Traffic charts at full width; one time axis per card instead of the page's
  sticky axis. An open anomaly gives the card a red glow.
- **Chart.** Back to front: a purple box per incident on that metric at 30% opacity, the
  full plot height (dashed when missed); the raw line, unchanged (D-061); under the plot, per
  detection block (D-054), a cyan dot at detection (hollow for a false alarm), a cyan line to
  the agent's first decision (to now while it waits) and an amber diamond there. The diamond
  sits one row below the dot: an answer lands about a second after detection, 2 px away,
  and on one row the diamond hid the dot. No grid lines, "now" line, onset tick or
  time-to-detect bracket. Hovering the plot inside a box adds the incident to the value's
  tooltip; a box and a mark group (dot, line, diamond) are each a tab stop with their own.
- **Refresh removed.** The button and its JS, `POST /refresh`, `Feed.restart()`,
  `OpsWallState.clear()` and `generation`. `Feed` keeps starting the consumers and the
  trimmer. `uv run reset-health` now says to restart the wall for a clean view. The user
  will design a new way later.
- **Snapshot pruned.** `rateRps`, `errorRatePct` (`_rate_and_errors`,
  `RATE_WINDOW_SECONDS`), the per-sample `errors` (`MetricSample.error_count`), `tier`,
  `replicas` and `version` (also off `ServiceState`) had no reader left. The pipeline's
  columns stay. `previousVersion`, `lastChange` and `secondsSinceDeploy` were unread before
  this change and stay; they are now all the wall takes from `service_deployments`. The
  user keeps that consumer for now; whether the wall shows deployments again or stops
  reading the topic is to be reviewed by 2026-11-23, ahead of the 2026-12-08 hackathon
  (`.scratch/opswall-redesign/issues/01-review-service-deployments-feed.md`).
**Why.** One chart per metric puts the injected incident, the detection and the answer on
the same time line as the raw value, which is the question the wall exists to answer
(D-036). Tables with fixed columns show what the wall can say before it has said it.
**Measured.** `uv run ops-wall` against the live cluster, headless Chrome at 1440 and 820 px
wide: the cards, the tables and the stacked narrow layout render; a box hover, a mark hover
(action, target, reason) and a keyboard-focused box each show their tooltip.
**Impact.** `scripts/opswall/static/{index.html,opswall.css,opswall.js,confluent-logo.svg,
confluent-mark.svg}`, `scripts/opswall/{state,server,feed,app,consumer}.py`,
`scripts/reset_health.py`. Tests: `tests/test_opswall_{state,server,feed,ingest}.py` (the
refresh, clear, rate, errors and tier tests removed; the feed's start and stop and a
failed start tested on their own), `tests/test_reset_health.py`. No JS test setup exists;
the page is a manual check.

Supersedes D-053. Amends D-036, D-054 and D-061.

**Amended by D-067.** web-gateway's card shows one chart, errors per second in % (linear,
0–100 %, a line at 25 %), with a mark at every deploy and rollback; checkout and payments
keep latency and traffic. The per-sample `errors` comes back for that chart, and the wall
reads `service_error_rate`. The deploy marks settle part of the `service_deployments`
review: the consumer stays.

**Amended by D-068.** The wall takes the frame of `inspiration/Ops wall redesign.html`: one
full-width row per service with an Issues panel, the dotted background, Geist and the
design's palette. The summary table, the legend and the Confluent logo go. checkout's row
shows its ticket sources, its ticket list and the two agents' lanes. The charts keep their
token names on the nearest new colours: detection is blue `#8FB8FF`, answers amber
`#F5B544`, and the incident boxes take the red `--crit`, since purple's nearest is that blue.

### D-067 · DECISION · web-gateway is detected by an error-rate threshold
**What.** Every service was detected the same way: `ML_DETECT_ANOMALIES` (ARIMA) on
10-second windows of latency and traffic. That is the hardest detector in the workshop, and
it is the first one attendees meet. The labs should build up: begin with something simpler
of the same shape (the simulator produces metrics, a Flink statement inspects them and flags
the agent, the agent's answer closes the loop, D-065), then move to ARIMA.
**Options.**
- The signal: (a) `error_count`, display only until now (D-058); (b) latency or traffic
  against a fixed bound.
- The window: (a) 10 s tumbling; (b) 10 s sliding, moving 1 s at a time.
- Where the flag goes: (a) its own narrow table; (b) error columns on `service_health`,
  written by a second INSERT.
- web-gateway's bad deploy: (a) a new incident type; (b) `BAD_DEPLOY` with a per-service
  effect and trigger.
- web-gateway's schedule: (a) its own deploy clock; (b) the shared scheduler with a shorter
  rest.
- Surges, whose only candidate was web-gateway: (a) surges on checkout, drops on payments;
  (b) both on both services, the first proposal; (c) drop them.
**Choice.** (a) each time, decided with the user in a grilling session on 2026-11-16
(`.scratch/web-gateway-threshold/spec.md`). The story for web-gateway: a deploy happens, a
lot of requests start failing, so it must have been a bad deploy; the release agent answers
ROLLBACK and the errors stop.
- **web-gateway** leaves the shared scheduler and gets its own deploy clock, with its own
  RNG (`Random(seed + 2)`). The first deploy is on the first window boundary at or after
  start + 60 s and is good; the second is bad. After that, each deploy is on a boundary 60,
  70, 80 or 90 s (uniform) after the previous one started, bad with probability ½, never
  three good in a row (so 4/7 of the deploys are bad in the long run). The clock is dated from the starts, so it is the same however
  incidents end. The quiet period doesn't apply. A good deploy is a deployment record
  (`last_change = DEPLOY`) and nothing else. A bad deploy is the same record plus a
  `BAD_DEPLOY_ERRORS` incident: 40 % of requests fail, traffic and latency stay normal,
  expected trigger `ERROR_RATE_HIGH`, expected action `ROLLBACK`, capped at 35 s and
  resolved like any incident (D-065). `--no-incidents`: web-gateway doesn't deploy.
- **checkout and payments** keep the shared scheduler and its three types: bad deploys on
  both, surges on checkout, drops on payments (`BAD_DEPLOY` loses web-gateway,
  `TRAFFIC_SURGE` moves from web-gateway to checkout). Their bad deploys keep latency ×4 and
  12 % errors. The quiet period, the "first three are one of each" rule, the gap and the
  rests stay. Each service still gets about 19 incidents an hour, 37 in all (was 56 on three).
  The grilling first agreed on all three types on both services; the offline re-check below
  found it lost the drops, and the user chose the split on 2026-11-17.
- **Versions never repeat.** A deploy bumps the patch past the highest version the service
  has run, on every service. Before, it bumped the current version, so the first deploy
  after a rollback reused the bad version's number. The generator state file (D-046) keeps
  the highest version; a file without it uses the higher of the saved `version` and
  `previous_version`.
- **Lab 2.2** (new): the 2.1 temporal join plus `error_count`, web-gateway only, a 10 s
  `TUMBLE`, `error_rate = SUM(error_count) / SUM(traffic_count)`, and `error_status` `HIGH`
  when `SUM(error_count) >= 0.25 * SUM(traffic_count)` (never with no requests), else
  `NORMAL`. Every window goes to a new table, `service_error_rate` (12 columns: service,
  window, `traffic_count`, `error_count`, `error_rate`, `error_status`, the deployment
  context, `detected_at`), created by the platform team's Terraform like the others (D-050).
- **ARIMA** moves to labs 2.3–2.5 (the files are renamed) and reads
  `WHERE m.service_name IN ('checkout', 'payments')`.
- **The agent.** The release agent takes `ERROR_RATE_HIGH` too: a second INSERT,
  `4.3_insert_release_agent_errors.sql`, reads `service_error_rate` where
  `error_status = 'HIGH'` and runs before the latency INSERT. The prompt names both
  triggers; its decision rules don't change. The capacity agent's `SCALE_OUT` example moves
  off web-gateway.
- **The wall.** web-gateway's card shows one chart, errors per second in % (linear, 0–100 %,
  a line at 25 %), with a mark at every deploy and rollback. The wall reads
  `service_error_rate`.
- `uv run evaluate` scores `ERROR_RATE_HIGH`; `uv run reset-health` covers
  `service_error_rate`.
**Why.**
- A fixed threshold on one metric is the simplest detector with ARIMA's shape, and errors
  are what a bad deploy is known by.
- Tumbling: incidents start on a window boundary, so the first window after a bad deploy is
  bad for all 10 s: one detection, one answer. A sliding window fires about 5 s sooner, but
  then every second until the rollback and for a few seconds after it, calling the agent
  each time; and it would be a new kind of window in the simplest lab.
- A narrow table keeps the first INSERT small. On `service_health` it would fill most of 22
  columns with typed NULLs.
- The engine and the wall key everything by incident type (candidates, effect, trigger,
  action, rest, `INCIDENT_METRIC`). A new type fits that; one `BAD_DEPLOY` with per-service
  effects would add per-service exceptions in the engine, the wall and `evaluate`.
- In the shared scheduler, web-gateway would share the RNG (its draws would shift checkout's
  and payments' schedule for a seed), the "one of each" rule (a type only one side can take
  can hold the other side back) and the quiet period (ARIMA's warm-up, which a threshold
  doesn't need). On its own clock its bad deploys can come often, about 27 an hour (4/7 of
  about 48 deploys) against about 8 per service before, without touching the other two.
- Good deploys, back on web-gateway only, show that the rule reacts to the errors, not to
  the deploy. D-057 removed them because an ARIMA latency false alarm soon after one rolled
  back a healthy version; a 0.5 % baseline against a 25 % threshold has no such false alarm.
  The reason still holds for checkout and payments.
- With good deploys, "3.4.4 is bad, rolled back, then 3.4.4 deployed again and fine" would
  contradict itself.
- Surges move rather than go, so the capacity agent keeps both of its answers and ARIMA's
  traffic detector keeps seeing changes in both directions. They go to one service and drops
  to the other because a traffic series with both loses the drops: ×5 surges widen its band,
  which is symmetric, until the lower bound is below zero (about −500 requests per window on
  checkout), and a ×0.1 drop to about 30 never falls under it. Before, the split by service
  came from the story (the edge gets surges, the backends get drops) and kept each series
  spiking one way without anyone writing down why.
**Measured.** Offline, the share of web-gateway windows (50 rps, about 500 requests) at or
above 25 % when *k* of their 10 seconds fail at the bad-deploy rate and the rest at 0.5 %;
`opsgen.distributions`, 20,000 windows per cell, seed 1
(`.scratch/web-gateway-threshold/research/threshold_check.py`):

| bad rate | k = 10 | k = 5 | k = 3 | k ≤ 2 |
|---|---|---|---|---|
| 0.30 | 99.38 % | 0 % | 0 % | 0 % |
| 0.35 | 100 % | 0.01 % | 0 % | 0 % |
| **0.40** | **100 %** | **0.54 %** | 0 % | 0 % |
| 0.45 | 100 % | 11.6 % | 0 % | 0 % |
| 0.50 | 100 % | 55.3 % | 0 % | 0 % |

k = 10 is the first window after a bad deploy. k = 1–3 is the window the rollback lands in
when an agent answers (about 1 s after detection, D-066). k = 5 is the window an expired
incident ends in: at 0.40, 0.5 % of those are flagged again, with `last_change = ROLLBACK`,
so the release agent answers OBSERVE.

ARIMA on checkout and payments, offline (`scripts/tuning/research/shared_scheduler.py`, output
in `.scratch/web-gateway-threshold/research/shared_scheduler.out`): the real generator through
the AR(1) replica at the labs' settings (12/360/99.95), scored with `evaluate`'s code, 300 seeds
× 3 h per run, same seeds, no fix:

| Candidates | Inc/h | Mix BD/SURGE/DROP % | Bad deploys | Surges | Drops | Echoes, other FPs | Fresh /svc |
|---|---|---|---|---|---|---|---|
| Before, all three services scored (D-065 row C) | 55.9 | 44/19/37 | 100 % | 100 % | 100 % | 0, 0 | 0.05 |
| Before, checkout and payments scored | 37.3 | 44/0/56 | 100 % | – | 100 % | 0, 0 | 0.04 |
| All three types on both (rejected) | 57.9 | 28/36/36 | 100 % | 100 % | **1.6 %** | 0, 0 | 0.04 |
| **Surges on checkout, drops on payments** | 37.3 | 44/28/28 | 100 % | 100 % | 100 % | 0, 0 | 0.04 |

Every incident fixed at T+11 (an answer about 1 s after detection) gives the same recall and
no echoes. The harness reproduces D-065's row C (55.9 an hour, 100 %, no echoes; fresh-start
0.05 against 0.06).
**Pending (live).** The time from a web-gateway bad deploy to its rollback; the release
agent's answers to `ERROR_RATE_HIGH` (ROLLBACK, right target); ARIMA's recall, false alarms
and echoes on checkout and payments with surges.
**Open.** Lab 2.2 now teaches `TUMBLE` first, so the windowing-only step of the old 2.2
(`2.3_window_metrics.sql` after the rename) may be redundant. Kept for now.
**Impact.** Issue 01 (versions): `opsgen/deployments.py` (`DeploymentState.highest_version`,
`highest()`), `opsgen/simulation.py` (restores it, `state_snapshot` saves it),
`opsgen/state.py` (docstring). Tests: `opsgen/tests/test_deployments.py` (no version
reused after a rollback, numeric order), `opsgen/tests/test_engine.py` (a restart keeps the
highest version; a state file from before D-067).
Issue 02 (the shared scheduler): `opsgen/engine.py` (`SHARED_SERVICES`, `CANDIDATES`, the
docstring's rules and rate). `scripts/tuning/sweep.py` and `scripts/tuning/research/tune.py`
score checkout and payments only (`ARIMA_SERVICES`; `tune.py`'s `TYPES_OF` follows the new
candidates); `scripts/tuning/research/close_loop.py`'s scoring is split out as `score_run`
(same output, checked on three scenarios) for the new `shared_scheduler.py`; the research
README. Tests: `opsgen/tests/test_engine.py` (the candidates; web-gateway gets no shared
incident; about 37 an hour; the gap/rest, chooser and resolve tests on checkout and payments),
`opsgen/tests/test_simulation.py` and `opsgen/tests/test_output.py` (surges on checkout,
drops on payments, two services open at once), `tests/test_tuning.py` (a quarter-hour's
traffic bound).
Issue 03 (web-gateway's deploy clock): `opsgen/engine.py` (`BAD_DEPLOY_ERRORS`,
`BAD_DEPLOY_TYPES`, its trigger, action and effect; `DeployClock`; `Config.deploy_clock_first`
and `deploy_clock_gaps`; `Engine.deploy_rng` and `_deploy_on_the_clock`; the deploy and
rollback branches take both bad-deploy types; the docstring), `opsgen/simulation.py` (the
clock's rng, `Random(seed + 2)`), `opsgen/sinks.py` (the `deploy` event's `DEPLOY` line; a
`BAD_DEPLOY_ERRORS` end shows its rollback), `opsgen/driver.py` (the banner names the clock),
docstrings in `opsgen/{deployments,services,distributions,cli}.py`. `scripts/tuning/sweep.py`
returns only the shared scheduler's incident types, the ones ARIMA is meant to catch;
`shared_scheduler.py` turns the clock off for its pre-D-067 runs, and its 300-seed output is
unchanged. Tests: the new `opsgen/tests/test_deploy_clock.py` (the opening; boundaries 60-90 s
apart; never three good in a row, 4/7 bad; no deploy inside an incident; the clock ignores how
incidents end and never moves the shared schedule; an `ERROR_RATE_HIGH` answer resolves a bad
deploy and a `LATENCY_HIGH` one doesn't; the start record; good and bad deploy records look
alike; 40 % errors with traffic and latency unchanged; `--no-incidents`); `test_engine.py`
(its `_run` is the shared scheduler alone; the cap and the resolve-or-not schedule include
the clock), `test_simulation.py` (a resolved `BAD_DEPLOY_ERRORS` and its records),
`test_output.py` (the banner, deploy lines per service, the quiet period with the clock's
opening, about 40 % errors only in a bad deploy's windows, the `deploy` JSON). Until issues 07
and 08, the wall and `evaluate` count each `BAD_DEPLOY_ERRORS` as missed.
Issue 04 (the table): `sql/sink_service_error_rate.sql` (new, 12 columns); its copy in
`platform-team/modules/developer/statements.tf`, chained after `service_incidents` and before
`service_health` (lab order), and in the module's `flink_statements` output; two column
comments that D-067 made stale, in `sql/` and `statements.tf` alike
(`service_metrics.error_count` is no longer display only; `remediation_decisions.trigger`
and `window_end` name `ERROR_RATE_HIGH` and `service_error_rate`). `OUTPUT_TOPICS`
(`scripts/common/topics.py`), so `uv run reset-health` stops `INSERT INTO service_error_rate`
statements and empties the topic, and `uv run reset` empties it; the readiness check's
`OUTPUT_TABLES` (`scripts/preprovisioned/provision.py`); the table counts in docstrings,
`platform-team/{main.tf,variables.tf,README.md}` (13 resources per person). Tests:
`tests/test_platform_team_sync.py` (six tables), `tests/test_reset_health.py`,
`tests/test_reset.py`, `platform-team/tests/developer_module.tftest.hcl` (the statement key;
the run block is now `the_three_output_tables_are_created`). Offline only: `terraform fmt`,
`validate` and `test` pass; the user runs the apply, which creates the table. The two
comment changes reach an existing table only after a DROP TABLE, which nothing needs.
Issue 05 (the labs): `sql/2.2_error_rate.sql` (new: the 2.1 temporal join plus
`error_count`, web-gateway only; a `windowed` CTE sums the window's requests and errors and
takes the deployment context with `LAST_VALUE`; the outer SELECT derives `error_rate`,
`error_status` and `detected_at`, the same shape as the ARIMA status query) and
`sql/2.2_service_error_rate.sql` (its INSERT). The ARIMA files are renamed (`git mv`):
`2.2_window_metrics` and `2.2_window_aggregation` → 2.3, `2.3_anomaly_detection` and
`2.3_anomaly_status` → 2.4, `2.4_service_health` → 2.5; each reads
`service_name IN ('checkout', 'payments')` and its header names its new number. References
to the old names and numbers: `opsgen/{engine,driver}.py`, `scripts/{evaluate,reset}.py`,
`sql/sink_service_health.sql`, the tuning research README, the README. Tests:
`tests/test_sql_pipeline.py` (the renamed files; ARIMA covers checkout and payments only and
no lab runs `ML_DETECT_ANOMALIES` on web-gateway; for 2.2: web-gateway only, the ratio of
sums at 0.25 with the zero-traffic guard, the INSERT body is the query, the column order in
the query and the DDL, the DDL types, `detected_at`, earliest offsets and the 10-second
window; D-058's guard now keeps `error_count` out of 2.3-2.5), `opsgen/tests/test_engine.py`
(the quiet-period test reads `2.4_anomaly_status.sql`).
Issue 06 (the agent): `sql/4.3_insert_release_agent_errors.sql` (new, fan-out leg 1: the
release agent on `service_error_rate` rows with `error_status = 'HIGH'`, trigger
`ERROR_RATE_HIGH`, its input the error rate against the 25 % threshold, errors of requests,
last change, seconds since deploy and previous version; the same answer parsing as the
latency leg, which becomes leg 2, and capacity leg 3). `sql/4.2_release_agent.sql`: the role
paragraph names both triggers, rule 1 covers a burst of failing requests, and the examples are
a checkout latency ROLLBACK, a web-gateway error-rate ROLLBACK and a payments latency OBSERVE
(both triggers get a ROLLBACK example, so the model learns the rule and not the trigger).
`sql/4.2_capacity_agent.sql`: SCALE_OUT on checkout (15 replicas, the scorer's formula), and
all three examples' numbers follow D-059's 30 requests a second (they were from the old
rates). `sql/checks/remediation_call_rate.sql` counts `service_error_rate` too;
`remediation_routing.sql` and the checks README; `uv run reset-health`'s next steps start the
error-rate agent INSERT first; `evaluate`'s hint no longer says "two" agent INSERTs. Tests:
`tests/test_agent_sql.py` (keyed by INSERT file, three of them; an example for every allowed
action and no other, instead of exactly one per action; both release legs write and tell their
own trigger and parse the answer identically; the error-rate leg's input lines; no example
pairs web-gateway with latency or traffic, SCALE_OUT on checkout, INVESTIGATE_UPSTREAM on
payments), `tests/test_reset_health.py`, `tests/test_evaluate_decisions.py`.
Issue 07 (the wall): `scripts/opswall/state.py` (`ErrorRateRow` and `apply_error_rate`, kept
like a health row; `Trigger.source` names the rows a trigger reads, and `ERROR_RATE_HIGH`
reads `service_error_rate` onto the `errors` metric; `INCIDENT_METRIC` maps
`BAD_DEPLOY_ERRORS` there; `MetricSample.error_count` is back and a card with the errors
chart gets each second's `errorPct`, null for a second without requests; `DeployMark`, one per
DEPLOY or ROLLBACK keyed by `deployed_at`, for a card with the errors chart; `charts` per card,
`ERRORS_CHART_SERVICES = ("web-gateway",)`; `errorThresholdPct` in the snapshot, tied by a test
to lab 2.2's `0.25`), `consumer.py` (a sixth topic, `service_error_rate`, from 10 minutes
back), `app.py` (docstring), `static/opswall.js` (cards build their charts from
`svc.charts`; the errors chart is linear 0-100 % with a dashed `--crit` threshold line and a
gap for a second without requests; a deploy-mark layer, solid for a deploy and dashed for a
rollback, each a keyboard stop, and the crosshair's tooltip names a deploy within 1.5 s;
labels for the new type, trigger and metric), `opswall.css` (`--text` deploy marks and the
`--crit` threshold: OKLab ΔE 20.1 and 18.3 from their nearest tokens, above the 15 floor),
`index.html` (three legend rows). Tests: `tests/test_opswall_state.py` (charts per card; an
error-rate breach is a block on the errors chart owned by its `BAD_DEPLOY_ERRORS`; the tile
waits on "error rate high"; a latency decision doesn't close it; the two row sources don't
overwrite each other; a window written twice; the error share per second; deploy and rollback
marks, not heartbeats, and only where the chart shows them; trim; the threshold matches the
SQL), `tests/test_opswall_ingest.py` (the consumer tails six topics). Rendered offline in
headless Chrome at 1440 and 820 px: the real `Simulation` (seed 7), lab 2.2's verdict per
window and a stand-in release agent answering ROLLBACK 1 s after each HIGH window; three bad
deploys, all answered (detection 10.4 s, answer 0.6 s), five good deploys
(`.scratch/web-gateway-threshold/research/wall_render.py`). Hover and keyboard focus were not
exercised (screenshots only).
Issue 08 (evaluate): `scripts/evaluation/detection.py` (`ERROR_RATE_HIGH`; `HealthRow` reads a
`service_error_rate` record too, its `error_status`, `error_count` and `error_rate`, and a HIGH
status is a signal; `merge_rows` makes a window found in both tables one row with both
verdicts), `scripts/evaluation/decisions.py` (`ERROR_RATE_HIGH` routes to the release agent; a
HIGH error-rate row expects one release-agent call; the context names the error rate),
`scripts/evaluate.py` (reads `service_error_rate` next to `service_health`, with
`--error-rate-topic`, `--error-rate-rows` and `--dump-error-rate`; either table may be missing,
with a hint naming its INSERT). Tests: `tests/test_evaluate_detection.py`,
`tests/test_evaluate_decisions.py`. Offline on dumps from the real `Simulation` (seed 7, 2 h:
the ARIMA replica's `service_health` for checkout and payments, lab 2.2's rule for web-gateway,
a stand-in release agent; `.scratch/web-gateway-threshold/research/evaluate_dumps.py`), 100
scored minutes: `BAD_DEPLOY_ERRORS` 50/50 in window 1; `BAD_DEPLOY` 26/26, `TRAFFIC_SURGE`
17/17 and `UPSTREAM_DROP` 17/17; no false positives or fresh-start noise on any service; 50
release-agent decisions, all valid, no routing errors, no wrong targets.

Amends D-056, D-057, D-058, D-060 and D-066.

**Amended by D-068.** Surges are retired rather than left on checkout, which becomes a ticket
pipeline. The shared scheduler serves payments alone (about 19 incidents an hour) and labs
2.3–2.5 read payments only. The capacity agent has no `SCALE_OUT` and no checkout example: its
examples are an `INVESTIGATE_UPSTREAM` and an `OBSERVE` on payments. The release agent's
latency ROLLBACK example is on payments too.

### D-068 · DECISION · checkout becomes a ticket pipeline; payments alone keeps ARIMA
**What.** checkout and payments were detected the same way: the simulator produced per-second
metrics, labs 2.3–2.5 windowed them, and `ML_DETECT_ANOMALIES` (ARIMA) flagged latency and
traffic spikes and lows for the release and capacity agents. Two services told one story. The
workshop should also show a second architecture: events that are not metrics, routed by
Flink, and handled by a chain of agents.
**Options.**
- checkout: (a) keep it a second ARIMA service; (b) turn it into a support-ticket pipeline.
- Traffic surges, whose only candidate was checkout (D-067): (a) move them to payments;
  (b) retire them, and the capacity agent's `SCALE_OUT` with them.
- The tickets: (a) more records in an existing stream (`service_metrics` or
  `service_incidents`); (b) a stream of their own.
- What the Flink filter routes on: (a) the real severity; (b) the priority the reporter set,
  with an agent recovering the real severity.
- The tickets' answer key: (a) columns of the ticket; (b) a topic of its own.
- Where the second agent writes: (a) a new table; (b) `remediation_decisions`, with a
  `ticket_id`.
- The wall: (a) add checkout's row to today's cards; (b) take the frame of the target design
  (`inspiration/Ops wall redesign.html`) now.
**Choice.** (b) each time, decided with the user in a grilling session on 2026-11-23/24 (four
rounds; `.scratch/checkout-tickets/spec.md`, which numbers the 39 decisions summed up here).
The story the wall tells for checkout: tickets keep arriving; a P1 lights up and the
classifier picks it up; if it is real, the responder recommends what to do; if it is a
customer shouting about a discount code, it stops there. Terms: a **ticket** is one support
request about checkout, from Zendesk or ServiceNow, never called an incident
(`service_incidents` keeps that meaning); its **priority** (`P1`–`P3`) is the reporter's, its
**severity** (`HIGH` | `LOW`) the real impact; the **true severity** is in the answer key and
the **verdict** is the classifier's (`INVALID` when its answer can't be parsed).
- **payments alone on ARIMA.** Labs 2.3–2.5 read `WHERE m.service_name = 'payments'` and keep
  `PARTITION BY service_name`, so the lab still shows the general pattern. Detector settings,
  columns and `service_health` don't change. `TRAFFIC_SURGE` leaves the simulator
  (`INCIDENT_TYPES`, `INCIDENT_WEIGHTS`, `CANDIDATES`, `TRIGGER`, `EXPECTED_ACTION`, `RESTS`
  and its ×5 effect), `SHARED_SERVICES = ("payments",)`, and the "first N are one of each"
  rule covers two types. The weights stay 0.50/0.25 (two bad deploys for every drop); the
  effects, the rests (390 s and 300 s), the 20 s gap, the 150 s quiet period and the 35 s cap
  don't change. Every seeded schedule shifts, because the scheduler draws differently;
  web-gateway's deploy clock doesn't (its own rng).
- **The metric agents.** The capacity agent answers `INVESTIGATE_UPSTREAM` (rule 1, the
  trigger is `TRAFFIC_LOW`) or `OBSERVE` (rule 2, `TRAFFIC_HIGH`, which now only false alarms
  such as the echo after a drop raise). Its examples are one of each on payments, its INSERT
  loses Suggested Replicas and Ready Replicas, and it keeps the three-line answer with Target
  always N/A. The release agent's checkout latency ROLLBACK becomes a payments one (2.9.x);
  nothing else in its prompt or rules changes.
- **checkout produces only tickets**: no `service_metrics`, no `service_deployments`, never an
  incident candidate. `opsgen/services.py` says which services produce metrics (web-gateway,
  payments) and which tickets (checkout).
- **The ticket clock** has its own rng, `Random(seed + 3)` (the scheduler is `seed`, traffic
  `seed + 1`, web-gateway's deploys `seed + 2`): Poisson(1/15) tickets per 1-second tick,
  about 4 a minute, each independent, from the start. `--no-incidents` stops it. Priority P1
  30 %, P2 30 %, P3 40 %; a P1 is really HIGH with probability ½, P2 and P3 are always LOW; a
  really-HIGH P1 expects `ROLLBACK`, `SCALE_OUT` or `INVESTIGATE_UPSTREAM` with equal odds;
  the source is `ZENDESK` or `SERVICENOW` with equal odds. About 1.2 P1s a minute: about 72
  classifier and 36 responder calls an hour per attendee. Ids look native (`ZD-48215`,
  `INC0041872`), with one counter per source in the generator state file (D-046), so a
  restart never reuses one. About 40 English templates in a checked-in module: 12 P1s that
  are really HIGH (4 per action, each with one clear cue), 12 P1s that are really LOW (4 of
  them shouting) and 16 P2–P3; the user reviews them.
- **Tables.** Sources `support_tickets` (`event_time`, `ticket_id`, `source`, `service_name`,
  `priority`, `subject`, `description`) and `ticket_answer_key` (`event_time`, `ticket_id`,
  `true_severity`, `expected_action`), with `service_incidents`' options; outputs
  `p1_tickets` (the same seven columns) and `ticket_triage` (the seven, then `severity`,
  `reason`, `raw_response`), read-uncommitted. `remediation_decisions` gains a nullable
  `ticket_id` as its last column, so the table is dropped and recreated. Terraform creates
  all four new tables, sources first, then the outputs in lab order: 17 resources per
  person. `flink_max_cfu` stays 10.
- **Section 5, "A second architecture: support tickets".** 5.1 routes the P1s into
  `p1_tickets`. 5.2 is the `ticket_classifier` agent and its INSERT into `ticket_triage`
  (latest offset; `Severity:` and `Reason:`). 5.3 is the `ticket_responder` agent and its
  INSERT into `remediation_decisions` for HIGH verdicts (latest offset; its input carries the
  classifier's reason; `Action:` and `Reason:`, no Target; agent `ticket_responder`, trigger
  `HIGH_SEVERITY_TICKET`, null target). Both prompts have the existing shape and no
  apostrophes, and use `llm_textgen_model`.
- **Housekeeping.** The topic and table lists, `LAB_AGENTS`, `uv run reset` and `uv run
  reset-health` cover the new tables and agents. ops-sim's `DecisionFeed` decodes a ticket
  decision and resolves nothing with it. `sql/checks` name the ticket legs. `sweep.py` is
  fixed; the research scripts keep a note that they predate this decision.
- **The wall** takes the frame of the target design: its header; one full-width row per
  service with a main panel and an "Issues · Last 10 min" panel (Injected; Detected "x of y"
  with the missed line; median time to detect; median time to remediate); the dotted
  background; Geist and Geist Mono, vendored with their OFL licence (Plex goes); the design's
  palette. The summary table and the legend go. web-gateway and payments keep their charts
  inside their rows. checkout's row has its two sources (a dot runs to the list for each new
  ticket), the newest tickets of each priority, and a 2-minute timeline with a Classifier and
  a Responder lane, its bars drawn between Kafka timestamps.
- **`evaluate`** gets a ticket section: true severity × verdict, answered and expected
  actions, filter leaks, median times. Ticket decisions stay out of the metric coverage.
  Acceptance: the classifier right on ≥ 95 % of P1s, the responder on ≥ 95 % of the HIGH
  tickets it answers, no leaks.
**Why.**
- **Surges are retired, not moved.** payments already has the drops, and a traffic series
  with both loses the drops: ×5 surges widen its band, which is symmetric, until the lower
  bound is below zero, and a ×0.1 drop never falls under it. Offline, with both types on both
  services, 1.6 % of drops were caught (D-067). There is no other ARIMA service to move them
  to. Without surges, `SCALE_OUT` answers no metric incident, so the capacity agent loses it;
  `TRAFFIC_HIGH` is then always a false alarm, and `OBSERVE` answers it.
- **Removed, not left without a candidate.** With the type still in `INCIDENT_TYPES` and no
  service to take it, the "first N are one of each" rule would wait for it forever and stop
  payments' incidents after the first two.
- **Tickets are a separate stream.** A ticket is an event with text, not a metric, and it is
  not an incident the simulator injected: in `service_metrics` or `service_incidents` it would
  blur both. Its own table makes routing on a field the lesson of 5.1, and its own rng keeps
  every other stream's schedule where it was for a seed.
- **Priority and severity differ** because reporters over-prioritise. The filter routes on
  what the reporter set, cheaply and deterministically; the classifier recovers the real
  impact, which is why an agent comes after the filter. P2 and P3 are always LOW, so the
  filter never drops a real outage.
- **The answer key is its own topic** so that nothing an agent reads carries the answer:
  `p1_tickets` and `ticket_triage` copy the ticket's columns, and they are the agents' input.
  Only the wall and `evaluate` read it, as with `service_incidents` for metric incidents
  (D-046).
- **The responder writes to `remediation_decisions`**, so there is one decisions table for
  ops-sim, the wall and `evaluate`.
- **The wall takes the design's frame now** because checkout's row is the design's: its
  sources, ticket list and agent lanes don't fit today's cards of charts, and building the
  row in today's frame would mean building it twice.
**Measured.** ARIMA on payments alone, offline (`.scratch/checkout-tickets/research/
payments_only.py`, output in `payments_only.out` next to it): the real generator through the
AR(1) replica at the labs' settings (12/360/99.95), scored with `evaluate`'s code
(`close_loop.score_run`), 300 seeds × 3 h per run, web-gateway's deploy clock on, payments'
rows and incidents scored:

| Run | Inc/h (steady) | Mix | Bad deploys | Drops | Window 1 | Echoes, other FPs | Fresh /svc |
|---|---|---|---|---|---|---|---|
| D-067: surges on checkout, drops on payments, both scored | 37.3 | BD/SURGE/DROP 44/28/28 | 100 % | 100 % | 100 % | 0, 0 | 0.04 |
| **D-068: payments alone** | 18.7 (19.0) | BD/DROP 45/55 | 100 % (7406/7406) | 100 % (9094/9094) | 100 % | 0, 0 | 0.05 |

Steady is hours 2 and 3 of each run. The mix by count is 45/55 although the weights are 2:1,
because the bad-deploy rest is the longer one. The fresh-start noise is D-065's row C (0.05),
and an incident costs 1.02 agent calls. Every incident fixed at T+11 (an answer about 1 s
after detection) gives the same recall and no echoes; 42 opposite traffic flags then land in
the window that holds the return to normal, which `evaluate` counts as neither.
**Open.**
- The web-gateway and payments rows of the target design (the releases list, the payments
  cluster, aggregated records) come with the full redesign. The design's payments row shows
  5 s windows, p95 latency and sub-services, none of which exist in our data, so it needs its
  own grilling. Until then checkout's timeline covers 2 minutes and the other rows' charts 10.
- From D-067: the windowing-only step (`2.3_window_metrics.sql`) may be redundant. Kept.
- `opswall-redesign` issue 01 (what the wall reads from `service_deployments`) is still open.
  The new frame shows no versions; web-gateway's deploy marks still need the consumer.
**Pending (live check, `.scratch/checkout-tickets/issues/12-live-check.md`).**
- The pool's CFU use with eight long-running statements (2.2, 2.5, the three 4.3 legs, 5.1,
  5.2 and 5.3), against the ceiling of 10.
- The time each agent takes, on both hops.
- Whether `ticket_triage` records get their Kafka timestamp when written, as decisions do
  (D-066). Otherwise the Classifier bars have no length; the fallback is a `classified_at`
  column set by `CURRENT_TIMESTAMP`, like `detected_at`.
- Both agents against the answer key on Confluent Cloud (≥ 95 %).
- ARIMA on payments alone, live, against the offline re-check above. The capacity agent
  answers `OBSERVE` to every `TRAFFIC_HIGH`.
- The checks of `.scratch/web-gateway-threshold/issues/10-recheck-after-next-changes.md` that
  still apply.
**Impact.** Issue 01 (this entry; surges retired, ARIMA on payments only, the metric agents,
tuning, the re-check): `opsgen/engine.py` (`TRAFFIC_SURGE`, `SURGE_LAMBDA_MULT` and the surge
effect gone; `SHARED_SERVICES = ("payments",)`; `CANDIDATES`; `RESTS`; the "first two are one
of each" rule; the module docstring and the rate), docstrings in
`opsgen/{services,cli,deployments}.py` (the banner in `driver.py` lists `INCIDENT_TYPES`, so it
now reads `rests: BAD_DEPLOY=390s UPSTREAM_DROP=300s`). The labs:
`sql/2.3_window_metrics.sql`, `2.3_window_aggregation.sql`, `2.4_anomaly_detection.sql`,
`2.4_anomaly_status.sql` and `2.5_service_health.sql` read payments alone, with their headers;
`sql/sink_service_health.sql` names no service, so it and `statements.tf` are unchanged;
`sql/checks/remediation_call_rate.sql`'s comment (60 `service_health` rows per 10 quiet
minutes). The agents: `sql/4.2_capacity_agent.sql` (two actions, rules 1 and 2, an
`INVESTIGATE_UPSTREAM` example kept from before and an `OBSERVE` one, both on payments; the
role paragraph gives payments' numbers, about 300 requests per window on 2 replicas, and says
no surge is expected), `sql/4.3_insert_capacity_agent.sql` (no replica arithmetic; the action
parsed in `INVESTIGATE_UPSTREAM` or `OBSERVE`), `sql/4.2_release_agent.sql` (the latency
ROLLBACK is payments at 893 ms against 226 ms, back to 2.9.4). `evaluate`:
`scripts/evaluation/decisions.py` (the capacity agent's actions; `suggested_replicas` and the
`SCALE_OUT` target check gone, so only a `ROLLBACK` carries a target; `SCALE_OUT` stays in
`ACTIONS` for the responder), `detection.py` (docstring: no incident expects `TRAFFIC_HIGH`),
`scripts/evaluate.py` (the target legend). The wall: `scripts/opswall/state.py`
(`INCIDENT_METRIC` without `TRAFFIC_SURGE`; `FIX_ACTIONS` keeps `SCALE_OUT` for the responder),
`static/opswall.js` (no surge label). Tuning: `scripts/tuning/sweep.py` (no surge multiplier,
no surge or per-service drop column, rate presets without checkout); a header note on each
`scripts/tuning/research/*.py` and in its README. Tests: `opsgen/tests/test_engine.py`
(payments alone gets bad deploys and drops; surges retired from every table; the first two
are one of each; the rests; about 19 an hour; the chooser, gap, rest and resolve tests on
payments; overlap now against web-gateway's clock), `test_simulation.py`,
`test_output.py` (no surge anywhere; payments' bad deploys and drops; 18 in the first hour;
the banner), `test_incident_records.py`, `test_deploy_clock.py`,
`tests/test_sql_pipeline.py` (`ARIMA_FILTER` is payments; no ARIMA lab names checkout),
`tests/test_agent_sql.py` (exactly an `INVESTIGATE_UPSTREAM` and an `OBSERVE` example on
payments, no `SCALE_OUT`; no replica arithmetic in the INSERT; no checkout example; the
payments ROLLBACK), `tests/test_evaluate_decisions.py` (the capacity agent's actions; only a
`ROLLBACK` carries a target), `tests/test_opswall_state.py` (the incident types the wall
knows), `tests/test_tuning.py` (the sweep scores payments alone; no surge or checkout column;
no checkout rate). `uv run ops-sim --dry-run --simulate 30 --seed 42`: no surge; payments has 4
bad deploys and 5 drops, checkout none; web-gateway's 49 deploy and incident lines are the
same as before the change.

Issue 02 (checkout writes no metrics and no deploys): `opsgen/services.py` (two tuples:
`METRIC_SERVICES`, web-gateway and payments at 50/30 requests a second, and `TICKET_SERVICES`,
checkout by name alone, with no latency, rate, replicas or version; `SERVICES` lists the three
names in the wall's order; `METRIC_SERVICES_BY_NAME` and `metric_service_names()` replace
`SERVICES_BY_NAME` and `service_names()`; the module docstring), `opsgen/simulation.py`
(metrics, the bootstrap, the 5 s re-emit, deploys and rollbacks for the metric services only,
so two metric records a second; the state file saves those two, and a pre-D-068 file loads
with checkout's entry ignored), `opsgen/driver.py` (the `--simulate` summary lists the two;
the banner names no service, so it is unchanged), `opsgen/cli.py` (`--show-records` takes
web-gateway or payments, with a payments example), docstrings in
`opsgen/{__init__,deployments,state}.py`. The wall: `scripts/opswall/state.py` (seeds its
cards from the names; a ticket service gets `charts: []`, and checkout's card keeps its header
and status and stays healthy), `static/opswall.js` (a card with no charts hides its time axis
too; checked in headless Chrome at 1440 and 820 px), `scripts/opswall/feed.py` (two samples a
second). `scripts/tuning/sweep.py` patches `METRIC_SERVICES`. Stale comments:
`scripts/common/topic_reader.py` (`service_health` is one row per 10 s), the headers of
`sql/source_service_metrics.sql` (two rows a second) and `source_service_deployments.sql`,
which `statements.tf` doesn't copy; and the column comments of `sql/source_service_incidents.sql`
and its byte-identical copy in `platform-team/modules/developer/statements.tf`, which named
`TRAFFIC_SURGE`, `TRAFFIC_HIGH` and `SCALE_OUT` and missed D-067's types: `incident_type` is
`BAD_DEPLOY` or `UPSTREAM_DROP` on payments and `BAD_DEPLOY_ERRORS` on web-gateway,
`expected_trigger` `LATENCY_HIGH` or `TRAFFIC_LOW` in `service_health` and `ERROR_RATE_HIGH` in
`service_error_rate`, `expected_action` `ROLLBACK` or `INVESTIGATE_UPSTREAM`. IF NOT EXISTS
leaves an existing table as it is, so the new comments reach a live table only when it is
recreated. Lab 2.1's four files name no service, so they are unchanged. Tests:
`opsgen/tests/test_simulation.py` (the split; metrics and deployment records for web-gateway
and payments only, two a tick, over an hour of deploys and rollbacks), `test_engine.py` (the
state file round trip and a pre-D-068 file with checkout; two heartbeats every 5 s),
`test_output.py` (the summary; the `--show-records` choices and help), `test_deployments.py`
(its fixtures on payments), `test_deploy_clock.py`, `test_decisions.py`, `tests/test_tuning.py`
(two services, about 80 requests a second, 2 × 13 warm-up rows), `tests/test_opswall_state.py`
(checkout has no charts and stays healthy; the cards in the wall's order; the tests that used
checkout as a latency service alone now use payments), `tests/test_opswall_ingest.py` (on
payments). `uv run ops-sim --dry-run --simulate 15 --seed 1`: no checkout line; the incident
and deploy lines are the same as issue 01's run for that seed, and only the traffic values
differ.

Issue 03 (the ticket generator: templates, the ticket clock, the answer key, the two source
tables): `opsgen/ticket_templates.py` (new; 40 templates as data, each with a priority, a true
severity, an expected action for HIGH and a `shouting` flag: 12 really-HIGH P1s, 4 per action,
12 really-LOW P1s, 4 of them shouting, and 8 each of P2 and P3; the placeholder pools and
`fill`), `opsgen/tickets.py` (new; `TicketClock`: Poisson(1/15) a tick, the mix drawn in a
fixed order, one id counter per source from `ZD-48215` and `INC0041872`, a `support_tickets`
record and a `ticket_answer_key` record per ticket, both stamped with the tick; off with
`--no-incidents`), `opsgen/simulation.py` (the clock on `Random(seed + 3)`; `tick` returns a
fourth list, the tickets; `state_snapshot` saves `next_ticket_numbers`, and a state file
without it starts at the defaults; the class docstring lists the four generators),
`opsgen/driver.py` (tickets go to the sink; a tick with a ticket saves the state, so a
generator killed between two 5 s saves never reuses an id; `--show-records checkout` prints
each ticket and its answer-key record as produced; `--simulate` prints a line per ticket and
ends with the tickets by priority × true severity and the HIGH ones by expected action; the
banner names the ticket clock, and reads `no incidents, no tickets` with `--no-incidents`),
`opsgen/sinks.py` (`DryRunSink.emit_tickets`: the `support_tickets` and `ticket_answer_key`
stream tags and the console line `TICKET  P1  ZD-48215  Zendesk  "…"  [truth: HIGH,
SCALE_OUT]`; `format_record` takes the answer key's `ticket_id` key), `opsgen/kafka_io.py`
(`TICKETS_TOPIC` and `ANSWER_KEY_TOPIC`, their serializer contexts, `emit_ticket`,
`emit_answer_key` and `emit_tickets`; raw-string keys, `service_name` and `ticket_id`; schemas
looked up, never registered), `opsgen/cli.py` (`--show-records` takes all three services; the
live start line names the two topics; the docstring), docstrings in
`opsgen/{__init__,state}.py`. The tables: `sql/source_support_tickets.sql` and
`sql/source_ticket_answer_key.sql` (new; the columns of spec decisions 13 and 14 and
`service_incidents`' options; the answer key's header says it is for the wall and `evaluate`
only), `schemas/support_tickets.avsc` and `schemas/ticket_answer_key.avsc` (new),
`platform-team/modules/developer/statements.tf` (two statements byte-identical to the SQL,
chained after `service_incidents` and before `service_error_rate`; the header's count),
`outputs.tf` (both in `flink_statements`), `main.tf` (the header's count). The lists:
`SOURCE_TOPICS` in `scripts/common/topics.py`, so `uv run reset` empties both topics, and
`SOURCE_TABLES` in `scripts/preprovisioned/provision.py` (eight tables), with the counts in
their docstrings and `scripts/reset.py`'s. `scripts/tuning/sweep.py` takes the fourth list.
Tests: `opsgen/tests/test_tickets.py` (new: every placeholder has a pool; 12/12/8/8 and 4 per
action; at least 4 shouting LOW P1s; each template's labels, by keyword: a HIGH one has a
start time, many customers and exactly its action's cue, and a LOW one never says many
customers; one to three sentences; the rate and mix over 20 hours; an answer key on the same
tick, null action exactly when LOW; the ids, through a restart and from a state file without
counters; a save on every tick with a ticket; tickets on, off or 30 times as many leave every
other record the same; same seed, same tickets; no quiet period; none with `--no-incidents`),
`test_output.py` (the dry-run line, `--show-records checkout`, the summary, the banner),
`test_simulation.py`, `test_engine.py`, `test_deploy_clock.py`, `test_determinism.py` and
`test_incident_records.py` (the fourth list), `tests/test_kafka_io_and_rtce.py` (the Kafka
sink against fake clients: topics, raw keys, timestamps, no registration),
`tests/test_schema_vs_ddl.py` (both pairs; the spec's columns in order; `service_incidents`'
options), `tests/test_platform_team_sync.py` (eight tables), `tests/test_reset.py`,
`tests/test_preprovisioned_readiness.py`, `tests/test_tuning.py`, and
`platform-team/tests/developer_module.tftest.hcl` (both keys; a plan run on both tables'
options). `uv run ops-sim --dry-run --simulate 15 --seed 1`: 63 tickets, the first at 00:54
(P1 7 HIGH and 8 LOW, P2 24, P3 24; ROLLBACK 2, SCALE_OUT 2, INVESTIGATE_UPSTREAM 3). Leaving
out the ticket lines, the banner (which now names the ticket clock) and the closing summary,
the output is the same as issue 02's for that seed line for line, metric values included.

Issue 04 (the ticket output tables, `remediation_decisions.ticket_id`, the lists, reset and
readiness): `sql/sink_p1_tickets.sql` (new; `support_tickets`' seven columns and options, the
5 s watermark included; the header says lab 5.1 writes it, lab 5.2 reads it, and that
read-uncommitted spares the classifier a wait of about a minute for each Flink commit, D-063),
`sql/sink_ticket_triage.sql` (new; the seven ticket columns, then `severity` STRING NOT NULL,
`HIGH`, `LOW` or `INVALID`, `reason` and `raw_response`, with the same options; lab 5.2 writes
it and lab 5.3 reads its HIGH rows), `sql/sink_remediation_decisions.sql` (`ticket_id` STRING,
nullable, as the last column; the comments of `window_end`, or the ticket's `event_time` for a
ticket decision, `agent_name` (`release_agent`, `capacity_agent` or `ticket_responder`),
`trigger` (`HIGH_SEVERITY_TICKET` added), `action` (`SCALE_OUT` from the responder only) and
`target` (a `ROLLBACK`'s version from the release agent, otherwise null); the header: three
agents in four INSERTs, three in 4.3 and one in 5.3, and what a ticket decision holds), the
three `sql/4.3_insert_*.sql` (`CAST(NULL AS STRING)` as the last value, and a header line
saying so). Terraform: `platform-team/modules/developer/statements.tf` (both statements,
byte-identical to the SQL, chained after `remediation_decisions`, `p1_tickets` then
`ticket_triage`, with the model's `depends_on` moved to `ticket_triage`; the
`remediation_decisions` copy; the header's counts and order), its `outputs.tf` (both keys) and
`main.tf` (ten tables), `platform-team/main.tf` (ten tables), `variables.tf` (`flink_max_cfu`
stays 10, and its description names the eight long-running statements: 2.2, 2.5, the three
4.3 legs, 5.1, 5.2 and 5.3), `outputs.tf` (the validation heading said "the two source
tables"), `README.md` (the pool's eight statements, the ten tables, 17 resources per person
and their list, eleven statements `COMPLETED`, 17 to add per new person, the readiness row and
the files table; a new "Updating stacks you built earlier": stop the 4.3 INSERTs, `DROP TABLE
remediation_decisions`, apply again). The lists: `OUTPUT_TOPICS` in `scripts/common/topics.py`
gains `p1_tickets` and `ticket_triage`, so `uv run reset` empties both and `uv run
reset-health` deletes whatever writes them (lab 5.1 and 5.2's INSERTs; the responder's goes
with the other `remediation_decisions` writers) and empties both; `OUTPUT_TABLES` in
`scripts/preprovisioned/provision.py` (ten tables); `scripts/reset.py` (`LAB_AGENTS` gains
`ticket_classifier` and `ticket_responder`; the docstring and next steps name 5.1–5.3),
`scripts/reset_health.py` (the docstring, the help, and a third next step: 5.1, then 5.2's
INSERT once 5.1 has caught up, since it reads from the latest offset, then 5.3's). The checks:
`sql/checks/remediation_routing.sql` (one query, two kinds of row: a window that reached two
metric agents, and a decision that breaks the ticket leg's rules, so `HIGH_SEVERITY_TICKET`
only from `ticket_responder`, with a `ticket_id` and no target, and no `ticket_id` from a
metric agent), `remediation_call_rate.sql` (metric decisions apart from ticket decisions;
`p1_tickets` rows, `ticket_triage` rows and their HIGH verdicts), and their README. ops-sim:
`opsgen/decisions.py` (docstrings only: a ticket decision has every field the engine needs,
so it decodes and matches nothing). Tests: `tests/test_agent_sql.py` (nine columns; each 4.3
INSERT's arity and its null `ticket_id`; the new column and the comments),
`tests/test_schema_vs_ddl.py` (the two tables' columns in order, `support_tickets`' options and
watermark, `severity` NOT NULL with its three values), `tests/test_platform_team_sync.py` (ten
tables; the `depends_on` chain: the five sources, the five outputs in lab order, the model
last), `tests/test_reset.py` (ten topics, four agents, the next steps),
`tests/test_reset_health.py` (five topics; the matcher and the three section 5 INSERTs; the
next steps in lab order), `tests/test_preprovisioned_readiness.py` (ten tables; a missing
`ticket_triage` names the platform team), `opsgen/tests/test_decisions.py` (the registered
schema gains `ticket_id`; a responder row, through the feed, decodes to a checkout
`HIGH_SEVERITY_TICKET` decision and leaves a payments drop expecting the same action
unresolved; a capacity decision with a null `ticket_id` resolves it as before),
`platform-team/tests/developer_module.tftest.hcl` (both keys and 11 statements; a plan run on
both tables' options and on `remediation_decisions`' `ticket_id`). `IF NOT EXISTS` never
changes a table, so `remediation_decisions` is dropped before the apply that recreates it; the
user does both.

Issue 08 (the design's frame and the Issues panels): `scripts/opswall/state.py` (each service
in the snapshot gets `issues`, over the incidents that *started* in the last 10 minutes, so an
incident cut by the chart's left edge is drawn but not counted: `injected`; `detected`, an
owned block and not missed, so a missed incident whose run is still drawn counts as missed
only; `missed`; `medianTimeToDetect`; `medianTimeToRemediate`, start → the first answer of
its blocks, both medians over answered incidents; a ticket service's come from
`_ticket_issues`, zeros until issue 09; the global `summary` and `_summary` gone, nothing else
read them; the docstring), `static/index.html` (the design's header: "Ops wall", "Releases,
incidents and live signals", "Updated hh:mm:ss" and the lamp, Live, Connecting… or
Reconnecting…; one `#rows` container; the summary table, the legend and the Confluent logo
gone), `static/opswall.css` (rewritten: the design's palette, the dotted page, the panels and
their "+" corner marks, a row of a main panel and a 200-px Issues panel that moves below the
row under 1000 px with its four numbers side by side; the chart tokens keep their names on the
nearest new colour, `--metric` `#8B919B`, `--cyan` `#8FB8FF`, `--amber` `#F5B544`, and the
incident boxes take `--crit`, because D-066's purple is nearest the blue detection already
holds; every chart mark is OKLab ΔE ≥ 15 from the others, 15.7 between the line and detection
at the closest; an anomaly turns the main panel's border red), `static/opswall.js` (a row per
service with its Issues panel, durations "6s" and "1m 05s" as the design writes them, "—"
with none; checkout's row hides its chart body; the clock on the 24-hour clock; the summary
code gone; the charts, tooltips, keyboard stops and in-place updates unchanged),
`static/fonts/` (`geist.woff2` and `geist-mono.woff2`, variable 400–700, copied from the
design file's bundle; `LICENSE.md` carries its OFL comment; the three Plex files gone),
`static/confluent-logo.svg` gone (the favicon stays). Tests: `tests/test_opswall_state.py`
(each service's `issues`; remediation is start → answer; a missed incident is missed and not
detected; an open one is injected only; only incidents started in the last 10 minutes; no
global summary; checkout's panel reads zeros; a false alarm counts nowhere; the summary tests
gone), `tests/test_opswall_server.py` (the snapshot's shape). Offline render in headless
Chrome at 1440 and 820 px (`.scratch/checkout-tickets/research/issue08_wall_render.py`): the
frame lands on the design's coordinates, with web-gateway 4 of 4, payments 3 of 4 with "1
missed" (its stand-in agents sat out one incident on purpose) and checkout at zeros; a box's
hover tooltip and a mark's keyboard focus work, and the focused node and the boxes survive the
next snapshots.

Amends D-032, D-034, D-040, D-043, D-046, D-050, D-056, D-058, D-059, D-060, D-061, D-065, D-066
and D-067.
