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
**Resolved by D-069.** The replacement is the platform team's `deploy_labs`, for a demo
environment only; attendees still start the labs themselves.

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
payments_only.py`, outputs in `payments_only.out` and, re-run on the fully merged tree,
`payments_only.after-all-issues.out` next to it): the real generator through the
AR(1) replica at the labs' settings (12/360/99.95), scored with `evaluate`'s code
(`close_loop.score_run`), 300 seeds × 3 h per run, web-gateway's deploy clock on, payments'
rows and incidents scored:

| Run | Inc/h (steady) | Mix | Bad deploys | Drops | Window 1 | Echoes, other FPs | Fresh /svc |
|---|---|---|---|---|---|---|---|
| D-067: surges on checkout, drops on payments, both scored | 37.3 | BD/SURGE/DROP 44/28/28 | 100 % | 100 % | 100 % | 0, 0 | 0.04 |
| **D-068: payments alone** | 18.7 (19.0) | BD/DROP 45/55 | 100 % (7406/7406) | 100 % (9094/9094) | 100 % | 0, 0 | 0.06 |

Steady is hours 2 and 3 of each run. The mix by count is 45/55 although the weights are 2:1,
because the bad-deploy rest is the longer one. An incident costs 1.02 agent calls. Every
incident fixed at T+11 (an answer about 1 s after detection) gives the same recall and no
echoes; 47 opposite traffic flags then land in the window that holds the return to normal,
which `evaluate` counts as neither, and the fresh-start noise is 0.08 per service. The row is
the re-run on the fully merged tree (issue 01's last comment). The first run, before issue 02,
gave 0.05 per service with or without the fix (D-065's row C) and 42 opposite flags;
everything else is the same. They moved because issue 02 took checkout out of the metric
services, so for a given seed payments' traffic draws change (from the same distribution).
**Open.**
- From D-067: the windowing-only step (`2.3_window_metrics.sql`) may be redundant. Kept.
**Pending (live check, `.scratch/checkout-tickets/issues/12-live-check.md`).**
- The pool's CFU use with eight long-running statements (2.2, 2.5, the three 4.3 legs, 5.1,
  5.2 and 5.3), against the ceiling of 10.
- The time each agent takes, on both hops.
- Whether `ticket_triage` records get their Kafka timestamp when written, as decisions do
  (D-066). Otherwise the Classifier bars have no length; the fallback is a `classified_at`
  column set by `CURRENT_TIMESTAMP`, like `detected_at`.
- Whether ticket decisions get their Kafka timestamp when written too (found in the code
  review). Lab 5.3 reads `ticket_triage`, which has a watermark on `event_time`. If Flink
  stamps each decision with its input's rowtime, a ticket decision carries the ticket's
  `event_time` as its Kafka timestamp. Then the Responder bars have no length, and neither
  have the wall's time to remediate and `evaluate`'s verdict → action median. The
  `classified_at` fallback doesn't cover this hop.
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

Issue 05 (lab 5.1, route the P1 tickets): `sql/5.1_p1_tickets.sql` (new; one INSERT from
`support_tickets` into `p1_tickets`, the seven columns named in `p1_tickets`' order, rather
than `SELECT *`, and `WHERE priority = 'P1'` its only filter; no hint, so it reads from the
table's default, the earliest offset, and replays up to 24 hours of tickets; the header opens
section 5, "A second architecture: support tickets": checkout produces tickets rather than
metrics, the reporter sets the priority and Flink routes on it, so only the P1s reach the
agents and their model calls, and P2 and P3 tickets stay in `support_tickets` with nothing
further done to them; it says to start 5.2's INSERT once 5.1 has caught up, as `reset-health`
does). Tests: `tests/test_sql_pipeline.py` (one statement, from `support_tickets` into
`p1_tickets`; the priority its only filter, with no join; `p1_tickets`' DDL columns, named
bare and in order; no hint, against `support_tickets`' earliest-offset default; the module
docstring names section 5). Nothing else needed the lab: `reset`, `reset-health` and the
call-rate check name 5.1 since issue 04, and Terraform runs no INSERT.

Issue 06 (lab 5.2, the ticket classifier): `sql/5.2_ticket_classifier.sql` (new; `CREATE
AGENT ticket_classifier` on `llm_textgen_model`, with no tools and the existing `WITH`
options; the prompt in the existing order: two labelled lines, `Severity: [HIGH | LOW]` and
`Reason:`; three examples, each an approved template filled in and shown the way the INSERT
shows a ticket, then its answer: a HIGH from "Checkout crawling since the campaign email went
out" (template 6), a plain LOW from "Saved cards not loading for me" (17) and a shouting LOW
from "URGENT!!! DISCOUNT CODE NOT WORKING" (13), each Reason naming how many customers and
whether they can still buy or pay, the shouting one saying it is one customer and checkout
works, whatever the capitals; the role, checkout's service desk triage engineer, told that
every ticket is a P1 and reporters over-prioritise; rule 1, HIGH when many customers cannot
complete checkout or pay right now, then rule 2, LOW for everything else, where capitals,
exclamation marks, "urgent" and the P1 never make a ticket HIGH; strict values; no
apostrophe), `sql/5.2_insert_ticket_classifier.sql` (new; `p1_tickets` from the latest offset,
so a restart never re-classifies old tickets with fresh model calls, and no filter, into
`ticket_triage`: the seven ticket columns, the severity, parsed with 4.3's regex and
`agent_status` handling, `HIGH` or `LOW` and otherwise `INVALID`, the reason with 4.3's regex,
and the raw answer; every P1 is written, LOW ones included, so the wall and `evaluate` see each
verdict, and 5.3 reads the HIGH rows; the input names the id, source, priority, subject and
description, the last two in `COALESCE(…, 'N/A')` because `CONCAT` returns null when any
argument is null). `sql/checks/remediation_call_rate.sql` has counted one classifier call per
P1 ticket since issue 04, so the checks are unchanged. Tests: `tests/test_agent_sql.py` (the
output lines and the allowed answers are per agent: the classifier's Severity and Reason, with
no Action or Target line; an example for each allowed answer; the classifier's examples are P1
templates from `opsgen/ticket_templates.py` filled from their pools, one HIGH, one plain LOW
and one shouting LOW; each Reason names the customers and whether they can buy or pay; the
shouting one is one customer while checkout works; the two rules in order; the INSERT joins
the rules keyed by INSERT file, so the latest offset, the agent called by name, `INVALID` for a
failed or out-of-set answer and the arity against the DDL of the table it writes, while the
filter, the identity columns and the null `ticket_id` stay rules of the three 4.3 legs; it
writes `ticket_triage`'s columns in order, parses the severity as the capacity leg parses an
action and the reason as every leg does, and its input carries the five ticket columns; no
`ESCALATE` in any agent file; the module docstring names section 5).

Issue 07 (lab 5.3, the ticket responder): `sql/5.3_ticket_responder.sql` (new; `CREATE AGENT
ticket_responder` on `llm_textgen_model`, with no tools and the existing `WITH` options; the
prompt in the existing order: two labelled lines, `Action: [ROLLBACK | SCALE_OUT |
INVESTIGATE_UPSTREAM]` and `Reason:`, and the sentence that there is no Target line because
there is never a version or a replica count to give; three examples, each a really-HIGH P1
template filled in and shown the way the INSERT shows a ticket, with a Triage Reason line as
the classifier would write it, then its answer: a ROLLBACK from "Blank checkout page after the
release" (template 2), a SCALE_OUT from "Checkout times out during the flash sale" (5) and an
INVESTIGATE_UPSTREAM from "Payment provider rejecting every payment" (10), each Reason naming
its cue, the release, the load or the provider; none of the classifier's templates (6, 13,
17) is reused; the role, checkout's incident commander, told that every ticket was already
classified high severity by the triage agent, whose reason comes with it, and asked for the
one action that fixes it fastest; rules 1 to 3, ROLLBACK for a release, an update, a deploy or
a new version, SCALE_OUT for slowness or timeouts under heavy load (a sale, a campaign, a peak
or a rush), INVESTIGATE_UPSTREAM for payments failing at a payment provider or gateway;
strict values, from the ticket and the triage reason only; no apostrophe),
`sql/5.3_insert_ticket_responder.sql` (new; `ticket_triage` from the latest offset, `WHERE
severity = 'HIGH'` in a derived table below the LATERAL join, as the error-rate leg reads
`error_status = 'HIGH'`, since 5.2 writes every verdict and there is no high-severity topic;
into `remediation_decisions` with spec decision 17's columns: the ticket's `event_time` as
`window_end`, its `service_name`, `ticket_responder`, `HIGH_SEVERITY_TICKET`, the action
parsed with 4.3's regex and `agent_status` handling, `ROLLBACK`, `SCALE_OUT` or
`INVESTIGATE_UPSTREAM` and otherwise `INVALID`, a null target, the reason with 4.3's regex,
the raw answer and the `ticket_id` last; the input is the classifier's five ticket lines, then
`Triage Reason:`, with the reason, the subject and the description in `COALESCE(…, 'N/A')`;
the header says the decisions table is the 4.3 legs' fan-in table and that the simulator
takes no action on a ticket decision, since D-065 resolves incidents and checkout has none).
`sql/checks/` name 5.3 since issue 04 (`remediation_routing.sql` and the responder's line in
`remediation_call_rate.sql`), and `reset`, `reset-health` and `evaluate` name its agent and
file, so nothing else changed. Tests: `tests/test_agent_sql.py` (the responder joins the
per-agent rules: its Action and Reason lines with no Target or Severity line, an example for
each of its three actions, its allowed answers and no apostrophe; its examples are really-HIGH
P1 templates from `opsgen/ticket_templates.py` filled from their pools, each expecting its
example's action, each with a Triage Reason naming many customers who cannot buy or pay, and
each Reason naming its cue; the three rules in order on their cues; the INSERT joins the rules
keyed by INSERT file, so the latest offset, the agent called by name and written as
`agent_name`, `INVALID` for a failed or out-of-set answer, and the arity, and the filter below
the join now covers the 4.3 legs and 5.3; it is one statement from `ticket_triage` into
`remediation_decisions` whose only filter is the HIGH verdict, its values are spec decision
17's, its action is parsed as the capacity leg's with its three actions, and its input carries
the ticket and the triage reason; only this INSERT writes `HIGH_SEVERITY_TICKET`, and it
writes no other trigger; the module docstring names 5.3).

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

Issue 09 (checkout's row: its tickets and agent runs): `scripts/opswall/state.py` (a `Ticket`
per `ticket_id`, filled in from five topics in any order by `apply_ticket`, `apply_p1_ticket`,
`apply_triage`, `apply_answer_key` and `apply_decision`; a record written twice keeps its first
Kafka timestamp, and one without falls back to the ticket's `event_time`; a decision with a
`ticket_id` goes to its ticket and nowhere else, so it never closes a metric block, counts as a
false alarm or sets a metric row's status; each service gets `row`, `"tickets"` for
`TICKET_SERVICES` and `"metrics"` for the others, so the page still holds no service names; a
ticket row gets `tickets` (the newest 5, and `received` over the last 10 minutes), `marks` (the
tickets of the last 2 minutes, `timelineSeconds` 120), `runs` (the classifier's, from the
`p1_tickets` record to the verdict, "High in 4s", and for a HIGH verdict the responder's, from
the verdict to the decision, "Scale out in 6s"; open and "Working 3s" until 30 s after its
start, then ending there as "No answer"; each keyed by lane and ticket, with the subject, the
verdict or action in words and the agent's reason), `openP1` (the open runs' tickets),
`agentsWorking` (their lanes), the status "Agent resolving P1" (`working`) or "Healthy", and
its `issues` from `_ticket_issues`: the really-HIGH P1s created in the last 10 minutes,
detected on a HIGH verdict, missed on LOW, INVALID or no verdict 30 s after the ticket, the
median ticket → HIGH verdict and ticket → the responder's action, whatever it is; `trim()`
drops a ticket 11 minutes after the last thing that happened to it; the docstring),
`scripts/opswall/consumer.py` (ten topics: `support_tickets`, `p1_tickets`, `ticket_triage` and
`ticket_answer_key` from 10 minutes back, the two the labs write with their Kafka timestamps),
`scripts/opswall/app.py` (docstring), `static/opswall.js` (a ticket row from `svc.row`: the
sources, Zendesk and ServiceNow with the design's icons, paths, node, line and ambient dots; the
list, "Tickets" and "N received", the newest 5 styled by priority, each with "ZD-48214 from
Zendesk at 24.11.2026 09:26" in local time; the timeline, with the marks, the Classifier and
Responder lanes and their chips, the bars by key with their labels just left of their start,
each a hover and keyboard stop whose tooltip gives the subject, the verdict or action and the
reason, and "Both agents idle" or "N of 2 agents working"; "Open P1 N" in the header; a ticket
id the previous snapshot didn't have, created in the last 10 s, colours its source by priority,
sends a dot down the path and the line, and fades its row in as the rows slide down, and none
on the first snapshot; the remediate block's title names the ticket), `static/opswall.css` (the
ticket row's layout, colours and animations, the design's; the timeline moves below the sources
and the list under the design's 1320 px; an amber dot for `working`; no animation under
`prefers-reduced-motion`). Tests: `tests/test_opswall_state.py` (the row kind; the list and
`received`; the marks; the classifier and responder runs and their labels, LOW and INVALID
stopping after the classifier; Working, then No answer after 30 s; two runs overlapping in a
lane; a run that ended before the timeline left out; `openP1`, `agentsWorking` and the status;
the Issues numbers; a ticket filling in from its five records in all 120 orders; P2 and P3
listed and marked with no runs; a ticket decision kept out of the metric rows; trimming),
`tests/test_opswall_ingest.py` (ten topics, the lab tables with Kafka timestamps; a record from
each ticket topic through the real Avro path reaches checkout's row), `tests/test_opswall_server.py`
(the ticket row's shape). Offline render in headless Chrome at 1440 and 820 px
(`.scratch/checkout-tickets/research/issue09_wall_render.py`, extending issue 08's, with the
real `Simulation`'s tickets and answer keys, a stand-in classifier wrong on one P1 in twenty
2–6 s after `p1_tickets`, and a stand-in responder answering the expected action 3–8 s after a
HIGH verdict): at a moment with an open run, checkout's columns land on the design's
coordinates, a LOW P1 shows one bar and a HIGH P1 two, the labels, chips, "1 of 2 agents
working" and "Open P1 1" read right, and web-gateway's and payments' rows keep issue 08's boxes;
a bar's hover tooltip and keyboard focus work, and the focused bar survives the next
snapshots. With `--dump` the harness also writes the four ticket tables and the ticket
decisions as JSONL.

Issue 10 (`evaluate` scores the ticket pipeline): `scripts/evaluation/tickets.py` (new; the P1
tickets created in the scored range and found in the answer key; the verdict is the first
`ticket_triage` record's severity, and a ticket with none written by the end of the range plus
30 s is never classified; true severity by verdict, with INVALID and never classified counted
wrong; for really-HIGH tickets classified HIGH, answered or not, the expected action or not, and
INVALID actions; an answer to a really-LOW ticket called HIGH is unexpected, one to a ticket with
no HIGH verdict a routing error, and a target, another agent or another trigger on a ticket
decision is listed; P2 or P3 tickets in `p1_tickets` or `ticket_triage` are leaks; the median
ticket → verdict and verdict → action times, from the Kafka timestamps; acceptance at 95 %, 95 %
and no leaks, `NO_DATA` with no P1 ticket in range, and for the responder until it has
answered), `scripts/evaluation/decisions.py` (`HIGH_SEVERITY_TICKET` → `ticket_responder`, whose
actions are the three; `DecisionRow` reads `ticket_id` and the Kafka timestamp; `score_decisions`
leaves out the rows with a `ticket_id`, so they are never unexpected metric decisions and never
move `counted_from`; the docstring), `detection.py` (docstring), `scripts/common/topic_reader.py`
(`read_topic(..., timestamps=True)` adds each record's Kafka timestamp as `kafka_timestamp`;
`evaluate` reads every table with it, and a dump keeps it), `scripts/evaluate.py`
(`--tickets-topic`, `--ticket-rows` and `--dump-tickets`, and the same for the answer key,
`p1_tickets` and `ticket_triage`; `--rows` or `--ticket-rows` means offline; a missing or empty
ticket table names ops-sim, `sql/5.1_p1_tickets.sql` or `sql/5.2_insert_ticket_classifier.sql`,
and no ticket decisions name `sql/5.3_insert_ticket_responder.sql`; either half runs without the
other's tables, and with no metric rows the tickets set the range by the same rule; the metric
decision half gets the metric decisions alone, so its "read in total" leaves the ticket ones
out; a "Tickets" section after the metric sections, with `--details` listing every wrong verdict
and wrong action with its `raw_response`; the docstring). Tests: `tests/test_evaluate_tickets.py`
(new: the 2×2, INVALID and never classified, the grace, response accuracy, a target, an
unexpected answer, a routing error, a misrouted decision, leaks, the medians, `NO_DATA`, the 95 %
boundaries; through `evaluate.main` with fake reads, the ticket half alone, each missing table's
hint, empty tables, the metric half the same with and without the ticket tables, the dump round
trip with its timestamps and `--details`; `read_topic`'s timestamps over a fake consumer),
`tests/test_evaluate_decisions.py` (a ticket decision is never an unexpected metric decision and
never moves `counted_from`; `from_record` reads `ticket_id` and the timestamp). Offline on dumps
(`.scratch/checkout-tickets/research/issue10_evaluate_dumps.py`, its outputs next to it): the real
`Simulation` (seed 7, 2 h), payments through the AR(1) replica and web-gateway through lab 2.2
with stand-in metric agents fed back, and stand-in ticket agents with one wrong verdict in twenty.
113 P1 tickets in range: the classifier 108/113 (96 %), the responder 51/51, 3 unexpected
answers, no leaks, medians 5.3 s and 5.8 s. The metric half reads the same with and without the
ticket tables: 93/93 incidents detected in time, 96/96 anomalous rows answered, 93/93 actions.

Issue 11 (the docs): `README.md` (the opening paragraph tells three stories with four Streaming
Agents, at about the length it had: web-gateway's bad deploys trip the fixed error-rate
threshold and the release agent rolls them back; payments is watched by ARIMA on latency and
traffic, with the release and capacity agents; checkout sends support tickets from Zendesk and
ServiceNow, Flink routes the P1s, the classifier recovers their real severity into
`ticket_triage` and the responder recommends an action; the release, capacity and responder
agents write `remediation_decisions`, in four INSERTs; the README doesn't describe the wall, so
its new look isn't mentioned), `platform-team/README.md` (`service_metrics` carries a sample a
second for web-gateway and payments only; the Real-Time Context Engine and Lightning sections
are 6–8, since section 5 is now the tickets; the cost paragraph: the metric agents' calls track
incidents, about 45 an hour per developer, and the ticket agents' calls track tickets, about 72
and 36, or 108 an hour; issue 04 had already written the resource list, the 17 resources and
their list, the eleven statements, the pool's eight long-running statements against 10 CFUs, the
readiness row, the files table, and the one "Updating stacks you built earlier" subsection that
drops `remediation_decisions` before the apply). Tests: `tests/test_evaluate_detection.py` (the
echo-tag and end-window tests used a `TRAFFIC_SURGE` on web-gateway; they now use an
`UPSTREAM_DROP` on payments, with the traffic signals flipped). A grep of the tracked tree,
apart from `DECISIONS.md`, `PLAN.md`, `checkpoints/`, `.agents/` and `inspiration/`, finds no
stale "three records per second", "checkout and payments" as the ARIMA pair, `TRAFFIC_SURGE`,
"two agents", "six tables" or "six topics". What it still finds is accurate: the scripts under
`scripts/tuning/research/`, kept as records with their pre-D-068 note; tests and a comment
saying surges are gone; and "two agents" for the two ticket agents on the wall or the two metric
agents in `remediation_routing.sql`. The repo has no markdown link check. A check of the
relative links and anchors in its tracked markdown finds only `PLAN.md` (gitignored) and the
`docs/tracks/PRE-PROVISIONED.md` links, which stay until the walkthrough is written.

Code review (of the merged `checkout-tickets` branch, 2026-11-24): `scripts/evaluation/tickets.py`
(a ticket never classified has no verdict, `TicketResult.verdict` None, so a verdict is only HIGH,
LOW or INVALID and `NEVER` is gone; the counts are unchanged, and `--details` lists such a ticket
as "never classified" rather than "got NEVER"; `TicketTables` moved here from
`scripts/evaluate.py`, and `score_tickets` takes it, the ticket decisions and the range; `INVALID`
comes from `decisions.py`, and `HIGH` and `LOW` stay `evaluate`'s own, since nothing in
`scripts/evaluation/` imports `opsgen`), `scripts/evaluation/decisions.py` (`score_decisions` is
the one place that leaves the ticket decisions out, and its report counts the metric decisions
read, `read`, which `evaluate` prints and checks instead of filtering a second time),
`scripts/evaluate.py` (the acceptance lines' colouring helper is `status_text`, not `verdict`).
ops-sim: `opsgen/simulation.py` (`tick` returns a `Tick` NamedTuple, `metrics`, `deployments`,
`events` and `tickets`, and the tests that took one field by index or unpacked one name it),
`opsgen/tickets.py` (each source is one row of `SOURCES`, a `Source` with its code, name, first
number and id format, in place of the constants, `SOURCE_NAMES`, `FIRST_NUMBERS` and `ticket_id`'s
if/else; the draws and ids are unchanged, `ops-sim --dry-run --simulate 15 --seed 1` prints issue
03's 63 tickets), `opsgen/sinks.py`. `scripts/reset.py`: `--keep-source` keeps the generator state
file, the whole of it. Everything in the file numbers past what the kept source topics hold, the
incident counter (D-046), each service's highest version (D-067) and the two ticket counters, so
deleting it, or any part of it, would let the next ops-sim reuse incident ids, versions and ticket
ids next to the old ones; a plain `reset` empties those topics and still deletes the file
(docstring, help, and the line it prints). `scripts/preprovisioned/provision.py` takes
`SOURCE_TABLES` and `OUTPUT_TABLES` from `scripts/common/topics.py` (it runs under `uv run deploy`
and already imports `scripts.common`). The wall: `scripts/opswall/state.py` builds a ticket row
with its own function, `_ticket_row`, and a metric row with `_metric_row`, each with only what the
page draws for it, so checkout's row no longer carries `charts`, `groundTruth`, `blocks`,
`metrics`, `deploys` or the version fields (issue 02's `charts: []` with them). The snapshot
carries what the page worked out or declared again: each lane's name and state, Working or Idle,
and `agentsText`, "Both agents idle" or "1 of 2 agents working" (the page counted the open runs'
lanes and wrote "of 2"); `sources` and each listed ticket's `sourceName`, from opsgen's table; the
Issues medians in words, `medianTimeToDetectText` and `medianTimeToRemediateText`, by `_dur_words`,
which now rounds a half up, as the page's `issueDur` did; and `actionWords`, one table for the
responder's bars and the metric rows' tooltips, which now say "roll back" as the bars do (the
page's own table said "Rollback"). The wall takes `HIGH` and `LOW` from `opsgen.tickets`, as it
takes the services from `opsgen.services`, and has one `INVALID`. Time to remediate is spec
decision 29's, the first block's time to detect plus its time to answer: it took the earliest
answer of any block, so a later block answered sooner shortened it. While the first block waits
unanswered (until it is answered, or removed 30 s after its last detection), that sum is undefined,
so it runs to the first answer, as before. `static/opswall.js` draws all that, and its `issueDur`,
`ACTION` table, source and lane names and lane-state logic are gone; `static/opswall.css` colours
checkout's status text with its dot, amber for "Agent resolving P1" and green for "Healthy" (spec
decision 35), and a metric row's "Healthy" stays grey (a `ticket-row` class on the row). The labs:
`sql/5.3_insert_ticket_responder.sql` labels the classifier's reason `Classifier said:` instead of
issue 07's `Triage Reason:`, which holds `Reason:`, so an answer that echoed its input would have
had the classifier's reason stored as its own; the prompt's examples and role say the same
(`sql/5.3_ticket_responder.sql`), and the INSERT's header says why. `sql/5.1_p1_tickets.sql`,
`2.2_service_error_rate.sql` and `2.5_service_health.sql` say "then run" where they said "then
Run"; `statements.tf` copies none of them, so Terraform is unchanged. This entry: the Measured
fresh-start figure from the re-run on the merged tree, and the ticket decisions' Kafka timestamp
under Pending (also in the spec's Pending and issue 12's checks). Tests:
`tests/test_evaluate_tickets.py` (never classified is a None verdict, and `--details` names it in
the report's words), `tests/test_reset.py` (`--keep-source` keeps the state file byte for byte),
`tests/test_opswall_state.py` (remediation is the first block's detect plus answer when a later
block is answered sooner, and start → the first answer while the first block waits; a ticket row's
fields and nothing of a metric row's; the lanes and `agentsText`; the sources and `sourceName`; the
medians in words, "1m 05s" and a half rounding up; `actionWords`; the orphan-incident trim test on
web-gateway), `tests/test_opswall_server.py` and `tests/test_opswall_ingest.py` (the new fields),
`tests/test_agent_sql.py` (no INSERT's input label holds a label its answer is parsed by; the
responder's examples and input use `Classifier said:`), and the opsgen tests on `Tick`'s field
names. Offline render with issue 09's harness at 1440 and 820 px (`review-fix-wall-*.png` next to
`issue09-wall-*.png`): at issue 09's moment (seed 4, found after 186 s), the same rows, lists,
bars, labels and Issues numbers as `issue09-wall-*.png`, checkout's "Agent resolving P1" now amber,
web-gateway's "Rolled back" green and payments' "Healthy" grey as before; the chips, "1 of 2 agents
working", the sources' names and a bar's tooltip come from the snapshot, and hover and keyboard
focus work. `uv run evaluate` on `research/issue09-dumps/`: classifier 62/65, responder 29/29, 2
unexpected answers, no leaks, as before.

`opswall-redesign` issue 02 (web-gateway's row: the design's deploy row), decided with the user in
a grilling session on 2026-11-24 (Q1–Q16, which also settled issue 03), resolving issue 01:
`scripts/opswall/state.py` (every row's chart, checkout's timeline included, shows the last
2 minutes, `CHART_SECONDS`, which the snapshot carries once as `chartSeconds` in place of
`axisSeconds` and checkout's `timelineSeconds`; `AXIS_SECONDS` is now `HISTORY_SECONDS`, the
10 minutes the state keeps, the Issues panel counts and the consumer reads back, and the blocks,
incidents and status are still worked out over it, so what has left the chart still counts; a
metric row's kind is `"releases"` for the row with the errors chart; `ServiceState.version` is
kept again from every `service_deployments` record, heartbeat included, and a releases row
carries it and `releases`, the newest 5 DEPLOYs of the last 10 minutes with their `deployedAt` and,
once a ROLLBACK left their version, `rolledBackAfter` and its words, and their `count`;
`previousVersion`, `lastChange`, `secondsSinceDeploy` and the three `ServiceState` fields behind
them are gone, since nothing read them), `scripts/opswall/consumer.py` (the renamed constant),
`static/opswall.js` (web-gateway's row: the laptop, the line with its ambient flow and the Releases
list on the left, "Running v 3.4.2" in the header, the errors chart on the right; a new release
lights the laptop, sends a packet down the line and pushes its row in with the design's fade, and
a rollback replays the fade on its row, which turns accent with "rolled back after 12s" on a second
line; every chart takes the design's chrome: a title with the latest second's value, plots of
200, 112 and 64 px, the dashed grid, `#ECEDEE` lines over a 5 % area, traffic in blue, "Latency,
mean" for the design's "p95"; the errors chart labels 100 %, 50 % and lab 2.2's 25 % on the plot,
a log chart its range in its title line; a deploy line is the design's deploy marker, accent and
dashed with its version, a rollback grey with "rollback" on its other side, versions placed first
and a label that fits nowhere hidden; one axis, "−2m … now", for every row), `static/opswall.css`
(the design's shared pieces, `flow-body`, `src-box`, `flow-line`, `list`, under checkout's row
too, and the releases row's styles; a hidden chart body stays hidden). Tests:
`tests/test_opswall_state.py` (the running version, heartbeat included; no leftovers; the 2-minute
chart against the 10-minute Issues panel; an open block from before the chart; the Releases list,
its count, a rollback's match and one whose deploy is older than the list; the incident scenarios
moved onto the 2-minute chart), `tests/test_opswall_ingest.py` (a live deployment record reaches
the version and the releases) and `tests/test_opswall_server.py`. Offline render at 1440 and
820 px with checkout's feeder (`.scratch/opswall-redesign/research/issue02-wall-*.png`):
web-gateway's row lands on the design's columns, two rolled-back releases read in accent, the
chart shows the bad deploy's box, its version and the answer, checkout's row is as before, and
hover and keyboard focus work.

`opswall-redesign` issue 03 (payments' row: the design's aggregate row). The design's row shows
5 s windows, p95 latency and five sub-services, none of which exist in our data, so the grilling
of 2026-11-24 made it one node, Flink's 10 s windows and the mean: `scripts/opswall/state.py` (a
`HealthRow` keeps its whole record, times in epoch seconds, the newer record for a window winning
and the first detection kept; payments' row kind is `"windows"`, carrying `windowSeconds`, 10, and
`windows`, the newest 7 `service_health` windows with their record and `flags`, each status
column that flags the window after the column it judges, `JUDGED`, and the count flagged in the
last 10 minutes; every metric row now carries its deploy marks, so payments' bad deploys and
rollbacks reach its charts, which settles issue 01's last question), `static/opswall.js`
(payments' row: one "payments" source box with the design's chip icon, its ambient flow into the
Σ box of the window, a line on to "Aggregated records" with "N flagged in 10 min"; each line is the
record's JSON with its own names, `avg_latency_ms`, `traffic_count`, `version`, `window_end`,
`latency_status`, `traffic_status`, cut at the panel's width, its tooltip the whole record; a
flagged status and the value it judges are accent and bold, the value first so it shows on a
line cut short, and the line is tinted; a window the page hasn't seen flushes the box and starts
its 10 s fill again, sends a packet out and pushes its line in, the arrival being the trigger,
and on the first snapshot the fill starts where the newest window's detection left it; "Window
10s" in the header; only a row's top chart labels its deploy lines, as the design labels its
latency markers; the design rows share one body builder), `static/opswall.css` (the row's styles
and the design's fill, flush, out and push animations). Tests: `tests/test_opswall_state.py` (the
newest 7, a whole record in its order, a window written twice, the flags and the flagged count,
only the windows row carries windows, payments' deploy marks, the row kind) and
`tests/test_opswall_ingest.py` (the dumped `service_health` rows reach the records whole). Offline
render at 1440 and 820 px with issue 02's harness, `--row windows`, its feeder writing all 19
columns of `2.5_service_health.sql` from the ARIMA replica
(`.scratch/opswall-redesign/research/issue03-wall-*.png`): payments' row lands on the design's
columns, a bad deploy's window reads `"avg_latency_ms":895.3` in accent on a tinted line, its
tooltip lists the 19 columns, the latency chart labels the deploy and the rollback and the
traffic chart draws their lines, and the other rows are as in issue 02's render.

Amends D-032, D-034, D-040, D-043, D-046, D-050, D-056, D-058, D-059, D-060, D-061, D-065, D-066
and D-067.

### D-069 · DECISION · A demo environment: the platform team's Terraform starts the labs' statements behind `deploy_labs`
**What.** Showing the finished pipeline meant starting the labs' twelve statements by hand,
as in the live checks (`.scratch/checkout-tickets/issues/12-live-check.md`, "Start the
statements"). D-051 removed `reset --with-labs` and left open whether anything replaces it.
**Options.** (a) A `uv` command that submits the `sql/` files with the presenter's card.
(b) The platform team's Terraform, behind a switch that is off by default.
**Choice.** (b), decided with the user in a grilling session on 2026-11-25.
- `deploy_labs`, a root variable that defaults to `false`, adds the labs' 12 statements to
  every developer stack in the apply: the INSERTs of 2.2 and 2.5, the two agents of 4.2, the
  three 4.3 legs, 5.1, then 5.2 and 5.3 (each its agent, then its INSERT). They continue
  D-037's chain after the model, in lab order. A demo stack has 29 resources.
- It is for a demo environment only, in its own state: a `demo` workspace with its own
  `demo.tfvars` and one email. The workshop's apply never sets it, because attendees write
  these statements (D-050), and the switch covers every environment in the apply.
- The statements are copied into heredocs from `sql/`, from each statement's first line, and
  `tests/test_platform_team_sync.py` checks them character for character, because the agents'
  prompts are multi-line strings. The tables are still compared with whitespace normalized.
- The agents keep a plain `CREATE AGENT`, as in `sql/`. Confluent's docs now list `CREATE AGENT
  IF NOT EXISTS`, against D-051's "no `IF NOT EXISTS`". That is untested live, and it would
  make the copies differ from what attendees type.
- Everything runs as `principal_id`, like the tables. The developer's own key only exists
  after `uv run deploy`.
- Only on empty topics: in the apply that creates the environment, or right after `uv run
  reset`. The agent INSERTs read from the latest offset, 2.2, 2.5 and 5.1 replay their
  sources, and `depends_on` waits only until the statement before is RUNNING, not until it
  has caught up. Over existing data, the agents would call the model for every replayed
  anomaly and P1.
- Starting over: stop ops-sim, `uv run reset`, then an apply with `-replace` on the four
  `create_*_agent` statements, then start ops-sim. Their completed statements stay in state,
  so only `-replace` runs them again. The INSERTs that `reset` deleted drop out of state and
  come back on their own. `reset-health` and an apply would not do: the sources would replay
  through the agents.
**Why.** The other eleven Flink statements are already the platform team's Terraform, and
the presenter runs `uv run deploy` and starts ops-sim either way, so a `uv` command saves no
step. Terraform's costs were accepted for a short-lived demo environment: 12 more copies of
the SQL, a separate state for the demo, eight long-running INSERTs owned by a person, a
replaced statement for every SQL change, and the `-replace` on a restart.
**Not checked live.** A demo apply, the restart's `-replace`, and whether deleting a completed
`CREATE AGENT` statement leaves the agent (assumed, as for the tables).
**Impact.** `platform-team/variables.tf` and `main.tf` (the switch, passed to the module);
`platform-team/modules/developer/statements.tf` (12 resources with `count = var.deploy_labs ?
1 : 0`), `variables.tf`, `outputs.tf` (`flink_statements` lists the labs' statements when they
exist) and `main.tf`; `terraform.tfvars.example`; `platform-team/README.md` ("A demo
environment"); `tests/test_platform_team_sync.py` (the exact copies, the switch, the chain);
`platform-team/tests/developer_module.tftest.hcl` and `environments.tftest.hcl` (none without
the switch, all twelve with it, in every environment).

Amends D-051 (its open question) and D-068 ("Terraform runs no INSERT", now true only
without `deploy_labs`).

**Amended by D-070.** `deploy_labs` starts 17 statements: section 6's five follow 5.3's
`INSERT`, in lab order (6.1, lab 6.2's connection and then its tool, 6.3's agent, 6.4). The
connection's endpoint is a new root variable, `patch_agent_url`, in place of 6.2's placeholder,
the one place a copy differs from its file; a plan with `deploy_labs` and no URL fails. A demo
stack has 39 resources, and starting over replaces seven statements: the five agents, the
connection and the tool.

### D-070 · DECISION · Code security incidents: a Streaming Agent that calls an external agent over A2A
**What.** The workshop shows Streaming Agents that only reason over their input. It should
also show one that calls an agent running somewhere else and waits for its answer: the
hierarchical pattern of the user's `dimi-vo/multi-agent-architectures` repo
(`hierarchical-workflow`), where a Flink agent calls LangGraph agents over A2A. Spec:
`.scratch/code-security-incidents/spec.md`.
**Options.** Decided with the user in a grilling session on 2026-11-26 (four rounds). The
branches that mattered:
- The external agent on each participant's laptop behind a tunnel they open, or one shared
  agent. → On each laptop; the presenter's machine and URL are the fallback on the day.
- What the Streaming Agent checks: whether the vulnerable package ships and is used (free
  text), a fixed version existing, known exploitation (KEV/EPSS), or production exposure. →
  The free text. The other three are field checks a `WHERE` can do, so they need no model.
- One agent that calls the tool and waits, or a decision stage and a separate call stage. →
  One agent.
- The external agent with a small local model, or none. → None for now: a LangGraph graph
  whose one node is a function. A model can come later without changing the Flink side.
- How the wall learns the external call's start and finish: in the reply, copied by our
  agent into its answer (a); a Kafka record the external agent writes (b); a local file the
  wall reads (c). → (a). It works the same in the fallback, where (b) and (c) would lose the
  external bar on the participants' walls.
- The decisions in `remediation_decisions`, or a table of their own. → Their own: every
  reader of `remediation_decisions` would misread them (the wall routes any `ticket_id` to
  checkout and drops unknown services; `FIX_ACTIONS` keys a label dict; `evaluate` counts
  unknown agents as misrouted).
**Choice.**
- **Story.** `ops-sim` streams CVE scan findings for River Shop's repositories into
  `cve_scans`, about 4 a minute on their own clock, with a `cve_answer_key`. Lab 6.1 keeps
  CVSS ≥ 9.0 in `critical_cves`. Lab 6.3's `security_agent` reads the scanner's sentence and
  answers `PATCH` or `DISMISS`. On `PATCH` it calls `patch_agent_tool` (lab 6.2) with one
  fixed line, `PATCH <repository> <cve_id> <package> <installed_version> -> <fixed_version>`,
  waits, and copies the reply into a `Patch:` line. Lab 6.4 writes `patch_decisions`, with
  the commit and the patch's start and finish parsed out of that line.
- **`patch_agent`** is started with `uv run patch-agent --public-url <tunnel URL>`: a
  LangGraph graph with no model, served over A2A by `a2a-sdk` (the latest release), in the
  main dependencies. It waits 4–10 s, then answers "Patched … in commit <7 hex digits of the
  request's SHA-1>, started …Z, finished …Z", or echoes a request it can't parse. It accepts
  any credentials, prints each call, and prints lab 6.2's SQL with its URL filled in, since
  `ALTER CONNECTION` can't change an endpoint.
- **Section 6**, "A third architecture: code security incidents". RTCE and Lightning move to
  sections 7–9, as they moved to 6–8 when the tickets became section 5.
- **`patch_status`** is derived by the INSERT: `PATCHED` with a commit, `FAILED` for a
  `PATCH` without one, `N/A` otherwise. The agent is never asked to invent a commit.
- **Agent settings.** Its own `WITH` block: `max_iterations` 3, `request_timeout` 60. The
  tool: `request_timeout` 30 and `max_retries` 0, since a retry would patch twice. That keeps
  one decision under the wall's 60 s give-up.
- **The model** is `llm_textgen_model`. Confluent's docs don't say that a Confluent-hosted
  model can call tools, so a live smoke test comes first; if it fails, a Bedrock connection
  model, as in the multi-agent repo, is brought back to the user before anything builds on it.
- **The wall** gets a fourth row, "Code security incidents", at the bottom: a laptop with
  "git" under it, a stream with a dot per finding, a "Scan findings" list (repository and
  score), and a timeline with a "Security agent" lane and an external lane, dashed, in its
  own colour, chipped "External · LangGraph · A2A". Our bar is hatched while it waits on the
  patch agent. It reads "Patch agent unreachable", in red, for a `FAILED` patch and for an
  `INVALID` whose answer starts "Exception while initializing A2A client", which is what a
  stopped patch agent gives: Flink fails at its agent card before the model answers, so lab 6.4
  can't write `FAILED` for it. Any other `INVALID` reads "Invalid answer". Its Issues panel
  scores against `cve_answer_key`, in spec decision 26's words: critical findings, right
  decisions, missed ones (wrong, `INVALID`, the unreachable ones among them, or none within 60 s
  of lab 6.1's record, so a right one that comes later too), and the median times to decide and
  to patch.
- **Terraform** creates the four tables and turns RTCE on for `patch_decisions`
  (`confluent_rtce_topic`). `remediation_decisions` stays a Console step for attendees, as in
  the reference workshop. Section 6 joins `deploy_labs`, with a `patch_agent_url` variable.
- **`reset`** drops `security_agent`, then the tool, then the connection (all three take
  `IF EXISTS`), and empties the four topics.
- **Deferred.** `evaluate` scoring for the findings, and which tunnel the lab recommends.
**Why.** It shows the A2A pattern on a story that needs a model (free text decides, not a
score), keeps the four tuned agents and their readers untouched, and keeps the external side
trivial, since the showcase is the call, not what the external agent can do.
**Not checked live.** Issue 02's smoke test (2026-11-26: a throwaway agent on
`llm_textgen_model` with `patch_agent_tool` and a one-line prompt, `patch-agent` behind a
cloudflared quick tunnel) answered the first questions. A Confluent-hosted model calls an A2A
tool, and that agent copied the reply exactly, `→` included. Flink fetches the card at the
connection's endpoint plus `agent_card_path` on every call, then posts `message/send` (A2A 0.3)
to `/`, with Basic auth from the connection's username and password on both, and never used the
card's `http://localhost:8001/` interface. A stopped patch agent fails at the card fetch, before
the model answers, so the whole agent call's answer is Flink's error alone ("Exception while
initializing A2A client for agent: … ERROR: Failed to obtain agent card: 502"), which lab 6.4
writes as `INVALID` and `N/A`, not `FAILED`. Still unchecked: whether `security_agent`, with its
own prompt and three lines, copies the reply exactly; whether `message/send` goes to the card's
URL or the connection's endpoint (the same host there); whether a stopped patch agent fails
`DISMISS` findings too, as the error's place suggests, with which `agent_status` and after how
long; whether `patch_decisions` records get their Kafka timestamp when written (D-068's
Pending, again).
**Impact.** Filled in by the issues under `.scratch/code-security-incidents/issues/`.

Issue 01 (`uv run patch-agent` and lab 6.2's SQL): `pyproject.toml` (`a2a-sdk[http-server]>=1.3.1`
and `langgraph>=1.2.24` in the main dependencies, the latest releases on 2026-11-26, and the
`patch-agent` script; `uv.lock` is refreshed but stays untracked). `scripts/patchagent/` (new):
`reply.py` (the contract, with no I/O: `REQUEST`, the request line's five fields, matched whole,
with surrounding whitespace and a trailing full stop tolerated and the CVE id in its `CVE-` form;
`commit_for`, the first 7 hex digits of the request's SHA-1; `format_reply`, "Patched
CVE-2026-41234 in checkout: bumped urllib3 1.26.4 → 1.26.18 in commit …, started …Z, finished
…Z" in UTC with milliseconds, or "Patched (<request>) in commit …" with the text echoed on one
line; `REPLY_COMMIT`, `REPLY_STARTED` and `REPLY_FINISHED`, one numbered group each and nothing
Java's regex lacks, for issue 07 to test lab 6.4 against), `graph.py` (a `StateGraph` with one
async node, `patch`, which draws a delay between `--min-delay` and `--max-delay`, 4 and 10 s by
default, streams it on LangGraph's custom stream, sleeps, and writes the reply; a comment says a
model node can replace it without changing the A2A side), `executor.py` (an a2a-sdk 1.3.1
`AgentExecutor`: the task goes submitted, working with "patching… N s", then completed with the
reply as its `result` artifact, as the multi-agent repo's executor does; an empty request is
refused as invalid params), `app.py` (the agent card, "Patch Agent" with one skill, "Patch a
vulnerable dependency", text in and out, its description giving the request line, and the
`--public-url` interface before `http://<host>:<port>/`, both JSON-RPC and A2A 0.3 as in the
multi-agent repo, so the 0.3 card's `url` is the tunnel; the JSON-RPC route at `/`, with 0.3's
method names kept on beside 1.0's; no authentication; at start-up, lab 6.2 with the endpoint
filled in, or a warning, the file as written and a line saying a tunnel is needed; a block per
call: the time, the request, "patching… N s", the reply; `--verbose` adds a line per HTTP request
with its method, path, JSON-RPC method and the Authorization header's scheme, never its value;
`--port` 8001 and `--host` localhost; a public URL without a scheme, a negative delay and delays
out of order are refused). `sql/6.2_patch_agent_tool.sql` (new; `CREATE CONNECTION
patch_agent_connection` of type `a2a` with the endpoint `<your patch-agent URL>` and `workshop`
as username and password, then `CREATE TOOL patch_agent_tool` with the card's path, a 30 s
timeout and no retries; the header opens no section and says what the two make, to run them one
at a time, to start `patch-agent` first, a marked `TODO (tunnel)`, that nothing checks the
credentials, why there are no retries, and that a new tunnel URL means dropping the tool and the
connection and creating both again, since `ALTER CONNECTION` changes only credentials). Tests:
`tests/test_patch_agent.py` (new, offline: the request line and its fields, free text rejected;
the commit; the reply both ways, its times in UTC, and the three regexes reading it back with the
finish not before the start; the graph with the delays at 0, and its delay within its bounds;
the server in-process, delays at 0: the card lists the public URL first, a send-message call
completes with the reply as its `result` artifact on A2A 1.0's `SendMessage` and on 0.3's
`message/send`, any basic-auth header or none is accepted, an empty request is refused, each call
prints its block, `--verbose` prints the scheme alone; the printed SQL is the file with only the
placeholder replaced; lab 6.2's two statements and their options, no stray apostrophe, the
placeholder once, no section opened and the tunnel TODO; the CLI's defaults and refusals). A
local `uv run patch-agent --min-delay 0 --max-delay 0` answered `curl` on both routes, and with
the default delays one call took 9.5 s. `platform-team/` is unchanged: section 6 joins
`deploy_labs` in issue 08.

Issue 03 (the finding generator: templates, the scan clock, the answer key, the two source
tables): `opsgen/finding_templates.py` (new; 20 templates as data, each a `FindingTemplate` with
an expected action, a `DISMISS` reason, the repositories it may name and the scanner's sentence:
6 `PATCH`, naming only web-gateway, checkout or payments, where the package is a runtime
dependency of the deployed service and the vulnerable code is called; 6 `DISMISS`, 2 each for
`dev_only` (test or build tooling only), `archived` (storefront-legacy only) and `not_called`
(shipped, no call to the vulnerable function); 8 ordinary findings in any of the five
repositories; no sentence says "patch" or "dismiss" or holds an apostrophe; the `Advisory` pools,
6 critical and 6 ordinary, each a fictional `CVE-2026-4xxxx` on a real package with its installed
and fixed versions, its vulnerable function and what the flaw does, the two pools sharing no CVE
so a CVE is critical everywhere or nowhere, and `CVE-2026-41234` being the patch agent's urllib3
example; the `{calls}` pool; `fill`, which draws the advisory, the other placeholders and the
repository in sorted name order and returns the finding's columns), `opsgen/findings.py` (new;
`ScanClock`: Poisson(1/15) a tick, 30 % critical, half of those `PATCH` and the rest `DISMISS`
with the three reasons equally, CVSS 9.0–10.0 for a critical finding and 4.0–8.9 otherwise, one
decimal, drawn in a fixed order; one id counter, `SCAN-000001` on, a `cve_scans` record and a
`cve_answer_key` record per finding, both stamped with the tick; off with `--no-incidents`),
`opsgen/simulation.py` (the clock on `Random(seed + 4)`, which nothing else used; `Tick` gains a
fifth list, `findings`; `state_snapshot` saves `next_finding_number`, and a state file without it
starts at 1; the class docstring lists the five generators), `opsgen/driver.py` (findings go to
the sink; a tick with a finding saves the state; `--simulate` prints a line per finding and ends
with `# findings: N in M min, X a minute` and the critical ones by expected action, or `#
findings: off (--no-incidents)`; the banner adds `findings: 1/15s critical=30% PATCH=50%`, and
reads `no incidents, no tickets, no findings`), `opsgen/sinks.py` (`DryRunSink.emit_findings`:
the `cve_scans` and `cve_answer_key` stream tags and the console line `FINDING  SCAN-000001
9.4  checkout  CVE-2026-41234  urllib3 1.26.4 -> 1.26.18  [truth: PATCH]`, or `[truth: not
critical]`), `opsgen/kafka_io.py` (`FINDINGS_TOPIC` and `CVE_ANSWER_KEY_TOPIC`, their serializer
contexts, `emit_finding`, `emit_cve_answer_key` and `emit_findings`; raw-string keys, `repository`
and `finding_id`; schemas looked up, never registered), `opsgen/cli.py` (the live start line
names the two topics; the docstring; no new flags, so `--show-records` shows no findings),
docstrings in `opsgen/{__init__,state}.py`. Every caller that unpacked `Tick`'s four lists takes
the fifth: `scripts/tuning/sweep.py` and the opsgen tests, with `emit_findings` on their recording
sinks. The tables: `sql/source_cve_scans.sql` and `sql/source_cve_answer_key.sql` (new; the
columns of spec decisions 8 and 9, `cvss_score` a `DOUBLE`, every column commented, and
`support_tickets`' options, bucket and 5 s watermark; the answer key's header says it is for the
wall and `evaluate` only), `schemas/cve_scans.avsc` and `schemas/cve_answer_key.avsc` (new),
`platform-team/modules/developer/statements.tf` (two statements byte-identical to the SQL,
chained after `ticket_answer_key` and before `service_error_rate`; the header's counts, 13
statements and 19 resources, 25 and 31 with `deploy_labs`), `outputs.tf` (both in
`flink_statements`), `modules/developer/main.tf`, the root `main.tf` and the root `outputs.tf`'s
validation text (twelve tables). The topic lists, `reset` and the readiness check are issue 04's;
the platform-team README's counts are issue 11's. Tests: `opsgen/tests/test_findings.py` (new:
6/2/2/2/8 by category; the five repositories; `PATCH` and `not_called` naming only the three
services, `archived` only storefront-legacy, ordinary ones any; every placeholder has a pool and
every template fills with none left over, its columns the advisory it names; the same seed fills
the same; fictional unique CVE ids, each fixed version above its installed one, the spec's
urllib3 example; no "patch", "dismiss" or apostrophe; one or two sentences; each critical
template's cues by keyword, shipped and called for `PATCH`, shipped and not called for
`not_called`, dev or build tooling for `dev_only`, archived for `archived`, and no advisory or pool
value adding one; the rate, the critical share and the `PATCH` share over 20 hours, and the three
reasons about a third each; the scores' ranges and both ends; one answer key per finding on the
same tick, null exactly when not critical; ids from `SCAN-000001`, increasing through a restart
from the state file, saved on every tick with a finding, and from 1 with a pre-D-070 state file;
findings on, off or 30 times as many leave metrics, deploys, incidents and tickets as they were;
the clock on `seed + 4`; the same seed gives the same findings; `--no-incidents` gives none),
`opsgen/tests/test_output.py` (the dry-run records and console lines, a live tick sending both
streams with the DDL's columns, `--show-records` changing nothing for findings, the `--simulate`
lines from the first minute, their summary, the banner, and both clocks off with
`--no-incidents`), `opsgen/tests/test_tickets.py` (tickets on, off or many leave the findings
too), `tests/test_kafka_io_and_rtce.py` (a finding to `cve_scans` keyed by its repository and its
answer key to `cve_answer_key` keyed by its id, never registering), `tests/test_schema_vs_ddl.py`
(both pairs, the columns in order, `support_tickets`' tail, every column commented and the answer
key's header), `tests/test_platform_team_sync.py` (`COPIED_TABLES`, twelve tables, the chain), and
in `platform-team/tests/` the counts (13, 25) and a plan run on the two tables' options.
`uv run ops-sim --dry-run --simulate 15 --seed 1` gives 63 findings, 20 critical (13 `PATCH`, 7
`DISMISS`), and leaving out the finding lines, the banner and the closing summary, the same output
line for line as the branch's base. The templates wait on the user's approval (issue 03's
Comments) before issue 06 takes its examples from them.

Issue 04 (`critical_cves`, `patch_decisions`, RTCE, the topic lists, reset, reset-health and
readiness): `sql/sink_critical_cves.sql` (new; `cve_scans`' nine columns and the WITH block,
bucket and 5 s watermark of `p1_tickets`, which are `support_tickets`'; the header says lab 6.1
writes it, lab 6.4 reads it, that a score under 9.0 in it is a filter leak, and that
read-uncommitted spares the security agent a wait of about a minute per finding, D-063),
`sql/sink_patch_decisions.sql` (new; the finding's `event_time`, `finding_id`, `repository`,
`cve_id` and `cvss_score` `DOUBLE`; `action` STRING NOT NULL, `PATCH`, `DISMISS` or `INVALID`,
and `reason`; `patch_status` STRING NOT NULL, `PATCHED`, `FAILED` or `N/A`, worked out by lab
6.4 and not the agent, `commit_sha`, and `patch_started_at` and `patch_finished_at`
`TIMESTAMP(3)`, null unless parsed; `raw_response`; the same options; the header says lab 6.4
writes it, that Terraform turns RTCE on for it, and that nothing in it says what the finding
really called for). Terraform: `platform-team/modules/developer/statements.tf` (both statements,
byte-identical to the SQL, chained after `ticket_triage`, the model's `depends_on` moved to
`patch_decisions`; a `confluent_rtce_topic.patch_decisions` with the cluster's cloud and region,
the environment, the cluster, `topic_name` and the description the provider requires, waiting
for the table's statement and off the chain, since nothing waits for it; the header's counts,
15 statements and 22 resources per developer, 27 and 34 with `deploy_labs`), its `outputs.tf`
(both keys in `flink_statements`, and a new `rtce_topics` output, by topic with its environment
and cluster, since the root's tests can't reach a module's resources), its `main.tf`, the root
`main.tf` and the root `outputs.tf`'s validation text (fourteen tables), `platform-team/README.md`
(step 5: `confluent rtce region list` must list the region now, since the apply turns RTCE on;
the rest of the README's counts are issue 11's). The lists: `SOURCE_TOPICS` in
`scripts/common/topics.py` gains `cve_scans` and `cve_answer_key`, `OUTPUT_TOPICS`
`critical_cves` and `patch_decisions`, so `uv run reset` empties all fourteen, `uv run
reset-health` deletes whatever writes the two new tables (lab 6.1's and 6.4's INSERTs) and
empties them, and the readiness check of `uv run deploy` checks all fourteen tables;
`scripts/reset.py` (`LAB_AGENTS` gains `security_agent`; after the five agents, `DROP TOOL IF
EXISTS patch_agent_tool` and then `DROP CONNECTION IF EXISTS patch_agent_connection`, each
reported like an agent, a failed one a problem without stopping the rest; the docstring, the
`--keep-source` help, and the closing lines name the INSERTs of 6.1 and 6.4 and lab 6.2's
connection and tool beside the agents of 6.3), `scripts/reset_health.py` (the docstring, the
help and its seven tables, and a fourth next step: 6.1, then 6.4's INSERT once 6.1 has caught
up, with `uv run patch-agent` and its tunnel started first),
`scripts/preprovisioned/provision.py` (docstring and comment counts), `scripts/setup_rtce.py`
(a `patch_decisions` Lightning example, the latest five `PATCHED` decisions with their commits,
so `--lightning patch_decisions` works). The checks: `sql/checks/remediation_call_rate.sql`
(section 6's rows: `critical_cves` rows, `patch_decisions` rows, the `PATCH` decisions, one
patch agent call each, and the `PATCHED` ones) and its README. Tests:
`tests/test_schema_vs_ddl.py` (both tables' columns in order, `action` and `patch_status` the
only NOT NULL columns, each naming its three values, the two patch times `TIMESTAMP(3)`,
`p1_tickets`' tail, and a comment without an apostrophe on every column),
`tests/test_platform_team_sync.py` (fourteen tables; the chain with the two after
`ticket_triage`; the RTCE topic on `patch_decisions`, waiting for its table; a resource body
now ends at the next resource of any type), `tests/test_reset.py` (fourteen topics, the source
topics being every `*_TOPIC` in `opsgen/kafka_io.py`; the five agents, then the tool, then the
connection, each with `IF EXISTS`, and how each is reported; a failed agent or tool drop exits
nonzero after the rest are dropped; the next steps), `tests/test_reset_health.py` (seven
topics; the matcher on both INSERTs and not on the two CVE sources; both deleted and emptied;
the seven tables named; the next steps in lab order, with "once 6.1 has caught up"),
`tests/test_preprovisioned_readiness.py` (fourteen tables; each missing CVE table names the
platform team), `tests/test_kafka_io_and_rtce.py` (the `patch_decisions` example and its curl),
and in `platform-team/tests/` the counts (15, 27), a plan run on the two tables' options and NOT
NULL columns, an apply run on the RTCE topic's environment, cluster, cloud, region and
description and on `rtce_topics`, and a root run checking every environment has one, on
`patch_decisions`, in its own environment and cluster.

Issue 05 (lab 6.1, route the critical findings): `sql/6.1_critical_cves.sql` (new; one INSERT
from `cve_scans` into `critical_cves`, the nine columns named in `critical_cves`' order, rather
than `SELECT *`, and `WHERE cvss_score >= 9.0` its only filter; no hint, so it reads from the
table's default, the earliest offset, and replays up to 24 hours of findings; the header opens
section 6, "A third architecture: code security incidents": a scanner reports CVEs in River
Shop's repositories, each with a CVSS score, Flink keeps the critical ones, 9.0 and above,
CVSS's own "Critical" band, so only they reach the security agent and its model calls, and the
rest stay in `cve_scans`; a critical score is not always a real risk (a dev dependency only, an
archived repository, vulnerable code never called), so the agent of 6.3 reads where the package
sits and only then calls the external patch agent of 6.2; `critical_cves` already exists
(Terraform), so this one statement is all you write; start 6.4's INSERT once 6.1 has caught up,
as `reset-health` says; running it twice copies every finding twice, and `uv run reset-health`
starts clean). Tests: `tests/test_sql_pipeline.py` (one statement, from `cve_scans` into
`critical_cves`; the score its only filter, with no join; `critical_cves`' DDL columns, read from
`sql/sink_critical_cves.sql`, named bare and in order; no hint and no `scan.startup.mode`,
against `cve_scans`' earliest-offset default; the header's first line opens section 6, the
counterpart of 6.2's test that it doesn't; the module docstring names section 6). Nothing else
needed the lab: `reset`, `reset-health`, the call-rate check and its README name 6.1 since issue
04, and `deploy_labs` starts none of section 6's statements until issue 08.

Issue 06 (lab 6.3, the security agent): `sql/6.3_security_agent.sql` (new; `CREATE AGENT
security_agent USING MODEL llm_textgen_model`, the prompt, then `USING TOOLS patch_agent_tool`,
the only `USING TOOLS` in the workshop, and a `WITH` block of its own: `handle_exception`
continue, `max_consecutive_failures` 3, `max_iterations` 3, which no other agent sets, and
`request_timeout` 60. The prompt in 5.2's and 5.3's order: the three lines, `Action: [PATCH |
DISMISS]`, `Reason: [one or two sentences]` and `Patch: [the answer from patch_agent_tool, copied
exactly | N/A]`, reworded from "the patch agent's answer" for the apostrophe rule; four examples,
each a finding shown as lab 6.4 will show it (`FINDING <id> in <repository>`, `CVE: <cve_id>
(CVSS <score>)`, `Package: <package> <installed_version>, fixed in <fixed_version>`, `Scanner
said:`) and filled in from an approved template and a critical advisory: `PATCH` from template 3,
jsonwebtoken in payments, with the request it sends (`PATCH payments CVE-2026-46471 jsonwebtoken
8.5.1 -> 9.0.0`) and the reply patch-agent gives that request (commit `e73f47b`, a 6.4 s patch),
copied into its Patch line; `dev_only` from template 7, jackson-databind in checkout; `archived`
from template 10, lodash in storefront-legacy; and `not_called` from template 12, the same
jsonwebtoken CVE in web-gateway, so one critical flaw shows up on both sides and only where the
package sits tells them apart; the role, River Shop's application security engineer; rule 1,
`PATCH` when both hold (a runtime dependency of a deployed service, and the vulnerable code used),
rule 2, `DISMISS` when any of the three reasons holds, then "The CVSS score is always critical
here. It never decides on its own."; the tool, only for `PATCH`: one call with exactly the
request line and nothing else, its answer copied exactly into the Patch line, `Patch: FAILED` and
a few words if the call fails, never a commit the tool did not return, and on `DISMISS` no call
and `Patch: N/A`; strict values. The header: the input and the output, why a critical score is
not enough, the tool and why only this agent has `max_iterations`, the 60 s against the wall's
give-up and the tool's 30 s, that the tool and `patch-agent` must exist, and that `uv run reset`
drops the agent). Tests: `tests/test_agent_sql.py` (the security agent joins `AGENT_FILES`,
`OUTPUT_LINES` with `Action:`, `Reason:` and `Patch:`, and `ALLOWED_ANSWERS` with `PATCH` and
`DISMISS` from `opsgen/finding_templates.py`, where lab 6.4's tests will find them, so the
per-agent tests hold it to its lines in order and no other agent's, an example per action and no
other, the actions stated in STRICT VALUES, no apostrophe and the failure handling; the prompt is
read up to `USING TOOLS` or `WITH`; its own tests: the three lines; the rules in order with their
cues; `USING TOOLS` naming the tool lab 6.2 creates, the prompt naming it too, no other file in
`sql/` with `USING TOOLS` and no other agent with `max_iterations`; the tool rules; the stated
request line holds `REQUEST`'s fields in its order and, filled in with any critical advisory,
parses back into them; one `PATCH` example and one `DISMISS` per reason, each an approved
critical template expecting its action, filled with the advisory its CVE line names, a
repository the template may name and a value of each pool, with no placeholder left, a critical
score and an id of its own; the `PATCH` example's request is the stated line filled in from its
finding, its reply is `format_reply`'s for that request with a delay within patch-agent's
bounds, and its Patch line is that reply; the `DISMISS` examples call nothing and write `N/A`;
each Reason names its cue; the four `WITH` options, and a `request_timeout` longer than the
tool's). `platform-team/` is unchanged: section 6 joins `deploy_labs` in issue 08.

Issue 07 (lab 6.4, the security agent's INSERT): `sql/6.4_insert_security_agent.sql` (new; one
INSERT from `critical_cves`, read from the latest offset in a derived table with no WHERE, into
`patch_decisions`, its values in the DDL's order: the finding's `event_time`, `finding_id`,
`repository`, `cve_id` and `cvss_score` as they came; `action`, the capacity leg's `CASE` with `IN
('PATCH', 'DISMISS')`; `reason`, the Reason line read to the end of its line (`[^\n]+`), since the
Patch line follows it; `patch_status`, `PATCHED` when the call succeeded, the Action line says
`PATCH` and the Patch line holds a commit, `FAILED` for such a `PATCH` without one, and `N/A`
otherwise, all three in one flat `CASE` that repeats the Action test, keeping 5.2's shape;
`commit_sha`, `patch_started_at` and `patch_finished_at`, each the first match on the Patch line of
`\*{0,2}Patch:\*{0,2}[^\n]*?` followed by `REPLY_COMMIT`'s, `REPLY_STARTED`'s or `REPLY_FINISHED`'s
pattern, whatever the action, the times through `TO_TIMESTAMP` with `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`
(its quotes doubled in the SQL string), null when they do not parse; `raw_response`. The input is
one `CONCAT` in the format of 6.3's examples, `FINDING <id> in <repository>`, `CVE: <cve_id> (CVSS
<score>)`, `Package: <package> <installed_version>, fixed in <fixed_version>` and `Scanner said:
<finding>`, with each text column in `COALESCE(…, 'N/A')` and the score cast bare, since 6.1 copies
no null score. The header: what it reads and writes; the latest offset, since every row is a model
call and, on `PATCH`, a call to the laptop; start it after 6.1's replay, with `patch-agent` and its
tunnel running; the parsing; `patch_status` worked out here, not by the agent; a stopped
patch-agent's answer, Flink's agent card error (issue 02), is `INVALID` and `N/A`; the time zones,
checked: `event_time` holds UTC (ops-sim's epoch milliseconds in a `TIMESTAMP(3)`, which the wall
reads as UTC), and `TO_TIMESTAMP` reads a string "under the 'UTC+0' time zone" (Confluent's
reference, read on 2026-11-26) into a `TIMESTAMP(3)`, whatever the workspace's session zone, so the
patch's times are UTC as well, where `TO_TIMESTAMP_LTZ` and a cast would have brought the session
zone in (D-054); the `COALESCE`s; running it twice writes every decision twice). Tests:
`tests/test_agent_sql.py` (6.4 joins `INSERTS`, so the latest-offset, `INVALID`, call-by-name,
arity, select-star, `ESCALATE` and input-label rules hold for it; the Reason test takes two endings,
to the end of the answer when Reason is the agent's last line, to the end of its line when another
follows; a small evaluator of the SQL the INSERT uses, with Flink's null rules, Java's find for
`REGEXP_EXTRACT` and `TO_TIMESTAMP`'s pattern turned into `strptime`'s, runs the select list and
the input on findings and answers: one statement with no filter; the columns in the DDL's order,
the finding's five copied; the action `CASE` the capacity leg's with the two actions; the labels
6.3 declares, the three patch patterns being the reply patterns after the Patch label; the reply
contract, `format_reply` for a parsed and an unparsed request with times given in CET, giving back
the commit and both times in UTC, 1.62 s after the finding's `event_time`; `patch_status` for
eleven answers: a patch, the same in markdown bold or after the example's tool lines, `Patch:
FAILED`, a reply copied without its commit, no Patch line, a `DISMISS`, an action out of the set
(its commit still read), issue 02's A2A error with `SUCCESS` or another status, and a failed
call whose answer says `PATCH`; the input built from each of 6.3's examples is that example's four lines, word for word; a
null in any text column still gives an input; each of 6.3's example answers parses back to its
action and reason, the `PATCH` one to `PATCHED` with its request's commit and its reply's times;
the header). `platform-team/` is unchanged: section 6 joins `deploy_labs` in issue 08.

Issue 08 (section 6 behind `deploy_labs`): `platform-team/modules/developer/statements.tf` (five
statements with `count = var.deploy_labs ? 1 : 0`, on the chain after 5.3's `INSERT` in lab
order: `insert_critical_cves`, `create_patch_agent_connection`, `create_patch_agent_tool`,
`create_security_agent` and `insert_security_agent`, each heredoc generated from its file,
lab 6.2's being two, the connection and then the tool; the connection's endpoint is
`${var.patch_agent_url}` in place of `<your patch-agent URL>`, the one difference, and a comment
says the password is in the SQL text as in the file, so in the state and the Console, since
Flink requires one and patch-agent checks neither, and that a new URL replaces the statement,
which fails while the connection exists; a `precondition` on the connection fails the plan when
the URL is empty, rather than a validation on the variable, which could read `deploy_labs` only
from Terraform 1.9, against the root's `>= 1.3.0`; the header's counts, 32 statements and 39
resources with `deploy_labs`; the labs' comment: seventeen, 6.1 among the replays, 6.4 among
the latest-offset reads, and `-replace` on the five agents, the connection and the tool), its
`variables.tf` (`patch_agent_url`, no default, like the module's other inputs), `outputs.tf`
(the five in `lab_statements`), the root `variables.tf` (`patch_agent_url`, a string defaulting
to `""`, with a validation: empty, or a URL with an `http` or `https` scheme, no space or
apostrophe, since it goes into a SQL string, and no trailing slash, since Flink adds the card's
path to it, where patch-agent strips one; `deploy_labs`' description, 17 statements and the
URL; `flink_max_cfu`'s description names 6.1 and 6.4 as two more long-running statements, not
yet checked against 10), the root `main.tf` (passes it), `terraform.tfvars.example` (beside
`deploy_labs`), `platform-team/README.md` ("A demo environment": the 17 statements; the URL, the
plan failing without it and the password in the SQL text; a first step, starting `uv run
patch-agent` and its tunnel and keeping both running while `ops-sim` runs, since Flink fetches
the card on every call, issue 02; `patch_agent_url` in `demo.tfvars`; 39 to add; ten `INSERT`s
running and 22 statements completed; 6.1 among the replays; starting over: `reset` drops five
agents, the tool and the connection and empties fourteen topics, the URL checked, `-replace` on
seven statements, `17 to add, 0 to change, 7 to destroy`; a new tunnel URL takes the same
steps; and the Files table). Tests: `tests/test_platform_team_sync.py` (the five in
`COPIED_LAB_STATEMENTS`, 6.2 twice; the extractor splits a file at `INSERT INTO`, `CREATE
AGENT`, `CREATE CONNECTION` and `CREATE TOOL` and copies a file's statements in its order; each
copy character for character, with `PLACEHOLDER`, imported from `scripts/patchagent/app.py`,
replaced by `${var.patch_agent_url}` for the connection alone; every statement of a copied file
copied; the placeholder once in 6.2, as its endpoint, and in no heredoc;
no copied file holding `${` or `%{`, which a heredoc would read as a template; the five behind
the switch; the chain), `platform-team/tests/developer_module.tftest.hcl` (none of the
seventeen without the switch; with it and a URL, 32 statements, the ten `INSERT`s' targets, the
five agents on `llm_textgen_model`, the security agent's `USING TOOLS`, the connection's
endpoint the URL with no placeholder left, the tool on the connection; with it and no URL, the
plan fails on the connection), `environments.tftest.hcl` (32 statements in every environment
with the switch and a URL; a URL without its scheme or with a trailing slash fails the plan on
the variable). Amends D-069 (its "Amended by D-070" paragraph).

Issue 09 (the wall's consumer, state and snapshot for the "Code security incidents" row):
`scripts/opswall/consumer.py` (four more threads, fourteen, each from about 600 s back:
`cve_scans`, `critical_cves` with its Kafka timestamp, `cve_answer_key`, `patch_decisions` with
its Kafka timestamp; no key decoded), `scripts/opswall/state.py` (a `Finding` per `finding_id`,
filled in from the four topics in any order as a ticket is: the scan's fields from the first
record that carries them, `critical_at` the `critical_cves` record's Kafka timestamp, the
decision's action, reason, patch status, commit, patch start and finish and its answer's Patch
line, `decided_at` its Kafka timestamp, a record written twice keeping its first timestamp and the
newer content, and `expected_action` from the answer key; `apply_finding`,
`apply_critical_cve`, `apply_cve_answer_key` and `apply_patch_decision`, which never goes near
`apply_decision`; trimmed like tickets; `CRITICAL_CVSS` 9.0, lab 6.1's rule; the row, `row:
"security"`, "Code security incidents", appended after the three services by its own function:
the security agent's run from the `critical_cves` record, or from the finding's `event_time`
when only the decision has come, to the decision, reading "Patched in 12s", "Dismissed in 8s",
"Patch agent unreachable" (`FAILED`, flagged `unreachable` for the page's red) or "Invalid
answer" (`INVALID`), open ("Working 3s") for 60 s and then "No answer"; the external run from the
patch's start to its finish once the decision has brought both, labelled with the commit and the
duration and carrying the reply, its times and its duration; the hatch, `waitFrom`/`waitTo` on
our run, the part of our bar the patch covers, since the laptop's clock and Kafka's can disagree;
marks for every finding of the last 2 minutes with its CVE id and whether it is critical; the
newest 5 findings with their repository, CVE, score, critical flag and `eventTime`, and "N
scanned" over 600 s; the two lanes, "Security agent" with its state and "External · LangGraph ·
A2A" with none, since the wall hears of the external call only when our agent answers;
`openCriticals` and "Agent on critical CVE"; the Issues panel in the services' keys, over the
critical findings of the last 10 minutes with an answer key: detected the right ones, missed the
wrong, `INVALID` or undecided 60 s after lab 6.1's record, finding -> decision and finding -> the
patch's finish for `PATCHED` ones), `scripts/opswall/server.py` (`/healthz`'s `serviceCount`
stays 3: it counts services, and the new row is none), docstrings in
`scripts/opswall/{app,consumer,state}.py`. A stopped patch agent's decision, `INVALID` and `N/A`
(issues 02 and 07), reads "Invalid answer", draws no external bar and counts as missed; spec
decision 24's red words show only for `FAILED`. Tests: `tests/test_opswall_state.py` (the row
last, with its keys and its empty shape; a `PATCH` gives our closed bar hatched over the
external one inside it; a `DISMISS` one bar; `FAILED` and `INVALID` their words; a stopped patch
agent; no decision, open for 60 s and then "No answer", closed late by a decision; the external
bar only once the decision has come, and only with both times; the hatch clipped to our bar; a
decision without its `critical_cves` record; a critical finding nobody routed gets no bar; the
marks; the list's order and the 10 s rule's fields; the header count and the lanes; the Issues
numbers, a `DISMISS` that called the tool anyway left out of the time to patch; the four records
in any order; written twice; trim; a run off the timeline and one clipped at its edge; the
critical line against lab 6.1; the three service rows unchanged, and the snapshot-shape tests
taking the fourth row), `tests/test_opswall_ingest.py` (a record from each finding topic through
the real Avro path, with the Flink schemas of `critical_cves` and `patch_decisions`, reaching
the row; fourteen threads, the four new ones from 600 s back, the two lab tables with their Kafka
timestamps), `tests/test_opswall_server.py` (`serviceCount` 3; the websocket's four rows and the
security row's empty shape). Until issue 10, the page draws the row as an empty chart row.

Issue 10 (the "Code security incidents" row on the page): `scripts/opswall/static/opswall.js`
(`updateRow` gains the `"security"` branch: the `security-row` class, no charts, and the remediate
block's title, "Measured from the finding's scan to the patch agent's finish, patched findings
only". The ticket row's timeline becomes `timelineHTML`, `timelineParts` and `updateChips`, each
given its title and its lanes, and `placeLabels` reads the timeline's own lanes, so checkout keeps
its look. `createSecurityBody` and `updateSecurityRow` draw: web-gateway's laptop with "git" under
it; a line with the ambient dots; "Scan findings", "N scanned" and the newest 5, each its
repository, its CVE and finding ids and "CVSS 9.8", a critical row in the accent; a new id among
them, not on the first snapshot and at most 10 s old, sends a packet along the line (in the
accent when one of them is critical, bright otherwise), rings the laptop and fades its row in, as
checkout's tickets do; "Open criticals N" in the header, N in the accent above 0; the timeline,
"Findings and agent runs", with its "Security agent" and "External · LangGraph · A2A" lanes at
checkout's lane tops and nothing on the right of its title, since the row has one agent of ours
and its chip says whether it works; a critical finding's mark, a full-height accent line with
its CVE id, the others short dashed ticks; our bars in the accent, hatched between `waitFrom`
and `waitTo` as a share of the bar, with "Patch agent unreachable" in red when `unreachable`; the
external bars dashed all round in a colour of their own; the tooltips: ours the CVE and its
repository, the finding id, score, lane and label, the action ("No PATCH or DISMISS came back."
for `INVALID`, "Waiting for the security agent." while open, "The security agent gave no
answer." after 60 s), the commit or, for `FAILED`, "No commit came back.", "Waiting on patch
agent for 6s, the hatched part." and the reason; the external one the patch's start and finish
in local time, its duration and the patch agent's reply), `scripts/opswall/static/opswall.css`
(`--external`, `#C4A7FF`, which no ticket lane uses; the laptop, "git", packet and list; the two
marks; the hatch, a repeating gradient in the lane's colour; the external bar and chip square
dashed; the row's healthy status green and "Open criticals" styled as checkout's "Open P1"; the
new row's fade stopped under `prefers-reduced-motion`). `state.py` is unchanged: the page draws
issue 09's snapshot as it is, so a stopped patch agent's `INVALID` reads "Invalid answer" in
grey. The page has no tests. A throwaway harness, issue 10's
`.scratch/code-security-incidents/research/issue10_wall_render.py`, serves the real app from the
real Simulation and stand-ins behind a clock it holds and checks 154 things in headless Chrome: five frames (a `PATCH` with its hatch
and external bar beside an open run, a `DISMISS`, a `FAILED`, an `INVALID`, a "No answer"), the
two tooltips on hover, the dots live (none on the first snapshot, accent, bright, none for a
25 s-old backread), and the three service rows byte for byte as HEAD's page draws them, at
1440 and 820 px, at three moments, two of them with checkout bars.

Issue 11 (the docs): `README.md` (the opening paragraph tells four stories with five Streaming
Agents, three lines longer: a scanner reports CVEs in River Shop's repositories, Flink keeps the
critical ones, and a security agent decides whether each matters; when one does, it calls an
external LangGraph agent on your laptop over A2A and records the commit it returns in
`patch_decisions`; coding agents reach that table and `remediation_decisions` through the
Real-Time Context Engine, and Lightning Queries read them over REST; `ticket_triage` is no longer
named, to keep the length), `platform-team/README.md` (the Real-Time Context Engine and Lightning
are sections 7–9, in steps 5 and 8; "What this does": the pool sized for ten long-running
statements, 6.1 and 6.4 among them and not yet checked against 10, seven source and seven output
tables, and RTCE on `patch_decisions`, with `remediation_decisions` still a Console step; step 6:
developers run `uv run patch-agent` and a tunnel in front of it in section 6, so their laptops
need outbound access for the tunnel, which tunnel is still a TODO, what the smoke test saw (a
cloudflared quick tunnel registered only with `--protocol http2`, on a network that blocks
outbound UDP on port 7844, which QUIC, its default transport, uses), and the presenter's patch
agent as the fallback; step 7: 22 resources per person and their list, the four tables and the
RTCE topic among them, and all fifteen statements `COMPLETED`; step 9: 22 per new person;
"Updating stacks you built earlier": a stack applied before D-070 needs nothing dropped; the
readiness row: fourteen tables; the cost paragraph: about 72 security-agent decisions an hour per
developer, about 36 of them, the ones it patches, making a second model call after the patch
agent answers, and the patch agent calls no model; the demo's 39 point at step 7's list alone; the
files table; issue 08 had already written `patch_agent_url` and the demo restart's seven
`-replace`s), `platform-team/modules/developer/statements.tf` (a comment: the agent SQL that names
`llm_textgen_model` is sections 4 to 6). No test: the issue is docs. A grep of the tracked tree,
apart from `DECISIONS.md`, `PLAN.md`, `checkpoints/` and `inspiration/`, finds no "three
stories", "four Streaming Agents", "four agents", "sections 6–8", "ten tables" or "ten topics",
nor "17 per person", "eleven statements", "five source tables", "five output tables" or "sections
4 and 5"; "eight long-running statements" stays in `flink_max_cfu`'s description, which adds
section 6's two. The repo still has no markdown link check. A one-off check of the 32 relative
links and anchors in its tracked markdown finds the same five as D-068's: `PLAN.md` (gitignored)
and the four `docs/tracks/PRE-PROVISIONED.md` links, which stay until the walkthrough is written.

Code review fixes (the review of issues 01–11 on 2026-11-26,
`.scratch/code-security-incidents/review-2026-11-26.md`, fixed in three passes). Pass 1, the
spec's findings: `scripts/opswall/state.py` (S-c1, the user's decision: our bar reads "Patch
agent unreachable", flagged `unreachable` for the page's red, for `FAILED` and now also for an
`INVALID` whose answer starts "Exception while initializing A2A client", `CARD_FAILED_ANSWER`,
read from `raw_response` into the finding's `card_failed`, since that is what a stopped patch
agent gives; it is still a miss, as every `INVALID` is; S-c3: a right decision is the expected
action within 60 s of lab 6.1's record, of the finding without one, when our bar gives up, and
one that comes later is missed however right it is, where checkout's rule counted it as
detected; the time to decide still counts it; the module docstring), `static/opswall.js` (S-a1:
the security row's Issues panel takes spec decision 26's words, "Critical findings", "Right
decisions", "Median time to decide" and "Median time to patch", set once when its body is made,
so the services' labels and `updateRow` are as they were; the comments),
`sql/checks/remediation_call_rate.sql` (S-c2: the header puts `PATCHED decisions` short of
`PATCH decisions` down to `FAILED` patches alone, a call that failed or timed out or a reply not
copied exactly, and says a stopped patch agent shows up as `INVALID` instead, with `PATCH
decisions` falling behind half of `patch_decisions rows`). This entry: "Not checked live" says
what issue 02 found and keeps what it left open (S-a2), and the Choice's wall bullet says what
is built (S-c1), as spec decisions 24 and 26 now do. Tests: `tests/test_opswall_state.py` (a
stopped patch agent's `INVALID`, on a `PATCH` and on a `DISMISS` finding, reads "Patch agent
unreachable" with `unreachable`, no commit and no hatch, and is a miss; an `INVALID` that quotes
the error after an answer of its own stays "Invalid answer"; a right decision 60 s after lab
6.1's record is right, and 60.5 s after it, 89 s after it, wrong and late, or late with no lab
6.1 record, missed, each once, with the time to decide and to patch counting the late ones),
`tests/test_agent_sql.py` (lab 6.4 writes issue 02's error as `INVALID` and `N/A`, with `SUCCESS`
or another status, and the call-rate check's header says a stopped patch agent gives `INVALID`
and blames `PATCHED` short of `PATCH` on `FAILED` alone). Offline render with issue 10's
harness, given an "unreachable" frame (issue 02's error as the whole answer) and the Issues
panel's words (`research/review-fix-1/`): 8 of 182 checks failed before the change (the labels
in all six frames, and that frame's label and tooltip) and 182 of 182 pass after it; the
stopped patch agent's bar reads "Patch agent unreachable" in red, with no external bar and "1
missed"; the labels fit at 1440 and 820 px; and the three service rows are the same PNG, byte
for byte, as HEAD's page draws them, at 1440 and 820 px, at T and at the two moments with
checkout bars. Pass 1's two loose ends, in a commit of their own: the stopped patch agent's
tooltip says "The patch agent could not be reached, so no decision came back." in place of "No
PATCH or DISMISS came back. No commit came back." (`static/opswall.js`; the harness checks the
sentence, 1 of 182 checks failing before and 182 of 182 after, `research/review-fix-1b/`, the
service rows byte for byte as before), and the spec's Pending list gives what issue 02 answered
as answered and keeps the rest. Pass 2, the Python smells, a refactor: `scripts/opswall/state.py` (M1:
a ticket's runs and a finding's share one `_Run` and one `_run`, the answered, open and "No
answer" logic, given the row's wait, `TICKET_WAIT_SECONDS` (30 s, was `RUN_WAIT_SECONDS`) or
`SECURITY_WAIT_SECONDS` (60 s), and what an answer reads; one `_run_json` for a bar's key, lane,
place and label, each row adding its ticket's or finding's fields and the lane's tooltip; one
`_lanes_json`; one `_latest` for what `trim()` drops them by; one `_fill`, with one rule: a field
a record leaves empty, None or "", waits for a record that carries it, as a ticket's did, where a
finding's kept an empty string, and a CVSS of 0.0 is still a score; M6: a `Lane`, its key, name
and whether the page says Working or Idle, for each of the four lanes, so the security row no
longer reads `SECURITY_LANES[0][1]` or writes its lanes by hand, and `PATCH` and `DISMISS` come
from `opsgen.finding_templates`; M4 and M5: our agent's lane is `SECURITY_AGENT_LANE`, apart from
the row kind `SECURITY_ROW` though the snapshot still says "security" for both, `_wait` is
`_hatch`, our bar's words come from `DECIDED_WORDS`, and `Finding` no longer keeps the scanner's
sentence, which the wall never read), `opsgen/findings.py`, `sinks.py` and `kafka_io.py` (M4:
`NewFinding.finding` is `NewFinding.scan`, the `cve_scans` record; M3: one `KafkaSink._produce`
for every record keyed by a raw string, so `emit_ticket`, `emit_answer_key`, `emit_finding` and
`emit_cve_answer_key` are gone, and `FINDINGS_TOPIC` is `CVE_SCANS_TOPIC`),
`opsgen/finding_templates.py` (M4: `FindingTemplate.finding` is `.sentence`), `scripts/reset.py`
(M4: `_drop_agents` is `_drop_lab_objects`; M7: "Dropping the 5 agents..." and `--keep-source`'s
"7 source topics" count with `len`). Declined, with the reasons in the review file: sharing
`_by_category` and `placeholders` or `public_url.rstrip("/")` (M3), one lane test left in
`_finding_run_json` and the snapshot's "security" lane key (M5, pass 3's with the JS), and the
topic plumbing and the prose counts (M7). Tests: `tests/test_opswall_state.py` (a field a ticket's
or a finding's record leaves empty is filled by a later one, which failed for the finding before;
`trim()` keeps a ticket or a finding by each of its moments), `tests/test_kafka_io_and_rtce.py`
(metrics, deployments and incidents with their keys, subjects and timestamps, and each batch's
poll). Issue 10's harness gives the same snapshot JSON, byte for byte, before and after, 153
snapshots of every row, checkout's runs and the six security frames at 16 moments and after
`trim()` (`research/review-fix-2/`), `ops-sim --dry-run --simulate 30 --seed 7` prints the same,
and 18 mutations of `state.py` are each caught.
