# Break 5 — the dashboard (`uv run ops-wall`)

## 1. Summary

**What.** Phase 5 built `uv run ops-wall`, a local live dashboard that reads four Kafka
topics and shows the simulator's injected incidents against the pipeline's detections
side by side, with the agent's decisions as pins on the zero line. Dark theme, variant-E
layout, served on `http://localhost:8000` from FastAPI, pushed over a websocket at 4 Hz.
Also adds the new `service_incidents` topic the dashboard reads from (schema, Terraform
heredoc, generator wiring, `reset` cleanup).

**Where it lives.**
- `scripts/opswall/` — package: `app.py`, `server.py`, `state.py`, `consumer.py`,
  `mock.py`, `static/{index.html,opswall.css,opswall.js,fonts/*}`.
- `sql/source_service_incidents.sql` + `schemas/service_incidents.avsc` +
  `platform-team/modules/developer/statements.tf` — the new source table.
- `opsgen/{groundtruth,sinks,kafka_io,driver}.py` — generator emits two records per
  incident to Kafka alongside the existing `incidents.jsonl`.
- `scripts/reset.py` + `tests/test_reset.py` — `service_incidents` joins `SOURCE_TOPICS`.
- `tests/test_opswall_{state,mock,server}.py` + `opsgen/tests/test_incident_records.py`
  — 36 new tests.
- `.scratch/mockups/variant-{a,b,c,d}-*.html` — the mockups that led to variant E.
- `.scratch/phase-5-plan/PHASE-5-PLAN.md` — the plan you approved before build.

**Cumulative state per phase.**
- Phase 0: brief read, repo mapped, PLAN.md written.
- Phase 1: offline generator (`opsgen/`, CLI, deterministic tests).
- Phase 2: Kafka path, deploy/reset/destroy tooling, source tables via Terraform.
- Phase 3: `service_health` (temporal join + `ML_DETECT_ANOMALIES`), evaluate detection,
  drop detection reworked (D-031), live-verified.
- Phase 4: two agents, `remediation_decisions` (8 cols, D-034), decision scoring,
  live-verified 3/3 incidents detected, 100% valid actions, ROLLBACK target correct.
- **Phase 5**: dashboard + `service_incidents` topic, 238 tests passing.

**Commits.**

| # | Commit    | Scope |
|---|-----------|-------|
| 1 | `1ad7920` | SQL source of truth + Avro + Terraform heredoc for `service_incidents` |
| 2 | `d50fc4d` | Generator emits start/end records to Kafka |
| 3 | `8e657b3` | `reset` purges the new topic too |
| 4 | `bada477` | `OpsWallState` — thread-safe collector, span consolidation, snapshot |
| 5 | `b3cb8a7` | Mock driver — 12-min scripted scene loop |
| 6 | `361a764` | Kafka consumer + FastAPI server + `uv run ops-wall` |
| 7 | `baeb879` | Variant-E frontend wired to `/ws`, IBM Plex vendored |
| 8 | this break | DECISIONS.md D-035..D-041, this file |

**Tests and lint.** `uv run pytest` → **238 passed**. `uv run ruff check .` → clean.

---

## 2. ⚠️ Deviations and workarounds

All recorded in `DECISIONS.md` (D-035 .. D-041). The ones that move the brief:

- **D-035 — dark-only.** §7.3 asked for light and dark; the accepted design is dark-first
  and dual-theme is deferred until asked for.
- **D-036 — per-metric truth-vs-detection mirror.** Replaces §7.3's tiles + sparkline +
  separate anomaly / AI panels.
- **D-039 — 10-minute startup read instead of 15.** Matches the 10-min chart width.
- **D-041 — no progressive-unlock panels.** The chart is drawn from minute one; empty
  topics just leave empty lanes.

No code-level workarounds this phase.

---

## 3. How to test it yourself

**Already verified by me** (I ran these and saw the result):

- `uv run pytest` — 238/238 pass, ruff clean. Covers appliers + dedup, span
  consolidation with/without actionable decisions, the mock exercising every rendering
  path over one loop, `/healthz` payload + error surfacing, `/ws` connect-and-snapshot,
  schema/DDL drift, `reset` topic list.
- Mock end-to-end: `uv run ops-wall --mock --no-browser --port 18923` → `/` returns the
  HTML, `/static/opswall.css` is 8.8 KB, `/static/opswall.js` is 17 KB,
  `/static/fonts/*.woff2` serve as `wOF2`, a websocket subscription returns the full
  snapshot (six services, per-service `warmup` tuple appears within seconds).
- WOFF2 magic bytes (`77 4F 46 32`) on all three vendored font files.

**Needs you** (I did not run these — they touch your environment, your browser, or
state only you can reach):

- **Terraform apply (required before live).** The new `service_incidents` table is in
  `platform-team/` but your environment doesn't have it yet. Run it from the host that
  already holds your `.tfstate` (see §4).
- Reset + run the generator so the new topic sees traffic:
  `uv run reset --with-labs && uv run ops-sim --seed 7`.
- `uv run ops-wall` — open http://localhost:8000 in your browser. Confirm:
  - six service rows under one sticky axis, dark background;
  - the top summary line fills in once decisions arrive;
  - at least one incident scrolls in from the right as `ops-sim` injects one — purple
    bar up, cyan bar down, pin on the zero line when the agent acts;
  - the "updated" time ticks forward.
- Mock preview: `uv run ops-wall --mock` cycles the eight scenes every 12 minutes;
  confirm each pin shape appears at least once.
- Hover a bar and a pin — tooltip should appear with the precise times and lag.

---

## 4. Commands you need to run

```bash
# 1. Register service_incidents in your environment. I have never run terraform.
cd platform-team
terraform apply   # review the single new resource + the moved depends_on first

cd ..
# 2. Blank slate so topics line up (optional; --with-labs rebuilds service_health + decisions)
uv run reset --with-labs

# 3. In one terminal — the simulator (also writes two records per incident to the new topic)
uv run ops-sim --seed 7

# 4. In another terminal — the dashboard (opens http://localhost:8000)
uv run ops-wall

# Dashboard-only preview without the environment:
uv run ops-wall --mock

# Bind to all interfaces so you can view from another device on the same LAN:
uv run ops-wall --host 0.0.0.0
```

---

## 5. Decisions and assumptions

All new ones in `DECISIONS.md`:

- **D-035** DEVIATION — dark-only.
- **D-036** DECISION — per-metric truth-vs-detection mirror.
- **D-037** DECISION — new `service_incidents` topic, two records per incident, platform
  Terraform.
- **D-038** DECISION — `evaluate` keeps reading `incidents.jsonl`.
- **D-039** DECISION — 10-min startup read, not 15.
- **D-040** DECISION — vendor IBM Plex Sans WOFF2 (OFL).
- **D-041** DECISION — no separate progressive-unlock panels.

No provisional assumptions outstanding — every design branch was settled with you in
grilling Q1–Q36 before I wrote anything.

---

## 6. Questions for me

1. Terraform is the first command in §4. Review the diff in `statements.tf` before you
   apply — one new `confluent_flink_statement` resource `create_service_incidents_table`
   and one changed `depends_on` on the model so the chain stays linear. If anything there
   looks wrong, say so before you apply.
2. Mock mode uses service names `catalog` + `web-gateway` where the plan originally said
   `recommendations` + `search` (which aren't in the service catalogue). Sensible
   stand-ins, worth calling out.
3. The variant-E mockup you supplied lives in your Downloads folder. It is not committed
   — only the three files it inspired are (`static/{index.html,opswall.css,opswall.js}`).
   If you'd like the original kept under `.scratch/mockups/` for reference, say so and
   I'll copy it in.

---

## 7. Next phase

**Phase 6** is the last one — the walkthrough, README, docs site, architecture diagram,
and screenshot list. Nothing new to code; all authoring. Three carry-overs:

- **D-034 reshape** — the walkthrough must describe the three-line agent answer, not the
  brief's six.
- **D-020 — the `deploy` readiness-check bug** that reports both source tables as missing
  even though they exist. Queued for before Phase 6 starts.
- **D-035 / D-036 / D-037 / D-039 / D-041 reshapes** — the dashboard section must describe
  the dark-only per-metric mirror with the `service_incidents` ground-truth lane, not the
  brief's tiles + separate panels.

Also still carried from Break 4, not blocking Phase 6:

- `SCALE_OUT` and `ESCALATE` never exercised on live traffic — the mock covers them, but
  a real surge during a session with the agents running would be the first live proof.
- Fresh-start noise on `payments` (D-030/D-031-adjacent).
- RTCE in the Console, the Lightning `curl`.

**What I need from you to start Phase 6.** Only explicit go-ahead; Phase 6 does not need
Terraform or any live command.
