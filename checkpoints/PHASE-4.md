# Break 4: agents and serving layer

## 1. Summary

Built the agent layer on top of `service_health`, and the decision half of
`uv run evaluate`. **Nothing in this phase has run in your environment yet** — section 3
is the run order.

- **`sql/4.2_release_agent.sql`, `sql/4.2_capacity_agent.sql`**: the two `CREATE AGENT`
  statements. Each prompt carries the three-line output format, the decision framework
  and one worked example per allowed action. Release: `ROLLBACK`, `ESCALATE`, `OBSERVE`.
  Capacity: `SCALE_OUT`, `INVESTIGATE_UPSTREAM`, `ESCALATE`, `OBSERVE`.
- **`sql/4.3_remediation_decisions.sql`**: the fan-in table, created explicitly because
  two INSERTs write to it. **8 columns** (D-034): `window_end`, `service_name`,
  `agent_name`, `trigger`, `action`, `target`, `reason`, `raw_response`. Everything the
  agent was told stays in `service_health` and is joined on
  `(service_name, window_end)`.
- **`sql/4.3_insert_release_agent.sql`, `sql/4.3_insert_capacity_agent.sql`**: the two
  long-running INSERTs. The anomaly filter sits in a derived table *below* the `LATERAL`
  join, so the model is called once per anomalous row, not once per window. Both read
  `service_health` from the latest offset, so starting them replays nothing.
- **Routing** (mutually exclusive): traffic anomalies go to the capacity agent; latency
  anomalies go to the release agent only when traffic is normal.
- **`suggested_replicas`** is computed in SQL and handed to the agent, which only decides
  whether to use it. The scorer recomputes it to check the agent copied it.
- **`uv run evaluate`, decision half** (D-032): valid-action share, routing and mutual
  exclusivity, coverage (agent calls against anomalous rows), and per incident the
  recommended action against the expected one. Targets are checked both ways: a
  `ROLLBACK` must name the incident's good version, a `SCALE_OUT` the replica count the
  SQL computed, and any other action must name none.
- **`sql/checks/remediation_routing.sql`, `remediation_call_rate.sql`**: the two
  verifications the brief asks for as queries.
- **`uv run setup-rtce --lightning remediation_decisions`**: the walkthrough's Lightning
  example (most recent decision for a trigger, with its reasoning), next to the existing
  `service_health` one.
- **Tests:** 193 pass, ruff clean.

**Cumulative state.**
- **Phase 0:** plan and decisions log.
- **Phase 1:** source-table DDL as the single source of truth, plus the offline generator.
- **Phase 2a:** platform-team Terraform module.
- **Phase 2b:** developer tooling and the generator's Kafka path.
- **Phase 3:** the detection pipeline to `service_health`, tuned live, and the detection
  half of `evaluate`. The drop-detection fix (D-031) is implemented but still unconfirmed
  live.
- **Phase 4:** the agents, `remediation_decisions`, and the decision half of `evaluate`.

Commit: this commit.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-034 (new): `remediation_decisions` is 8 columns, not the brief's 20, and
  the agent answers in 3 lines, not 6.** Decided with you in the Break 4 design review.
  The context columns are gone (both readers already hold `service_health`), `Confidence`
  is gone, the two typed target columns became one free-text `target`, `WATCH` became
  `OBSERVE`, and the five actions are now one shared vocabulary. Each INSERT is about
  half the size. *Impact:* the §11 acceptance checks still hold (the enum is still
  validated), §9's closing-the-loop contract keeps a stable schema and an explicit but
  untyped target, and the Phase 6 walkthrough must document the three-line format.
- **DEVIATION D-033 (new): the agent SQL follows the current docs, not the F1 reference,
  on three points.**
  - *Agent name*: `AI_RUN_AGENT('release_agent', …)` as a **string**. The reference passes
    a backticked identifier and works live, but the docs type the argument as `STRING`
    and quote it in all 16 examples.
  - *Failure handling*: both agents set `'handle_exception' = 'continue'` (D-007), so one
    failed model call can't end a statement that runs all day. `max_consecutive_failures`
    is set explicitly to 3 because two doc pages give different defaults (1 and 3), and
    `request_timeout` to 60 s instead of the default 600.
  - *`max_iterations` is not set*: it bounds tool-calling, and these agents have no tools.
  - *Impact*: if the string form is rejected, switch to backticks — one edit in each
    INSERT, and the test that pins it.
  - D-034 undid three of the prompt-level extras recorded here: no `request_id`, no
    `TRY_CAST`, and targets are stored exactly as the agent wrote them.
- **DEVIATION D-031 (open from Break 3): drop services at 6 requests/s and an aligned
  generator start.** Chosen offline; still needs the live confirmation run. It can run
  at the same time as this phase, since the agents read `service_health` either way.
- **Open issue from Break 2b:** the `deploy` readiness check reports both source tables
  missing although they exist (D-020). Fix before Phase 6.

No workarounds in code.

## 3. How to test it yourself

**Verified by me (offline only):**
- `uv run pytest`: 193 passed. `uv run ruff check .`: clean.
- `tests/test_agent_sql.py` pins the contract the four SQL files and the scorer share:
  the three output lines in order (and that the dropped ones are really gone), one
  example per allowed action, the allowed actions equal to the scorer's set and no
  acceptance of the other agent's actions, no apostrophe inside a prompt literal, the
  filter below the `LATERAL` join, the latest-offset hint, `INVALID` on failure, mutually
  exclusive routing, and the 8 table columns in order against both positional INSERTs.
- `tests/test_evaluate_decisions.py`: the scoring rules of D-032.
- The decision half of `evaluate` rendered end to end against a synthetic run
  (a matched incident, a mismatch, a failed call and a decision outside every incident).

**Verified live in your environment on 2026-11-12** (fresh `reset` at 09:31; scored
08:47-09:47, so 18 minutes of data and 3 incidents). I ran `uv run evaluate` read-only;
the rows are dumped under `.scratch/eval/*-phase4.jsonl` for re-scoring.

| check (brief section 11) | result |
|---|---|
| Valid actions | **100%** (8 of 8), target ≥ 95% |
| Routing mutually exclusive | **yes**, 0 windows under both agents |
| Agent calls track anomalies | **8/8** anomalous rows decided, against 252 rows in the same span |
| Action matches ground truth | **2/2** incidents |
| Target | `ROLLBACK` named **2.9.4**, the incident's `good_version` |
| Detection | **3/3** incidents within 3 windows, **0** false positives |
| Fresh-start noise | `payments` **2** in 5 minutes (target ≈ 1); the other five services 0 |

- **Decisions seen:** `INVESTIGATE_UPSTREAM` ×3 for the checkout drop, `ROLLBACK` ×5 for
  the payments bad deploy. Every reason quoted the real numbers, none invented.
- **D-033 confirmed**: the quoted agent name and the three `WITH` options were accepted.
- **D-031 confirmed**: the checkout drop was detected in window 2 — the case that failed
  0/2 at the Break 3 rates.
- **INC0001 (a surge on `auth`) produced no decision**: it fired six minutes before the
  agents were created, and the INSERTs read from the latest offset. `evaluate` now counts
  coverage from the first decision, so this is not reported as a missing decision.

**Still not exercised — needs a traffic surge while the agents run:**
- **`SCALE_OUT`**, which is the path with the most machinery: the SQL replica
  arithmetic, the agent copying that number, and the target check against it.
- **`ESCALATE`** on `auth` (it runs at its replica ceiling).
Surges are a quarter of incidents, so leaving everything running covers this; the
remaining steps below are unchanged.

**Not verified — needs you:** RTCE in the Console, and the Lightning `curl`.

**Run order.** `ops-sim` keeps running throughout; `service_health` must already exist.

1. **The two agents** (instant, no cost until they are called):
   - `sql/4.2_release_agent.sql`
   - `sql/4.2_capacity_agent.sql`
   Then `SHOW AGENTS;` should list both, and
   `SHOW CREATE AGENT \`release_agent\`;` should show the options back.
2. **The table** (instant): `sql/4.3_remediation_decisions.sql`.
3. **The two INSERTs** (long-running, one LLM call per anomalous window — this is where
   the money goes):
   - `sql/4.3_insert_release_agent.sql`
   - `sql/4.3_insert_capacity_agent.sql`
   Both should reach `RUNNING` and then sit idle until the next incident.
4. **Watch one incident.** `ops-sim` announces `INCIDENT START`. Within about 30 seconds:
   ```sql
   SELECT window_end, service_name, agent_name, `trigger`, `action`, target, reason
   FROM `remediation_decisions` ORDER BY window_end DESC LIMIT 10;
   ```
   Expect `ROLLBACK` with the previous version for a bad deploy, `SCALE_OUT` with a
   replica count for a surge on a service below its ceiling, `ESCALATE` for a surge on
   `auth` (it runs at its ceiling), and `INVESTIGATE_UPSTREAM` for a drop.
5. **The two checks**, once a few incidents have passed:
   - `sql/checks/remediation_routing.sql` — expect **zero rows**.
   - `sql/checks/remediation_call_rate.sql` — expect decisions to track anomalous rows,
     not health rows.
6. **Tell me** and I'll run `uv run evaluate` read-only. It needs roughly an hour of data
   and at least three incidents of different types to be meaningful.

**What failure looks like:**
- the INSERT fails immediately: usually the `AI_RUN_AGENT` argument form (D-033) or a
  column-order mismatch — send me the exception text;
- `action = 'INVALID'` on every row: the model isn't answering, or it isn't following the
  output format — `raw_response` shows which;
- decisions appear for ordinary windows: the filter was pushed above the `LATERAL` join,
  which would make every window an LLM call. Stop the statements and tell me.

**Cost shape.** Roughly one model call per anomalous window: about 5–8 per incident
(AR(1) keeps an incident flagged for most of its duration), plus about 0.6 per
service-hour of false positives. At one incident every 4–6 minutes that's on the order of
60–100 calls an hour, not thousands.

### RTCE, in the Console (section 5 of the walkthrough)

RTCE is enabled per topic in the Console; there is no CLI or Terraform path.

1. Open **[Topics](https://confluent.cloud/go/topics)** in your environment's cluster.
2. Open the topic **`service_health`** → the **Real-Time Context Engine** panel →
   **Enable** (the button may read **Turn on**).
3. Repeat for **`service_deployments`** and **`remediation_decisions`**.
4. The **Topics** list then shows Real-Time Context Engine **On** for those three.
5. `uv run setup-rtce`, pick your coding agent, restart it, and ask:
   - "What topics do I have access to in the Real-Time Context Engine?"
   - "Which services are unhealthy right now?"
   - "Which version of checkout is running and when was it deployed?"
   - "What did the agents recommend in the last 15 minutes?"
6. Lightning Queries: `uv run setup-rtce --lightning` prints a ready-to-run `curl` for
   `service_health`, and `uv run setup-rtce --lightning remediation_decisions` the
   walkthrough's one (most recent `LATENCY_HIGH` decision with its reasoning). Both need
   RTCE enabled on the topic first. The direct Lightning API is still EAP (D-008), so a
   `api_not_enabled` response is a known outcome, not a bug.

## 4. Commands you need to run

No Terraform and no new infrastructure: `llm_textgen_model` already exists from Phase 2.
The statements in section 3 are all that's needed.

## 5. Decisions and assumptions

**Needs your confirmation first:**
- **D-033**, the three deviations from the reference, especially the quoted agent name.
  Confirmed the moment the INSERTs run.

**Recorded this phase:**
- **D-007:** confirmed against the docs. `handle_exception` exists, takes `CONTINUE` or
  `FAIL`, defaults to `FAIL`, and belongs in the `WITH` clause of `CREATE AGENT`. It is
  missing from the `CREATE AGENT` reference page and documented in the Agent Runtime
  Guide.
- **D-032:** how `evaluate` scores decisions. One number per incident, taken from the
  decision on its first anomalous window.
- **D-033:** the agent SQL deviations, plus the prompt-level choices the brief delegates
  (no apostrophes, `COALESCE` on nullable context).
- **D-034:** the simplified table, answer format and action vocabulary, from the Break 4
  design review.

## 6. Questions for me

1. **Agent prompts.** The brief delegates the wording to me. Read
   `sql/4.2_release_agent.sql` and `sql/4.2_capacity_agent.sql` and tell me if the
   persona, the framework or the examples should change — they are the most
   attendee-visible text in the workshop. Two things I decided as delegated wording:
   `Reason` is capped at two sentences, and "never `OBSERVE` when the error rate has
   breached" stays in both frameworks.
2. **D-031's live confirmation.** Answered by the same run: confirmed.

## 7. Next phase

**Phase 5: the dashboard (`uv run ops-wall`, localhost:8000).**
- A tile per service: status, version and time since deploy, replicas, request rate,
  error rate, and a latency sparkline with the expected band.
- An anomaly panel that unlocks when `service_health` has rows, and an AI remediation
  panel that unlocks when `remediation_decisions` does.
- An incident timeline overlaid with `incidents.jsonl`.
- Light and dark mode.

What I need from you: the results of section 3, and your answers to section 6. The
dashboard reads the same three topics read-only, so it needs nothing new.
