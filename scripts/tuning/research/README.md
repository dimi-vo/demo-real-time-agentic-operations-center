# Tuning research harnesses

Offline scripts for tuning two things together: the **data** the simulator produces (traffic
means, spike sizes and lengths, rests between incidents) and **our reference ARIMA detector**
(the `ML_DETECT_ANOMALIES` settings in labs 2.3 and 2.4). They answer questions like "does
confidence 99.95 still catch every drop at 60 incidents an hour?" in seconds to minutes,
without a live replay. They are the evidence behind D-059 (data and detector settings) and
D-064 (window size).

Developer tools only: not part of the workshop, and not run by `uv run pytest`.

## Building blocks (`scripts/tuning/`, tested in `tests/test_tuning.py`)

| Module | What it does |
|---|---|
| `arima.py` | Dependency-free replica of `ML_DETECT_ANOMALIES` for AR(p): one model per series, warm-up, refit interval, symmetric bounds `forecast ± z·sigma`, whole-number rounding for traffic. |
| `calibrate.py` | Checks the replica against what Confluent actually computed, using a recorded `service_health` dump. |
| `sweep.py` | Runs the **real generator** (`opsgen.Simulation`) at chosen rates and multipliers, through the replica, scored with `evaluate`'s code. |

```bash
uv run python -m scripts.tuning.sweep --seeds 4 --hours 3 [--detectors]
# Live refits on every row (D-031); calibrate's --update-interval defaults to 5.
uv run python -m scripts.tuning.calibrate --rows health.jsonl --confidence 99.95 --update-interval 1
```

The replica matches Confluent within a few percent on quiet series. After a big jump it is
conservative: its bounds can be up to ~1.5× wider than Confluent's for a while, so it
under-predicts false alarms just after incidents (D-031). It can't model `d`, `q` or STL.
Always confirm a winner live.

## The scripts in this folder

Run every script from the repo root: `uv run python -m scripts.tuning.research.<name> ...`.
Output goes to stdout; redirect it to keep it (for example `> .scratch/<feature>/t_grid.out`).
Runs are seeded, so the same arguments print the same numbers. The scripts use every CPU core.

| Script | Question | Data |
|---|---|---|
| `tune.py` | Which data settings × detector settings catch every incident at ~60/h with the fewest false alarms? | `sampler.py` (fast, any setting) |
| `tune_diag.py` | Where do one setup's false alarms come from? | `sampler.py` |
| `window_compare.py` | What changes with 5 s windows instead of 10 s? | real generator (`sweep.py`) |
| `window_echoes.py` | Which false alarms do 5 s windows add, and do shorter spikes remove them? | real generator |
| `window_phi.py` | Which AR(1) coefficient does the detector fit at each window size? | real generator |
| `live_recovery.py` | Live vs replica: how much does the band widen after an incident, and how fast does it recover? | a live `service_health` dump |
| `live_surge_end.py` | Live vs replica, window by window, around the end of each incident | a live `service_health` dump |
| `shared_scheduler.py` | With web-gateway out of the shared scheduler, which candidates keep every incident caught on checkout and payments? (D-067) | real generator |
| `sampler.py` | (library) One 10-s window of the per-second `service_metrics` model | - |

### `tune.py`: data × detector settings

What it simulates:

- **Data**: three services (median latency 35/140/220 ms). Each 10-s window comes from
  `sampler.window()`: the SUM of 10 per-second Poisson traffic counts and the plain AVG of 10
  per-second latencies, as Flink aggregates `service_metrics` (D-058). It doesn't run
  `opsgen.Simulation`, so any mean or spike size can be tried. D-059 checked that it agrees with
  the generator: means within 0.1%, spreads within 3%.
- **Schedule**: the issue-06 rules (D-060). There is one schedule per service, and incidents
  may overlap across services. A service waits 20 s between its incidents, each type has its
  own rest, the first three incidents are one of each type, and the mix is 50/25/25. Nothing
  happens during a quiet period that is tied to `minTrainingSize`.
- **Detector**: the replica's AR(p), refitted every `upd` rows. It uses a running-sums fast
  path, which `selftest` checks against `scripts.tuning.arima.detect`.
- **Scoring**: `scripts.evaluation.detection.score`, the same code as `uv run evaluate`. An
  incident is detected when its trigger is flagged within 3 windows.

A setup is a `C(...)` value (fields at the top of `tune.py`):

| Field | Meaning | Default |
|---|---|---|
| `means` | traffic means, requests/s (web-gateway, checkout, payments) | `(50, 30, 30)` |
| `surge`, `drop` | traffic multipliers during a surge / an upstream drop | `5.0`, `0.1` |
| `bd` | latency multiplier during a bad deploy | `4.0` |
| `dur` | spike length, s (whole 10-s windows) | `20` |
| `conf`, `p`, `min_t`, `max_t`, `upd` | `confidencePercentage`, AR order, `minTrainingSize`, `maxTrainingSize`, refit interval (rows) | `99.9`, `1`, `12`, `360`, `1` |
| `gap` | seconds between two incidents on one service | `20` |
| `sched_min_t` | the `minTrainingSize` the quiet period follows, if not `min_t` | `None` |

The defaults are issue 05's **starting point** (confidence 99.9). The settings the simulator
and labs use now (D-059) are `c99.95`. Pass a setup either by name, from `CONFIGS` (`start`,
`c99.5`, `c99.95`, `c99.99`, `d30`, `m25`, `m100`, `small`, `big`, `max180`, `min6`, `ar2`,
`upd5`, ...), or as an expression: `"C(conf=99.95, dur=30)"`. Rests are written
`BAD_DEPLOY/TRAFFIC_SURGE/UPSTREAM_DROP` in seconds; the simulator's are `390/300/300`.

```bash
# The fast detector agrees with the replica (run after touching either).
uv run python -m scripts.tuning.research.tune selftest

# For each setup: the smallest rest per type with >= 99.5% recall (rest grid
# 0/60/120/180/240/300/420/600/900/1200 s), one grid step of margin above it, validated on
# fresh seeds, then the BAD_DEPLOY rest raised until ~60 incidents/h.
# --mix also searches all three rests for ~60/h with the mix closest to 50/25/25.
uv run python -m scripts.tuning.research.tune search SEEDS VSEEDS MINUTES NAME... [--mix]
uv run python -m scripts.tuning.research.tune search 20 20 120 c99.95 "C(conf=99.95, dur=30)"

# One setup at given rests, on fresh seeds.
uv run python -m scripts.tuning.research.tune validate c99.95 390/300/300 50 180

# Same data and schedule, ten detector settings (looser/stricter confidence, Confluent's
# defaults, min 6, max 60, AR(2), AR(3)). The quiet period stays at ours. This is the check
# that the attendee competition has something to separate.
uv run python -m scripts.tuning.research.tune compete c99.95 390/300/300 20 120

# False alarms per service-hour on data with no incidents (30 seeds x 3 h per service).
uv run python -m scripts.tuning.research.tune quiet start c99.95 c99.99
```

D-059's tables used 200 seeds × 120 minutes for `search` and 400 seeds × 180 minutes for the
final `validate`. Start with a few seeds and scale up for the setups that matter.

How to read a result line:

```
c99.95 [conf=99.95] steady 59.2/h rests 390/300/300: 60.0 inc/h | mix 45/18/37% |
  BD 100.00 (0/161) SURGE 100.00 (0/66) DROP 100.00 (0/132) | late 0 |
  FP/svc-h noise 0.000 echo 0.000 | fresh-start/svc 0.944 | w1:100.0
```

- `steady 59.2/h`: incidents per hour from the schedule alone, after the first 30 minutes.
  `60.0 inc/h` is the rate over the scored runs.
- `mix`: share of bad deploys / surges / drops.
- `BD 100.00 (0/161)`: recall per type in %, then (missed / total).
- `late`: incidents flagged, but after the 3-window limit.
- `FP/svc-h noise`, `echo`: false alarms per service-hour. Noise is a flag away from any
  incident. An echo is the opposite traffic flag right after a spike (`TRAFFIC_LOW` after a
  surge, `TRAFFIC_HIGH` after a drop).
- `fresh-start/svc`: false alarms per service in the first minutes after a start. These are
  mostly the first surge's and the first drop's echoes (D-059).
- `w1:100.0`: % of incidents first flagged in window 1, 2, ... (`wmiss` = never flagged).

Once a setup is chosen, carry it into the code: means in `opsgen/services.py`, spike sizes,
`max_duration` and rests in `opsgen/engine.py`, detector settings in `sql/2.3_*.sql` and
`sql/2.4_service_health.sql`. Then confirm it on the real
generator with `scripts.tuning.sweep` and live with `evaluate`. `tune.py`'s service constants
(`MEDIAN`, `TYPES_OF`, `WEIGHTS`) and `sampler.py` mirror the simulator: D-059/D-060, with
D-067's candidates (checkout gets bad deploys and surges, payments bad deploys and drops;
web-gateway is not scored, it is detected by an error-rate threshold). Update them if the simulator's model changes.

### `tune_diag.py`: where false alarms come from

```bash
uv run python -m scripts.tuning.research.tune_diag NAME RESTS SEEDS MINUTES
uv run python -m scripts.tuning.research.tune_diag c99.95 390/300/300 50 120
```

Groups the fresh-start and in-run false alarms by service, signal, the type of the last
incident before them, the seconds since it ended, and (fresh start) the seconds since the
start.

### `window_*.py`: window size

These run the real generator through `sweep.py`, with the window length patched per worker:
`sweep.WINDOW_MS` and `detection.WINDOW_SECONDS`. To try other sizes, edit `VARIANTS`,
`SEEDS` and `HOURS` in `window_compare.py`, and the lists in `window_echoes.py` and
`window_phi.py`. Each takes no arguments.

```bash
uv run python -m scripts.tuning.research.window_compare  # per variant: first flagged window, FP/echo rate, forecast error, band width
uv run python -m scripts.tuning.research.window_echoes   # what the extra false alarms are, with 20 s and 10 s spikes
uv run python -m scripts.tuning.research.window_phi      # the fitted AR(1) coefficient per series and window size
```

D-064's numbers (6 seeds × 3 h): both sizes catch every incident in its first window. At 5 s,
a 20 s spike spans 4 windows and the fitted coefficient rises from ~0.47 to ~0.73. Every
traffic spike then echoes: 0 → 7.85 false alarms per service-hour. 10 s spikes bring that
down to 2.19. The offline harness doesn't model the watermark delay. D-064 has that part of
the decision.

### `live_*.py`: live detector vs replica

These need a `service_health` dump from a live run. `evaluate` writes one:

```bash
uv run evaluate --minutes 60 --dump health.jsonl
uv run python -m scripts.tuning.research.live_recovery health.jsonl [MIN_TRAINING] [CONFIDENCE]
uv run python -m scripts.tuning.research.live_surge_end health.jsonl [MIN_TRAINING] [CONFIDENCE]
```

Pass the `minTrainingSize` and `confidencePercentage` the live statement ran with (default
12 and 99.95, the labs' values). Both scripts find incidents from the data itself (runs of
windows far from the rolling median), so no incidents dump is needed.

- `live_recovery.py`: for each incident, the band half-width after it, relative to the 30
  windows before it, live vs replica, then medians by distance from the incident's end. This
  shows how far the replica under-predicts after-incident false alarms (D-031).
- `live_surge_end.py`: the window before each incident, its windows and the four after it.
  Each line shows the live status, forecast and bounds next to the replica's, for traffic and
  latency. Use it to read an echo window by window.
