"""`uv run ops-sim` — the River Shop traffic and incident generator.

  uv run ops-sim                         # live: produce to Kafka, read the agents' decisions
  uv run ops-sim --dry-run               # print raw events to stdout, no Kafka
  uv run ops-sim --dry-run --simulate 60 # fast synthetic clock, window summaries
  uv run ops-sim --dry-run --seed 42     # reproducible schedule and traffic
  uv run ops-sim --no-incidents          # baseline traffic only: no incidents, no deploys
  uv run ops-sim --show-records payments # also print payments' service_metrics record every 5 s

payments gets bad deploys and upstream drops from the shared schedule (D-068); web-gateway
deploys on its own clock, and about 4 in 7 of its deploys are bad: they fail 40 % of requests
(D-067). Those two produce the metrics and deploys. checkout produces support tickets instead,
and no metrics, deploys or incidents (D-068).

Live, ops-sim also tails `remediation_decisions`: an agent's correct answer resolves its
incident on the next tick, and an incident nobody resolves expires at its 35 s cap (D-065).
Before lab 4.3 there are no decisions, so every incident expires. `--dry-run` and
`--simulate` never connect to Kafka, so there every incident expires too.

There are no timing flags: the schedule (quiet period, incident cap, gap and rests, and
web-gateway's deploy clock) lives in `Config` in opsgen/engine.py (D-060, D-067).
"""

from __future__ import annotations

import argparse
import sys
import time

from opsgen.driver import aligned_start, run_realtime, run_simulate
from opsgen.engine import Config
from opsgen.services import metric_service_names
from opsgen.simulation import Simulation
from opsgen.sinks import DryRunSink


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="River Shop AIOps traffic and incident generator")
    p.add_argument("--dry-run", action="store_true", help="Print events to stdout without connecting to Kafka")
    p.add_argument(
        "--simulate",
        type=float,
        metavar="MINUTES",
        help="With --dry-run: run a synthetic clock for MINUTES without sleeping, printing per-window summaries",
    )
    p.add_argument("--seed", type=int, help="Seed the RNG so the whole schedule and traffic are reproducible")
    p.add_argument("--no-incidents", action="store_true", help="Never inject an incident (so never deploy either)")
    p.add_argument(
        "--show-records",
        metavar="SERVICE",
        choices=metric_service_names(),
        help="Also print SERVICE's service_metrics record to stderr every 5 s, as it is sent "
        f"({', '.join(metric_service_names())}). Live and --dry-run only",
    )
    p.add_argument("--creds", help="Path to the credential card (live mode; default: read from credentials.env)")
    return p


def _config_from_args(args: argparse.Namespace) -> Config:
    return Config(no_incidents=args.no_incidents)


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    if args.simulate is not None and not args.dry_run:
        sys.exit("Error: --simulate requires --dry-run (it never connects to Kafka).")
    if args.show_records and args.simulate is not None:
        sys.exit("Error: --show-records needs real time, so it can't be used with --simulate.")

    config = _config_from_args(args)

    if args.simulate is not None:
        sim = Simulation(args.seed, start=0.0, config=config)
        run_simulate(sim, args.simulate)
        return

    if args.dry_run:
        sim = Simulation(args.seed, start=aligned_start(time.time()), config=config)
        _announce(args.show_records)
        run_realtime(sim, DryRunSink(), root=None, show_records=args.show_records)
        return

    # Live: produce to Kafka, loading the card and continuing from persisted state.
    from opsgen.decisions import DecisionFeed
    from opsgen.kafka_io import KafkaSink
    from opsgen.state import load_state, state_path
    from scripts.common.credentials import load_card
    from scripts.common.paths import get_project_root

    root = get_project_root()
    path, card = load_card(args.creds, root)
    print(f"Using credential card: {path}")

    # The saved state carries the versions and the incident counter, so a restart
    # continues both (incident ids must never repeat).
    initial = load_state(state_path(root))
    sim = Simulation(args.seed, start=aligned_start(time.time()), config=config, initial_state=initial)
    print("Producing to service_metrics, service_deployments and service_incidents. Ctrl-C to stop.")
    sink = KafkaSink(card)
    print("Reading remediation_decisions: a correct agent answer resolves its incident.")
    _announce(args.show_records)
    run_realtime(sim, sink, root=root, decisions=DecisionFeed(card), show_records=args.show_records)


def _announce(service: str | None) -> None:
    if service:
        sys.stderr.write(f"Showing {service}'s service_metrics record every 5 s, as it is sent.\n")


if __name__ == "__main__":
    main()
