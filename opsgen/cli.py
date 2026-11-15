"""`uv run ops-sim` — the River Shop traffic and incident generator.

  uv run ops-sim                         # live: produce to Kafka (Phase 2)
  uv run ops-sim --dry-run               # print raw events to stdout, no Kafka
  uv run ops-sim --dry-run --simulate 60 # fast synthetic clock, window summaries
  uv run ops-sim --dry-run --seed 42     # reproducible schedule and traffic
  uv run ops-sim --no-incidents          # baseline traffic only: no incidents, no deploys

There are no timing flags: the schedule (quiet period, spike length, gap and rests) lives
in `Config` in opsgen/engine.py (D-060).
"""

from __future__ import annotations

import argparse
import sys
import time

from opsgen.driver import aligned_start, run_realtime, run_simulate
from opsgen.engine import Config
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
    p.add_argument("--creds", help="Path to the credential card (live mode; default: read from credentials.env)")
    return p


def _config_from_args(args: argparse.Namespace) -> Config:
    return Config(no_incidents=args.no_incidents)


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    if args.simulate is not None and not args.dry_run:
        sys.exit("Error: --simulate requires --dry-run (it never connects to Kafka).")

    config = _config_from_args(args)

    if args.simulate is not None:
        sim = Simulation(args.seed, start=0.0, config=config)
        run_simulate(sim, args.simulate)
        return

    if args.dry_run:
        sim = Simulation(args.seed, start=aligned_start(time.time()), config=config)
        run_realtime(sim, DryRunSink(), root=None)
        return

    # Live: produce to Kafka, loading the card and continuing from persisted state.
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
    run_realtime(sim, KafkaSink(card), root=root)


if __name__ == "__main__":
    main()
