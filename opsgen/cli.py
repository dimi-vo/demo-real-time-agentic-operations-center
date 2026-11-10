"""`uv run ops-sim` — the River Shop traffic and incident generator.

  uv run ops-sim                         # live: produce to Kafka (Phase 2)
  uv run ops-sim --dry-run               # print raw events to stdout, no Kafka
  uv run ops-sim --dry-run --simulate 60 # fast synthetic clock, window summaries
  uv run ops-sim --dry-run --seed 42     # reproducible schedule and traffic
  uv run ops-sim --no-incidents          # benign deploys only

The scheduling overrides (quiet period, spacing, duration, cooldown, benign cadence)
are in seconds and exist to make the workshop quick to develop against.
"""

from __future__ import annotations

import argparse
import sys
import time

from opsgen.driver import run_realtime, run_simulate
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
    p.add_argument("--no-incidents", action="store_true", help="Benign deploys only; never inject an incident")
    p.add_argument("--creds", help="Path to the credential card (live mode; default: read from credentials.env)")

    g = p.add_argument_group("schedule overrides (seconds)")
    g.add_argument("--quiet-period", type=float, default=Config.quiet_period, help="Incident-free period at start")
    g.add_argument("--spacing-min", type=float, default=Config.spacing_min, help="Min gap after an incident ends")
    g.add_argument("--spacing-max", type=float, default=Config.spacing_max, help="Max gap after an incident ends")
    g.add_argument("--duration-min", type=float, default=Config.duration_min, help="Min incident degradation duration")
    g.add_argument("--duration-max", type=float, default=Config.duration_max, help="Max incident degradation duration")
    g.add_argument("--cooldown", type=float, default=Config.cooldown, help="Min gap between incidents on one service")
    g.add_argument("--benign-first", type=float, default=Config.benign_first, help="When the first benign deploy fires")
    g.add_argument("--benign-min", type=float, default=Config.benign_min, help="Min gap between benign deploys")
    g.add_argument("--benign-max", type=float, default=Config.benign_max, help="Max gap between benign deploys")
    return p


def _config_from_args(args: argparse.Namespace) -> Config:
    return Config(
        quiet_period=args.quiet_period,
        spacing_min=args.spacing_min,
        spacing_max=args.spacing_max,
        duration_min=args.duration_min,
        duration_max=args.duration_max,
        cooldown=args.cooldown,
        benign_first=args.benign_first,
        benign_min=args.benign_min,
        benign_max=args.benign_max,
        no_incidents=args.no_incidents,
    )


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
        sim = Simulation(args.seed, start=time.time(), config=config)
        run_realtime(sim, DryRunSink(), root=None, write_groundtruth=False)
        return

    # Live: produce to Kafka, loading the card and continuing from persisted state.
    from opsgen.kafka_io import KafkaSink
    from opsgen.state import load_state, state_path
    from scripts.common.credentials import load_card
    from scripts.common.paths import get_project_root

    root = get_project_root()
    path, card = load_card(args.creds, root)
    print(f"Using credential card: {path}")
    initial = load_state(state_path(root))
    sim = Simulation(args.seed, start=time.time(), config=config, initial_state=initial)
    print("Producing to service_requests and service_deployments. Ctrl-C to stop.")
    run_realtime(sim, KafkaSink(card), root=root, write_groundtruth=True)


if __name__ == "__main__":
    main()
