"""--seed makes the whole schedule and traffic reproducible (section 4.5)."""

import io

from opsgen.driver import run_simulate
from opsgen.engine import Config
from opsgen.simulation import Simulation


def _tick_run(seed, ticks=300):
    sim = Simulation(seed, start=0.0)
    out = []
    for k in range(ticks):
        reqs, deps, events = sim.tick(float(k))
        out.append((reqs, deps, [(e.kind, e.service_name, e.new_version) for e in events]))
    return out


def test_same_seed_same_ticks():
    assert _tick_run(42) == _tick_run(42)


def test_different_seed_differs():
    assert _tick_run(42) != _tick_run(43)


def _simulate_output(seed, minutes=20):
    sim = Simulation(seed, start=0.0, config=Config())
    buf = io.StringIO()
    run_simulate(sim, minutes, out=buf)
    return buf.getvalue()


def test_simulate_output_reproducible():
    assert _simulate_output(42) == _simulate_output(42)


def test_simulate_output_seed_sensitive():
    assert _simulate_output(42) != _simulate_output(7)
