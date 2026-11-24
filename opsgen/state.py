"""Persist the generator's state so a restart continues from the current versions
and incident numbering (brief section 4.5, D-046). Replicas are fixed per service and
come from `opsgen/services.py`, so only versions (current, previous, and the highest ever
run, so a deploy never reuses a number: D-067) / deployed_at / last_change are persisted
per metric service, plus the last incident number used and the next ticket number of each
ticket source (D-068), so neither an incident id nor a ticket id is ever reused:

    {"services": {"payments": {"version": ..., ...}, ...}, "incident_counter": 7,
     "next_ticket_numbers": {"ZENDESK": 48231, "SERVICENOW": 41889}}

A file saved before D-068 also has checkout, which no longer deploys; `Simulation` ignores
that entry. It has no ticket numbers either, so tickets start at the defaults.
"""

from __future__ import annotations

import json
from pathlib import Path

STATE_FILENAME = "generator_state.json"


def run_dir(root: Path) -> Path:
    return Path(root) / "runs" / "pre-provisioned"


def state_path(root: Path) -> Path:
    return run_dir(root) / STATE_FILENAME


def load_state(path: Path) -> dict | None:
    """The persisted state, or None if there is none / it's unreadable."""
    path = Path(path)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def save_state(path: Path, snapshot: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(snapshot, indent=2, sort_keys=True))
    tmp.replace(path)


def clear_state(path: Path) -> None:
    Path(path).unlink(missing_ok=True)
