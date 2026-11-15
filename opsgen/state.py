"""Persist the generator's state so a restart continues from the current versions
and incident numbering (brief section 4.5, D-046). Replicas are fixed per service and
come from `opsgen/services.py`, so only versions / deployed_at / last_change are persisted
per service, plus the last incident number used:

    {"services": {"checkout": {"version": ..., ...}, ...}, "incident_counter": 7}
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
