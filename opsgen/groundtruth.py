"""Machine-readable incident ground truth (brief section 4.5), one JSON object per
line in runs/pre-provisioned/incidents.jsonl. `uv run evaluate` and the dashboard
overlay read this to compare "what happened" with "what was detected and recommended."

Written only when the generator runs on a real (wall-clock) timeline — the live
Kafka path and `--dry-run` without `--simulate`. `--simulate` uses a synthetic clock
and prints its own log instead, so it never pollutes this file.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from opsgen.engine import BAD_DEPLOY, Incident

INCIDENTS_FILENAME = "incidents.jsonl"


def incidents_path(root: Path) -> Path:
    return Path(root) / "runs" / "pre-provisioned" / INCIDENTS_FILENAME


def _iso(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def incident_record(inc: Incident) -> dict:
    rec = {
        "incident_id": inc.incident_id,
        "type": inc.type,
        "service_name": inc.service_name,
        "started_at": _iso(inc.degradation_start),
        "ended_at": _iso(inc.degradation_end),
        "expected_trigger": inc.expected_trigger,
        "expected_action": inc.expected_action,
    }
    if inc.type == BAD_DEPLOY:
        rec["bad_version"] = inc.bad_version
        rec["good_version"] = inc.good_version
    return rec


def append_incident(path: Path, inc: Incident) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(incident_record(inc)) + "\n")


def clear(path: Path) -> None:
    Path(path).unlink(missing_ok=True)
