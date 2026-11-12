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


def _epoch_millis(seconds: float) -> int:
    return int(seconds * 1000)


def start_record(inc: Incident, now: float) -> dict:
    """The first record for an incident, written when degradation starts. ended_at is
    null — a consumer reading it knows "something has started, don't know when it ends".
    The second record (end_record) carries the close. Field names and types match
    schemas/service_incidents.avsc and sql/source_service_incidents.sql."""
    return {
        "event_time": _epoch_millis(now),
        "incident_id": inc.incident_id,
        "service_name": inc.service_name,
        "incident_type": inc.type,
        "started_at": _epoch_millis(inc.degradation_start),
        "ended_at": None,
        "expected_trigger": inc.expected_trigger,
        "expected_action": inc.expected_action,
        "good_version": inc.good_version if inc.type == BAD_DEPLOY else None,
    }


def end_record(inc: Incident, now: float) -> dict:
    """The second record for an incident, written when degradation ends. Carries both
    timestamps so a consumer that missed the start record can still render the span."""
    return {
        "event_time": _epoch_millis(now),
        "incident_id": inc.incident_id,
        "service_name": inc.service_name,
        "incident_type": inc.type,
        "started_at": _epoch_millis(inc.degradation_start),
        "ended_at": _epoch_millis(inc.degradation_end),
        "expected_trigger": inc.expected_trigger,
        "expected_action": inc.expected_action,
        "good_version": inc.good_version if inc.type == BAD_DEPLOY else None,
    }


def last_incident_number(path: Path) -> int:
    """The highest `INC<n>` number already logged, or 0. A restarted generator continues
    numbering after it, so incident ids stay unique within one ground-truth file."""
    path = Path(path)
    if not path.is_file():
        return 0
    highest = 0
    for line in path.read_text().splitlines():
        try:
            iid = json.loads(line).get("incident_id", "")
        except ValueError:
            continue
        if isinstance(iid, str) and iid.startswith("INC") and iid[3:].isdigit():
            highest = max(highest, int(iid[3:]))
    return highest


def append_incident(path: Path, inc: Incident) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(incident_record(inc)) + "\n")


def clear(path: Path) -> None:
    Path(path).unlink(missing_ok=True)
