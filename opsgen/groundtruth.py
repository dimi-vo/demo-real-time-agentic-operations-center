"""Incident ground truth (brief section 4.5): the two records the generator writes to the
`service_incidents` topic for every incident. It is the only answer key (D-046): ops-wall
draws it, and `uv run evaluate` scores against it.

Field names and types match schemas/service_incidents.avsc and
sql/source_service_incidents.sql.
"""

from __future__ import annotations

from opsgen.engine import Incident


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
    }
