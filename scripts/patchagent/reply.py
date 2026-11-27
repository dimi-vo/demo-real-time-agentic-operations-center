"""The patch agent's contract: the request line it is sent and the reply it answers. No I/O.

The security agent (lab 6.3) sends one line, ``PATCH <repository> <cve_id> <package>
<installed_version> -> <fixed_version>``, and copies the reply into its ``Patch:`` line.
Lab 6.4 pulls the commit and the patch's start and finish back out of that line with
REGEXP_EXTRACT, and its patterns are tested against ``REPLY_COMMIT``, ``REPLY_STARTED``
and ``REPLY_FINISHED`` below, so the reply and the parsing cannot drift apart (D-070).
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

# Use with fullmatch. Surrounding whitespace and a trailing full stop are tolerated, since a
# model may add either; anything else is not a request (and is echoed in the reply).
REQUEST = re.compile(
    r"\s*PATCH (?P<repository>\S+) (?P<cve_id>CVE-\d{4}-\d{4,}) (?P<package>\S+) "
    r"(?P<installed_version>\S+) -> (?P<fixed_version>\S+?)\.?\s*"
)

# Lab 6.4 runs these in Flink's REGEXP_EXTRACT, which is Java regex: one numbered group
# each, and nothing Python-only.
_ISO_MS = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z"
REPLY_COMMIT = re.compile(r"in commit ([0-9a-f]{7})\b")
REPLY_STARTED = re.compile(rf"started ({_ISO_MS})")
REPLY_FINISHED = re.compile(rf"finished ({_ISO_MS})")


def commit_for(request_text: str) -> str:
    """The pretend commit: the first 7 hex digits of the request's SHA-1."""
    return hashlib.sha1(request_text.encode("utf-8"), usedforsecurity=False).hexdigest()[:7]


def _utc_iso(moment: datetime) -> str:
    """2026-12-08T11:42:03.120Z: UTC, ISO 8601, milliseconds. ``moment`` must be timezone-aware."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def format_reply(request_text: str, started: datetime, finished: datetime) -> str:
    """One line: what was patched, the commit, and when the patch started and finished.

    A request that does not match ``REQUEST`` is patched all the same, with its text echoed
    in place of the bump, on one line.
    """
    tail = f"in commit {commit_for(request_text)}, started {_utc_iso(started)}, finished {_utc_iso(finished)}"
    request = REQUEST.fullmatch(request_text)
    if request is None:
        return f"Patched ({' '.join(request_text.split())}) {tail}"
    return (
        f"Patched {request['cve_id']} in {request['repository']}: bumped {request['package']} "
        f"{request['installed_version']} → {request['fixed_version']} {tail}"
    )
