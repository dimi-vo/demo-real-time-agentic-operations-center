"""Confirm the Confluent CLI has a session (it mints the developer's Global key)."""

from __future__ import annotations

import subprocess


def confluent_logged_in() -> bool:
    """True if `confluent` has a usable session, via a cheap read-only call."""
    try:
        result = subprocess.run(
            ["confluent", "organization", "list", "-o", "json"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    err = (result.stderr or "").lower()
    return "not logged in" not in err and "log in" not in err
