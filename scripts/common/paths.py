"""Where the repo root is, and where this track's run files live.

This workshop has a single track (pre-provisioned), so everything the developer's
tooling writes lives under ``runs/pre-provisioned/``: the credential card, the
deployment record, the generator's persisted state and the incident ground truth.
"""

from __future__ import annotations

from pathlib import Path

RUN_SUBDIR = ("runs", "pre-provisioned")


def get_project_root() -> Path:
    """The repo root (the directory that contains pyproject.toml), found from this file."""
    here = Path(__file__).resolve()
    for parent in (here, *here.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    # Fallback: scripts/common/paths.py -> repo root is two levels up.
    return here.parents[2]


def run_dir(root: Path) -> Path:
    return Path(root).joinpath(*RUN_SUBDIR)


def credentials_dir(root: Path) -> Path:
    return run_dir(root) / "credentials"
