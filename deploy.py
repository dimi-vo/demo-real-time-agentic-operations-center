"""`uv run deploy` — the developer's one deploy command for the Self-Healing IT workshop.

Only one track exists (pre-provisioned): the platform team built the environment, and
this mints the developer's keys and writes the credential card. `--track
pre-provisioned` is accepted for symmetry with the walkthrough; it's the default.
"""

from __future__ import annotations

import argparse

from scripts.common.paths import get_project_root
from scripts.preprovisioned import provision


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Deploy the Self-Healing IT workshop (pre-provisioned track)")
    parser.add_argument(
        "--track",
        choices=["pre-provisioned"],
        default="pre-provisioned",
        help="Only the pre-provisioned track exists (default).",
    )
    parser.add_argument("--automated", action="store_true", help="No prompts (for CI / scripted runs)")
    args = parser.parse_args(argv)

    provision.deploy(args, get_project_root())


if __name__ == "__main__":
    main()
