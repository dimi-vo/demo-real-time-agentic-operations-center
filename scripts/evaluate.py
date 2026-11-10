"""`uv run evaluate` — developer tool (not part of the walkthrough) that compares the
pipeline's output against the generator's incident ground truth.

Implemented in Phases 3-4: the detection half (reads `service_health`, scores detection
latency and false positives against `incidents.jsonl`) in Phase 3, and the decision half
(reads `remediation_decisions`, scores action validity and accuracy) in Phase 4.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> None:
    raise SystemExit(
        "uv run evaluate is implemented in Phases 3-4 (it needs service_health and "
        "remediation_decisions, which are built then)."
    )


if __name__ == "__main__":
    main()
