"""The scan clock: CVE scan findings for River Shop's repositories and their answer key (D-070).

A scanner reports findings, each one CVE in one repository. Each 1-second tick draws
Poisson(1/15) findings, about 4 a minute, from the first tick on. For each one, in this order:
- whether it is critical: 30 %;
- for a critical one, its expected action, `PATCH` or `DISMISS` with equal odds, and for a
  `DISMISS` its reason, one of the three with equal odds;
- a template of that category, and its advisory, repository and other placeholders
  (`opsgen/finding_templates.py`);
- its CVSS score, one decimal: 9.0 to 10.0 when critical, else 4.0 to 8.9.

So about 1.2 critical findings a minute, 0.6 of them `PATCH` (a call to the patch agent).

The clock draws from its own generator, `Random(seed + 4)`, and nothing else draws from it, so
findings never move another service's schedule or checkout's tickets for a seed. Ids are
`SCAN-000001`, `SCAN-000002`, ...: one counter, in the generator state file (D-046), so a
restart never reuses an id.

Each finding is two records on the same tick: the finding (`cve_scans`, what the scanner said)
and its answer key (`cve_answer_key`: the expected action, or null when it isn't critical),
which only the wall (and later `evaluate`) reads.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import NamedTuple

from opsgen.distributions import poisson
from opsgen.finding_templates import DISMISS, DISMISS_REASONS, PATCH, TEMPLATES, FindingTemplate, fill

RATE = 1 / 15  # findings a second, the Poisson mean of each tick
CRITICAL_SHARE = 0.30
PATCH_SHARE = 0.5  # of the critical ones; the rest are DISMISS
CRITICAL_SCORES = (90, 100)  # tenths, both ends included: CVSS 9.0 to 10.0
ORDINARY_SCORES = (40, 89)  # CVSS 4.0 to 8.9
FIRST_NUMBER = 1  # the number of the first finding on a fresh start
ID_FORMAT = "SCAN-{:06d}"


class NewFinding(NamedTuple):
    """One finding as the simulator writes it: its scan, the `cve_scans` record, and its
    `cve_answer_key` record, both on the same tick."""

    scan: dict
    answer_key: dict


def _by_category() -> dict[tuple[str | None, str | None], tuple[FindingTemplate, ...]]:
    out: dict[tuple[str | None, str | None], list[FindingTemplate]] = {}
    for t in TEMPLATES:
        out.setdefault((t.expected_action, t.dismiss_reason), []).append(t)
    return {k: tuple(v) for k, v in out.items()}


TEMPLATES_BY_CATEGORY = _by_category()


@dataclass
class ScanClock:
    """When findings arrive, and what they are. `tick(event_time_ms)` returns the findings
    reported on that tick. `next_number` is the next id's number; `on` is False with
    `--no-incidents`, and the clock then produces nothing and draws nothing."""

    rng: random.Random
    next_number: int = FIRST_NUMBER
    on: bool = True

    def tick(self, event_time_ms: int) -> list[NewFinding]:
        if not self.on:
            return []
        return [self._finding(event_time_ms) for _ in range(poisson(RATE, self.rng))]

    def _finding(self, event_time_ms: int) -> NewFinding:
        rng = self.rng
        action = reason = None
        if rng.random() < CRITICAL_SHARE:
            action = PATCH if rng.random() < PATCH_SHARE else DISMISS
            reason = rng.choice(DISMISS_REASONS) if action == DISMISS else None
        template = rng.choice(TEMPLATES_BY_CATEGORY[(action, reason)])
        filled = fill(template, rng)
        score = rng.randint(*(CRITICAL_SCORES if template.critical else ORDINARY_SCORES)) / 10
        finding_id = ID_FORMAT.format(self.next_number)
        self.next_number += 1
        scan = {
            "event_time": event_time_ms,
            "finding_id": finding_id,
            "repository": filled["repository"],
            "cve_id": filled["cve_id"],
            "package": filled["package"],
            "installed_version": filled["installed_version"],
            "fixed_version": filled["fixed_version"],
            "cvss_score": score,
            "finding": filled["finding"],
        }
        answer_key = {"event_time": event_time_ms, "finding_id": finding_id, "expected_action": action}
        return NewFinding(scan, answer_key)
