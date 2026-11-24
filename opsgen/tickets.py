"""The ticket clock: checkout's support tickets and their answer key (D-068).

checkout produces no metrics. Instead, tickets arrive as if from Zendesk and ServiceNow, each
with the priority its reporter picked. Each 1-second tick draws Poisson(1/15) tickets, about 4
a minute, from the first tick on (no quiet period). For each one, in this order:
- its priority: P1 30 %, P2 30 %, P3 40 %;
- its true severity: HIGH with probability 1/2 for a P1, always LOW for P2 and P3, so routing
  on the priority never drops a real outage;
- for a HIGH one, its expected action, one of the three with equal odds;
- its source, Zendesk or ServiceNow with equal odds;
- a template with that priority, severity and action, and the values for its placeholders
  (`opsgen/ticket_templates.py`).

The clock draws from its own generator, `Random(seed + 3)`, and nothing else draws from it, so
tickets never move another service's schedule for a seed. Ids look native, one counter per
source: `ZD-48215`, `ZD-48216`, ... and `INC0041872`, ... (seven digits). The counters are in
the generator state file (D-046), so a restart never reuses an id.

Each ticket is two records on the same tick: the ticket (`support_tickets`, the seven columns
the agents read) and its answer key (`ticket_answer_key`: the true severity, and the expected
action or null), which only the wall and `evaluate` read.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import NamedTuple

from opsgen.distributions import poisson
from opsgen.services import TICKET_SERVICES
from opsgen.ticket_templates import ACTIONS, HIGH, LOW, TEMPLATES, Template, fill

SERVICE = TICKET_SERVICES[0]  # checkout
RATE = 1 / 15  # tickets a second, the Poisson mean of each tick
PRIORITIES = ("P1", "P2", "P3")
PRIORITY_WEIGHTS = (0.30, 0.30, 0.40)
P1_HIGH_SHARE = 0.5

ZENDESK = "ZENDESK"
SERVICENOW = "SERVICENOW"
SOURCES = (ZENDESK, SERVICENOW)
SOURCE_NAMES = {ZENDESK: "Zendesk", SERVICENOW: "ServiceNow"}
# The next number each source gives out on a fresh start: the design's next ids.
FIRST_NUMBERS = {ZENDESK: 48215, SERVICENOW: 41872}


def ticket_id(source: str, number: int) -> str:
    """`ZD-48215` for Zendesk, `INC0041872` (seven digits) for ServiceNow."""
    return f"ZD-{number}" if source == ZENDESK else f"INC{number:07d}"


class NewTicket(NamedTuple):
    """One ticket as the simulator writes it: the `support_tickets` record and its
    `ticket_answer_key` record, both on the same tick."""

    ticket: dict
    answer_key: dict


def _by_category() -> dict[tuple[str, str, str | None], tuple[Template, ...]]:
    out: dict[tuple[str, str, str | None], list[Template]] = {}
    for t in TEMPLATES:
        out.setdefault((t.priority, t.true_severity, t.expected_action), []).append(t)
    return {k: tuple(v) for k, v in out.items()}


TEMPLATES_BY_CATEGORY = _by_category()


@dataclass
class TicketClock:
    """When tickets arrive, and what they are. `tick(event_time_ms)` returns the tickets
    created on that tick. `next_numbers` is the next id number per source; `on` is False with
    `--no-incidents`, and the clock then produces nothing and draws nothing."""

    rng: random.Random
    next_numbers: dict[str, int] = field(default_factory=lambda: dict(FIRST_NUMBERS))
    on: bool = True

    def tick(self, event_time_ms: int) -> list[NewTicket]:
        if not self.on:
            return []
        return [self._ticket(event_time_ms) for _ in range(poisson(RATE, self.rng))]

    def _ticket(self, event_time_ms: int) -> NewTicket:
        rng = self.rng
        priority = rng.choices(PRIORITIES, weights=PRIORITY_WEIGHTS, k=1)[0]
        severity = HIGH if priority == "P1" and rng.random() < P1_HIGH_SHARE else LOW
        action = rng.choice(ACTIONS) if severity == HIGH else None
        source = rng.choice(SOURCES)
        template = rng.choice(TEMPLATES_BY_CATEGORY[(priority, severity, action)])
        subject, description = fill(template, rng)
        tid = ticket_id(source, self.next_numbers[source])
        self.next_numbers[source] += 1
        ticket = {
            "event_time": event_time_ms,
            "ticket_id": tid,
            "source": source,
            "service_name": SERVICE,
            "priority": priority,
            "subject": subject,
            "description": description,
        }
        answer_key = {
            "event_time": event_time_ms,
            "ticket_id": tid,
            "true_severity": severity,
            "expected_action": action,
        }
        return NewTicket(ticket, answer_key)
