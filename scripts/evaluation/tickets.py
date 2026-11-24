"""Score the ticket pipeline (D-068): `p1_tickets` (lab 5.1), `ticket_triage` (lab 5.2) and the
ticket decisions in `remediation_decisions` (lab 5.3), against the tickets the generator filed
(`support_tickets`) and its answer key (`ticket_answer_key`).

Pure functions over plain data, so they're unit-tested offline. The definitions are:

- **Scored tickets.** The P1 tickets, by the priority they were filed with, created in the
  scored range (the metric half's) and found in the answer key.
- **Verdict.** The `severity` of the ticket's first `ticket_triage` record: HIGH, LOW or
  INVALID. A ticket with no triage record written by the end of the range plus 30 s is
  **never classified**, which usually means the 5.2 INSERT isn't running; a record read
  without a Kafka timestamp always counts. A verdict is right when it equals the true severity,
  so INVALID and never classified count as wrong. Classifier accuracy = right verdicts /
  scored tickets.
- **Answers.** A ticket decision is a row with a `ticket_id`; a ticket's answer is its first
  one by Kafka timestamp. For really-HIGH tickets classified HIGH: answered or not, and right
  when the action is the expected one (INVALID covers a failed call or an action outside the
  three). Responder accuracy = right actions / answered.
  - An answer to a really-LOW ticket called HIGH (a classifier false alarm) is **unexpected**,
    and not scored against the responder.
  - An answer to a ticket with no HIGH verdict is a **routing error**: the 5.3 INSERT reads
    HIGH rows only.
  - A ticket decision carries no target, and comes from `ticket_responder` on
    `HIGH_SEVERITY_TICKET`; any other is listed.
- **Filter leaks.** P2 or P3 tickets created in the range that are in `p1_tickets` or
  `ticket_triage`.
- **Timing.** The medians of ticket `event_time` → its triage record's Kafka timestamp, and of
  that → its decision's Kafka timestamp.
- **Acceptance.** The classifier at least 95 %, the responder at least 95 %, no leaks. With no
  scored ticket, all three report NO_DATA instead of failing; the responder also reports
  NO_DATA until it has answered.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from statistics import median

from scripts.common.topic_reader import KAFKA_TIMESTAMP, epoch_seconds
from scripts.evaluation.decisions import ALLOWED_ACTIONS, TICKET_RESPONDER, DecisionRow
from scripts.evaluation.detection import NO_DATA, UNLOGGED_TAIL_SECONDS

P1_TICKETS = "p1_tickets"
TICKET_TRIAGE = "ticket_triage"
HIGH = "HIGH"
LOW = "LOW"
INVALID = "INVALID"
NEVER = "NEVER"  # no triage record: the verdict of a ticket the classifier never saw
# A verdict counts when it was written by the end of the span plus this grace.
CLASSIFY_GRACE_SECONDS = 30.0
CLASSIFIER_TARGET = 0.95
RESPONDER_TARGET = 0.95
PASS = "PASS"
FAIL = "FAIL"


@dataclass(frozen=True)
class Ticket:
    """A `support_tickets` or `p1_tickets` row."""

    ticket_id: str
    event_time: float
    priority: str
    subject: str | None = None

    @classmethod
    def from_record(cls, rec: dict) -> Ticket:
        return cls(
            ticket_id=rec["ticket_id"],
            event_time=epoch_seconds(rec["event_time"]),
            priority=_text(rec.get("priority")),
            subject=rec.get("subject"),
        )


@dataclass(frozen=True)
class AnswerKey:
    """A `ticket_answer_key` row: what the ticket really is."""

    ticket_id: str
    true_severity: str
    expected_action: str | None = None

    @classmethod
    def from_record(cls, rec: dict) -> AnswerKey:
        return cls(
            ticket_id=rec["ticket_id"],
            true_severity=_text(rec.get("true_severity")),
            expected_action=_text(rec.get("expected_action")) or None,
        )


@dataclass(frozen=True)
class Triage:
    """A `ticket_triage` row: the classifier's verdict on one P1 ticket."""

    ticket_id: str
    event_time: float
    priority: str
    severity: str
    reason: str | None = None
    raw_response: str | None = None
    written_at: float | None = None  # the record's Kafka timestamp, when it was read with one

    @classmethod
    def from_record(cls, rec: dict) -> Triage:
        return cls(
            ticket_id=rec["ticket_id"],
            event_time=epoch_seconds(rec["event_time"]),
            priority=_text(rec.get("priority")),
            severity=_text(rec.get("severity")).upper() or INVALID,
            reason=rec.get("reason"),
            raw_response=rec.get("raw_response"),
            written_at=epoch_seconds(rec.get(KAFKA_TIMESTAMP)),
        )


def _text(v: str | None) -> str:
    return v.strip() if isinstance(v, str) else ""


def default_range(tickets: list[Ticket], minutes: float) -> tuple[float, float]:
    """The last `minutes` of tickets, ending UNLOGGED_TAIL_SECONDS before the newest one: the
    metric half's rule, for a run that has no metric rows."""
    to = max(t.event_time for t in tickets) - UNLOGGED_TAIL_SECONDS
    return to - minutes * 60.0, to


@dataclass
class TicketResult:
    ticket: Ticket
    truth: AnswerKey
    triage: Triage | None = None
    decision: DecisionRow | None = None  # the responder's first answer

    @property
    def verdict(self) -> str:
        return self.triage.severity if self.triage is not None else NEVER

    @property
    def classified_right(self) -> bool:
        return self.verdict == self.truth.true_severity

    @property
    def to_answer(self) -> bool:
        """Really HIGH and classified HIGH: the responder should answer it."""
        return self.truth.true_severity == HIGH and self.verdict == HIGH

    @property
    def answered_right(self) -> bool:
        return self.decision is not None and self.decision.action == self.truth.expected_action


@dataclass
class TicketReport:
    range_from: float
    range_to: float
    results: list[TicketResult] = field(default_factory=list)
    bad_targets: list[DecisionRow] = field(default_factory=list)  # a ticket decision never has a target
    unexpected: list[DecisionRow] = field(default_factory=list)  # answers to really-LOW tickets called HIGH
    routing_errors: list[DecisionRow] = field(default_factory=list)  # answers to tickets with no HIGH verdict
    misrouted: list[DecisionRow] = field(default_factory=list)  # not ticket_responder on HIGH_SEVERITY_TICKET
    leaks: list[tuple[str, str, str]] = field(default_factory=list)  # (table, ticket_id, priority)

    @property
    def matrix(self) -> Counter:
        return Counter((r.truth.true_severity, r.verdict) for r in self.results)

    @property
    def classified_right(self) -> int:
        return sum(r.classified_right for r in self.results)

    @property
    def invalid(self) -> int:
        return sum(r.verdict == INVALID for r in self.results)

    @property
    def never_classified(self) -> int:
        return sum(r.verdict == NEVER for r in self.results)

    @property
    def classifier_accuracy(self) -> float | None:
        return self.classified_right / len(self.results) if self.results else None

    @property
    def to_answer(self) -> list[TicketResult]:
        return [r for r in self.results if r.to_answer]

    @property
    def answered(self) -> list[TicketResult]:
        return [r for r in self.to_answer if r.decision is not None]

    @property
    def right_actions(self) -> int:
        return sum(r.answered_right for r in self.answered)

    @property
    def invalid_actions(self) -> int:
        return sum(r.decision.action not in ALLOWED_ACTIONS[TICKET_RESPONDER] for r in self.answered)

    @property
    def responder_accuracy(self) -> float | None:
        return self.right_actions / len(self.answered) if self.answered else None

    @property
    def median_to_verdict(self) -> float | None:
        """Ticket `event_time` → the triage record's Kafka timestamp."""
        return _median([r.triage.written_at - r.ticket.event_time for r in self.results
                        if r.triage is not None and r.triage.written_at is not None])  # fmt: skip

    @property
    def median_to_action(self) -> float | None:
        """The triage record's Kafka timestamp → the decision's."""
        return _median([r.decision.written_at - r.triage.written_at for r in self.results
                        if r.triage is not None and r.decision is not None
                        and r.triage.written_at is not None and r.decision.written_at is not None])  # fmt: skip

    @property
    def classifier_status(self) -> str:
        return _status(self.classifier_accuracy, CLASSIFIER_TARGET)

    @property
    def responder_status(self) -> str:
        return NO_DATA if not self.results else _status(self.responder_accuracy, RESPONDER_TARGET)

    @property
    def leaks_status(self) -> str:
        return NO_DATA if not self.results else FAIL if self.leaks else PASS


def _status(share: float | None, target: float) -> str:
    return NO_DATA if share is None else PASS if share >= target else FAIL


def _median(values: list[float]) -> float | None:
    return median(values) if values else None


def score_tickets(
    tickets: list[Ticket],
    answer_keys: list[AnswerKey],
    p1_rows: list[Ticket],
    triage: list[Triage],
    decisions: list[DecisionRow],
    range_from: float,
    range_to: float,
) -> TicketReport:
    keys = {k.ticket_id: k for k in answer_keys}
    cutoff = range_to + CLASSIFY_GRACE_SECONDS
    verdicts: dict[str, Triage] = {}  # what the classifier said in time, for the classification
    any_verdict: dict[str, Triage] = {}  # what the responder's INSERT could have seen, for its routing
    for t in triage:
        any_verdict.setdefault(t.ticket_id, t)
        if t.written_at is None or t.written_at <= cutoff:
            verdicts.setdefault(t.ticket_id, t)
    in_span = {t.ticket_id for t in tickets if range_from <= t.event_time < range_to}
    scoped = sorted((d for d in decisions if d.ticket_id in in_span), key=_written)
    answers: dict[str, DecisionRow] = {}
    for d in scoped:
        answers.setdefault(d.ticket_id, d)
    rep = TicketReport(
        range_from, range_to,
        bad_targets=[d for d in scoped if d.target],
        misrouted=[d for d in scoped if not d.routed_correctly],
    )  # fmt: skip
    rep.leaks = _leaks(tickets, p1_rows, triage, range_from, range_to)
    for tid, d in answers.items():
        truth, verdict = keys.get(tid), any_verdict.get(tid)
        if verdict is None or verdict.severity != HIGH:
            rep.routing_errors.append(d)
        elif truth is not None and truth.true_severity == LOW:
            rep.unexpected.append(d)
    for t in sorted(tickets, key=lambda t: t.event_time):
        if t.priority != "P1" or not (range_from <= t.event_time < range_to) or t.ticket_id not in keys:
            continue
        tid = t.ticket_id
        rep.results.append(TicketResult(t, keys[tid], verdicts.get(tid), answers.get(tid)))
    return rep


def _leaks(
    tickets: list[Ticket], p1_rows: list[Ticket], triage: list[Triage], range_from: float, range_to: float
) -> list[tuple[str, str, str]]:
    """P2 and P3 tickets created in the span that got past lab 5.1's filter. The priority is
    the one the ticket was filed with, or the row's own copy for a ticket not read."""
    filed = {t.ticket_id: t.priority for t in tickets}
    out: list[tuple[str, str, str]] = []
    for table, rows in ((P1_TICKETS, p1_rows), (TICKET_TRIAGE, triage)):
        for r in sorted(rows, key=lambda r: r.event_time):
            priority = filed.get(r.ticket_id, r.priority)
            leak = (table, r.ticket_id, priority)
            if priority != "P1" and range_from <= r.event_time < range_to and leak not in out:
                out.append(leak)
    return out


def _written(d: DecisionRow) -> float:
    """Order a ticket's answers by when they were written (file order without timestamps)."""
    return d.written_at if d.written_at is not None else 0.0
