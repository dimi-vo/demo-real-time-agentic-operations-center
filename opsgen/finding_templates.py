"""The CVE scan finding templates and the pools that fill them (D-070).

A finding is one CVE in one repository, as a scanner reports it. Each template is one kind of
finding: the action the security agent should take on it when it is critical (`PATCH` or
`DISMISS`, None for an ordinary one), why a `DISMISS` (`dev_only`, `archived` or
`not_called`), the repositories it may name, and the scanner's sentence. Placeholders are
`str.format` names: `{repository}`, the advisory's fields (`{package}`, `{installed_version}`,
`{function}`, `{flaw}`, ...) and `{calls}`, all filled by `fill`.

The score already says critical, so a critical finding's sentence says where the package
sits, and that decides it (spec decision 16):
- 6 `PATCH`: the package is a runtime dependency of a deployed service (web-gateway, checkout
  or payments) and the scanner says the vulnerable code is used;
- 6 `DISMISS`, 2 per reason: `dev_only`, the package comes in only through test or build
  tooling; `archived`, the repository is storefront-legacy, archived and not deployed;
  `not_called`, it ships, but the reachability check finds no call to the vulnerable function;
- 8 ordinary findings, in any repository, which lab 6.1 never routes.

No sentence says "patch" or "dismiss", and none holds an apostrophe, so lab 6.3's examples can
be taken from them. The scan clock (`opsgen/findings.py`) picks a template of the category it
drew.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, fields

from opsgen.services import SERVICES

PATCH = "PATCH"
DISMISS = "DISMISS"
DEV_ONLY = "dev_only"
ARCHIVED = "archived"
NOT_CALLED = "not_called"
DISMISS_REASONS = (DEV_ONLY, ARCHIVED, NOT_CALLED)

# River Shop's own repositories (spec decision 4): the three services, the CI tooling, and
# the storefront the services replaced, archived.
REPOSITORIES: tuple[str, ...] = (*SERVICES, "ci-tools", "storefront-legacy")


@dataclass(frozen=True)
class FindingTemplate:
    expected_action: str | None  # PATCH | DISMISS for a critical finding, None for an ordinary one
    dismiss_reason: str | None  # one of DISMISS_REASONS for a DISMISS, else None
    repositories: tuple[str, ...]  # the repositories it may name
    finding: str  # the scanner's sentence

    @property
    def critical(self) -> bool:
        return self.expected_action is not None


@dataclass(frozen=True)
class Advisory:
    """One CVE: a real package at a plausible vulnerable version and the version that fixes
    it, with a fictional id (spec decision 5). The same CVE can be found in several
    repositories."""

    cve_id: str
    package: str
    installed_version: str
    fixed_version: str
    function: str  # the vulnerable function, which the scanner's reachability check looks for
    flaw: str  # what it lets an attacker do, a clause that follows the package or the function


ADVISORY_FIELDS: tuple[str, ...] = tuple(f.name for f in fields(Advisory))


def _patch(finding: str) -> FindingTemplate:
    return FindingTemplate(PATCH, None, SERVICES, finding)


def _dismiss(reason: str, repositories: tuple[str, ...], finding: str) -> FindingTemplate:
    return FindingTemplate(DISMISS, reason, repositories, finding)


def _ordinary(finding: str) -> FindingTemplate:
    return FindingTemplate(None, None, REPOSITORIES, finding)


TEMPLATES: tuple[FindingTemplate, ...] = (
    # --- PATCH: a runtime dependency of a deployed service, and the vulnerable code is used --
    _patch(
        "{package} {installed_version} is a runtime dependency of {repository}, which runs in production, "
        "and the reachability check found {calls} calls to the vulnerable {function} on the request path."
    ),
    _patch(
        "{repository} ships {package} {installed_version} in its production image, and its request handlers "
        "call {function}, the vulnerable function, on every incoming request."
    ),
    _patch(
        "The deployed {repository} service loads {package} {installed_version} at runtime, and the scanner "
        "traced a call path from a public endpoint to {function}, which {flaw}."
    ),
    _patch(
        "{package} {installed_version} is in the production lock file of {repository}, and the reachability "
        "check confirms that the running service calls {function}, the vulnerable code."
    ),
    _patch(
        "{repository} calls {function} from {package} {installed_version} in its live API handlers, and the "
        "package is a runtime dependency of the deployed service."
    ),
    _patch(
        "The scanner found {package} {installed_version} in the {repository} container that serves production "
        "traffic, with {calls} reachable calls to the vulnerable {function}."
    ),
    # --- DISMISS, dev_only: only test or build tooling brings it in ------------------------
    _dismiss(
        DEV_ONLY,
        SERVICES,
        "{package} {installed_version} comes into {repository} only as a dev dependency of its unit tests, "
        "and the production build leaves it out.",
    ),
    _dismiss(
        DEV_ONLY,
        (*SERVICES, "ci-tools"),
        "{package} {installed_version} is pulled in only by the build scripts in {repository}, which run in CI "
        "and never end up in anything that is deployed.",
    ),
    # --- DISMISS, archived: storefront-legacy, archived and not deployed --------------------
    _dismiss(
        ARCHIVED,
        ("storefront-legacy",),
        "{package} {installed_version} is pinned in {repository}, a repository that was archived in 2024 and "
        "is no longer deployed anywhere.",
    ),
    _dismiss(
        ARCHIVED,
        ("storefront-legacy",),
        "{repository} is archived and read-only, and nothing has been deployed from it since 2024, but its "
        "lock file still pins {package} {installed_version}.",
    ),
    # --- DISMISS, not_called: shipped, but the vulnerable function is never reached ---------
    _dismiss(
        NOT_CALLED,
        SERVICES,
        "{package} {installed_version} ships with the deployed {repository} service, but the reachability check "
        "found no call to {function}, the only vulnerable function, anywhere in its code.",
    ),
    _dismiss(
        NOT_CALLED,
        SERVICES,
        "{repository} has {package} {installed_version} as a runtime dependency in production, but the scanner "
        "traced no code path that reaches {function}, the function that {flaw}.",
    ),
    # --- ordinary findings, below critical, any repository ---------------------------------
    _ordinary("{package} {installed_version} in {repository} {flaw}."),
    _ordinary("In {repository}, {package} {installed_version} has a known issue: it {flaw}."),
    _ordinary("{repository} pins {package} {installed_version}, which {flaw}, and {fixed_version} fixes it."),
    _ordinary("The scanner matched {package} {installed_version} in {repository} to {cve_id}: the package {flaw}."),
    _ordinary("{repository} depends on {package} {installed_version}, whose {function} {flaw}."),
    _ordinary(
        "An older {package}, {installed_version}, in {repository} {flaw}, and versions from {fixed_version} on "
        "are not affected."
    ),
    _ordinary("{package} {installed_version} in {repository} {flaw}, according to the advisory for {cve_id}."),
    _ordinary("A dependency scan of {repository} reports that {package} {installed_version} {flaw}."),
)

# The advisories a finding names, by whether it is critical: a CVE is critical or not, never
# both, so lab 6.1 routes every finding of a CVE or none.
ADVISORIES: dict[bool, tuple[Advisory, ...]] = {
    True: (
        Advisory(
            "CVE-2026-41234",
            "urllib3",
            "1.26.4",
            "1.26.18",
            "PoolManager.urlopen",
            "forwards credentials to any host a crafted redirect points to",
        ),
        Advisory(
            "CVE-2026-42817",
            "jackson-databind",
            "2.9.10.1",
            "2.9.10.8",
            "ObjectMapper.readValue",
            "runs code from crafted JSON when polymorphic typing is on",
        ),
        Advisory(
            "CVE-2026-43390",
            "lodash",
            "4.17.15",
            "4.17.21",
            "_.template",
            "runs JavaScript injected through its template options",
        ),
        Advisory(
            "CVE-2026-44052",
            "pyyaml",
            "5.3.1",
            "5.4.1",
            "yaml.full_load",
            "runs arbitrary code from a crafted YAML document",
        ),
        Advisory(
            "CVE-2026-45128",
            "netty-codec-http",
            "4.1.42.Final",
            "4.1.44.Final",
            "HttpObjectDecoder.decode",
            "lets a crafted request smuggle a second request past the proxy",
        ),
        Advisory(
            "CVE-2026-46471",
            "jsonwebtoken",
            "8.5.1",
            "9.0.0",
            "jwt.verify",
            "accepts tokens signed with the none algorithm",
        ),
    ),
    False: (
        Advisory(
            "CVE-2026-40311",
            "express",
            "4.17.1",
            "4.19.2",
            "res.redirect",
            "can send a user to another site through a malformed redirect URL",
        ),
        Advisory(
            "CVE-2026-40586",
            "requests",
            "2.25.1",
            "2.31.0",
            "Session.send",
            "leaks the proxy password to the destination server after a redirect",
        ),
        Advisory(
            "CVE-2026-47019",
            "lodash",
            "4.17.15",
            "4.17.21",
            "_.trimEnd",
            "spends seconds of CPU on a crafted string",
        ),
        Advisory(
            "CVE-2026-47702",
            "jackson-core",
            "2.13.0",
            "2.15.0",
            "JsonParser.nextToken",
            "runs out of memory on a deeply nested document",
        ),
        Advisory(
            "CVE-2026-48245",
            "urllib3",
            "1.26.4",
            "1.26.18",
            "Retry.increment",
            "keeps the request body when a 303 redirect turns a POST into a GET",
        ),
        Advisory(
            "CVE-2026-48863",
            "netty-codec-http",
            "4.1.42.Final",
            "4.1.44.Final",
            "HttpContentDecompressor.decode",
            "inflates a crafted body without a size limit",
        ),
    ),
}

# The other placeholders' values.
POOLS: dict[str, tuple[str, ...]] = {
    "calls": ("2", "3", "4", "6", "9", "14"),
}


def placeholders(template: FindingTemplate) -> list[str]:
    """The placeholder names in a template's sentence, sorted."""
    return sorted({name for _, name, _, _ in string.Formatter().parse(template.finding) if name is not None})


def fill(template: FindingTemplate, rng: random.Random) -> dict[str, str]:
    """A finding's columns from a template: its repository, the advisory it names (`cve_id`,
    `package`, `installed_version`, `fixed_version`) and the scanner's sentence. Every finding
    draws an advisory and a repository, and a template's other placeholders draw from `POOLS`,
    all in sorted name order, so a seeded rng gives the same finding."""
    choices = {
        "advisory": ADVISORIES[template.critical],
        "repository": template.repositories,
        **{name: POOLS[name] for name in placeholders(template) if name in POOLS},
    }
    drawn = {name: rng.choice(choices[name]) for name in sorted(choices)}
    advisory: Advisory = drawn.pop("advisory")
    values = {**{f: getattr(advisory, f) for f in ADVISORY_FIELDS}, **drawn}
    return {
        "repository": values["repository"],
        "cve_id": advisory.cve_id,
        "package": advisory.package,
        "installed_version": advisory.installed_version,
        "fixed_version": advisory.fixed_version,
        "finding": template.finding.format(**values),
    }
