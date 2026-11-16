"""Per-service deployment state and the version/rollback semantics (brief section 4.3).

Every deploy records the prior version and bumps the patch number past the highest
version the service has ever run, so a version number is never reused: after 3.4.4 is
rolled back to 3.4.3, the next deploy is 3.4.5 (D-067). A rollback restores the prior
version and records the one it rolled back from. The only deploys are bad deploys, each
rolled back when its incident ends (D-057). Versions switch instantly — no mixed-version
period — which keeps the temporal join demo crisp.
`SCALE` is reserved for a later project and is never emitted here.
"""

from __future__ import annotations

from dataclasses import dataclass

from opsgen.services import Service

INITIAL = "INITIAL"
DEPLOY = "DEPLOY"
ROLLBACK = "ROLLBACK"


def next_patch_version(version: str) -> str:
    """Increment the patch number: 3.4.2 -> 3.4.3."""
    parts = version.split(".")
    parts[-1] = str(int(parts[-1]) + 1)
    return ".".join(parts)


def highest(*versions: str | None) -> str:
    """The highest of the given versions, compared numerically (3.4.10 > 3.4.9). Nones
    are skipped."""
    return max((v for v in versions if v), key=lambda v: tuple(int(p) for p in v.split(".")))


@dataclass
class DeploymentState:
    """Mutable current deployment state of one service. `deployed_at` is epoch seconds."""

    service_name: str
    version: str
    previous_version: str | None
    deployed_at: float
    last_change: str
    desired_replicas: int
    ready_replicas: int
    tier: str
    owner_team: str
    # The highest version this service has run; a deploy bumps past it (D-067). Defaults
    # to the higher of `version` and `previous_version`.
    highest_version: str = ""

    def __post_init__(self) -> None:
        self.highest_version = highest(self.highest_version, self.version, self.previous_version)

    @classmethod
    def initial(cls, service: Service, now: float) -> DeploymentState:
        return cls(
            service_name=service.name,
            version=service.initial_version,
            previous_version=None,
            deployed_at=now,
            last_change=INITIAL,
            desired_replicas=service.ready_replicas,
            ready_replicas=service.ready_replicas,
            tier=service.tier,
            owner_team=service.owner_team,
        )

    def deploy(self, now: float) -> tuple[str, str]:
        """Bump the patch number past the highest version run so far, so a version is
        never reused. Returns (old_version, new_version)."""
        old = self.version
        self.previous_version = old
        self.version = self.highest_version = next_patch_version(self.highest_version)
        self.deployed_at = now
        self.last_change = DEPLOY
        return old, self.version

    def rollback(self, now: float) -> tuple[str, str]:
        """Restore the prior version. Returns (rolled_back_from, restored_to).

        Caller must ensure `previous_version` is set (a bad deploy always sets it).
        """
        assert self.previous_version is not None, "rollback with no previous_version"
        rolled_from = self.version
        self.version = self.previous_version
        self.previous_version = rolled_from
        self.deployed_at = now
        self.last_change = ROLLBACK
        return rolled_from, self.version
