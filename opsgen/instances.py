"""Deterministic instance naming, like a Kubernetes ReplicaSet.

An instance id is `{service}-{version_hash}-{replica_suffix}`:

- `version_hash` is the first 6 hex chars of a hash of `service + version`, so a
  deploy changes it and a rollback restores the old one;
- `replica_suffix` is 5 lowercase alphanumeric chars derived deterministically from
  `service + version + replica index`, so names are stable across generator restarts.

Nothing here uses the shared rng: instance names must be reproducible from state
alone, independent of the random stream.
"""

from __future__ import annotations

import hashlib

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _digest(*parts: str) -> bytes:
    return hashlib.sha1(":".join(parts).encode("utf-8")).digest()


def version_hash(service: str, version: str) -> str:
    """First 6 hex characters of a hash of service + version."""
    return hashlib.sha1(f"{service}:{version}".encode()).hexdigest()[:6]


def replica_suffix(service: str, version: str, index: int) -> str:
    """5 lowercase alphanumeric characters, deterministic from service+version+index."""
    n = int.from_bytes(_digest(service, version, str(index)), "big")
    out = []
    for _ in range(5):
        n, r = divmod(n, 36)
        out.append(_B36[r])
    return "".join(out)


def instance_id(service: str, version: str, index: int) -> str:
    return f"{service}-{version_hash(service, version)}-{replica_suffix(service, version, index)}"


def instance_ids(service: str, version: str, ready_replicas: int) -> list[str]:
    """The ready instances currently serving this service at this version."""
    return [instance_id(service, version, i) for i in range(ready_replicas)]
