"""Kafka output path — Schema Registry lookup and Avro key/value serialization.

Implemented in Phase 2, mirroring the reference simulator: resolve the latest
registered schema for each subject at startup, serialize service_requests values
(append-only, raw string key = service_name) and service_deployments key+value
(upsert, key Avro-encoded against the registered service_deployments-key subject).

Kept as a placeholder now so the package layout is stable; `opsgen.cli` exits with a
clear message for live mode until this lands.
"""

from __future__ import annotations


class KafkaSink:  # pragma: no cover - Phase 2
    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError("The Kafka path is implemented in Phase 2; use ops-sim --dry-run.")
