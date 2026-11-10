"""Resolve who the developer is, which environment is theirs, and what is in it.

The pre-provisioned deploy starts from nothing but the Global key it mints for the
developer. Everything else is derived from it: the owning user ID and email; the
environment the platform team created (found by the `<sanitized local part>-<4 hex>_env`
name when the key can see several); and the resources inside it (the cluster and Flink
pool by their fixed names, the environment's Schema Registry, the region's Flink
endpoint). Nothing here creates anything.

`choose_environment` and `sanitize_local_part` are pure so the decision is testable
without HTTP; the fetchers are thin wrappers over the Confluent Cloud REST API.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import NamedTuple

import requests

API = "https://api.confluent.cloud"
TIMEOUT = 30

# Confluent environment names allow only [A-Za-z0-9_-] and must start and end
# alphanumeric; the platform module leaves room for the "-<4 hex>_env" suffix.
MAX_LOCAL_PART = 40

# What platform-team/modules/developer names the two resources it builds per developer.
CLUSTER_NAME = "RIVER-OOPS-DEMO-CLUSTER"
POOL_NAME = "RIVER-OOPS-DEMO-pool"

# crn://confluent.cloud/organization=<id>/environment=env-…/…
_ORG_IN_CRN = re.compile(r"/organization=([^/]+)")


class Environment(NamedTuple):
    id: str
    name: str


@dataclass(frozen=True)
class Choice:
    """What `choose_environment` decided. Exactly one field is set: `env` (decided) or
    `candidates` (ambiguous — ask)."""

    env: Environment | None = None
    candidates: tuple[Environment, ...] = ()


class NoEnvironmentError(SystemExit):
    pass


NO_ENVIRONMENT_MESSAGE = (
    "Your Global key sees no environment you're directly bound to.\n"
    "Ask your platform team for a direct EnvironmentAdmin binding on your user (a binding\n"
    "through an SSO group mapping isn't enough — API keys don't carry group-mapped permissions)."
)


class NotProvisionedError(SystemExit):
    pass


@dataclass(frozen=True)
class Resources:
    organization_id: str
    cloud: str
    region: str
    cluster_id: str
    cluster_name: str
    cluster_bootstrap: str
    cluster_rest_endpoint: str
    compute_pool_id: str
    schema_registry_id: str
    schema_registry_url: str
    flink_rest_endpoint: str


def sanitize_local_part(email: str) -> str:
    """`John.Doe@acme.com` -> `John-Doe`. Twin of `local.sanitized` in
    platform-team/main.tf (keep both, and the example lists, in step). Raises
    ValueError when nothing usable is left."""
    local = email.split("@", 1)[0]
    name = re.sub(r"[^A-Za-z0-9_-]", "-", local)
    name = re.sub(r"-+", "-", name)
    name = name[:MAX_LOCAL_PART].strip("-_")
    if not name:
        raise ValueError(f"{email!r} has no letters or digits before '@' to name an environment after")
    return name


def choose_environment(envs: list[Environment], email: str) -> Choice:
    """Pick the developer's environment. No I/O. One visible -> it; several -> the one
    named `<sanitized>-<4 hex>_env`, else a candidate list; none -> NoEnvironmentError."""
    if not envs:
        raise NoEnvironmentError(NO_ENVIRONMENT_MESSAGE)
    if len(envs) == 1:
        return Choice(env=envs[0])
    try:
        stem = sanitize_local_part(email)
    except ValueError:
        stem = None
    if stem is not None:
        pattern = re.compile(rf"^{re.escape(stem)}-[0-9a-f]{{4}}_env$", re.IGNORECASE)
        matches = [e for e in envs if pattern.match(e.name)]
        if len(matches) == 1:
            return Choice(env=matches[0])
    return Choice(candidates=tuple(sorted(envs, key=lambda e: e.name.lower())))


def resolve_user_id(key: str, secret: str) -> str:
    resp = requests.get(f"{API}/iam/v2/api-keys/{key}", auth=(key, secret), timeout=TIMEOUT)
    _refuse_rejected_key(resp)
    resp.raise_for_status()
    owner = ((resp.json().get("spec") or {}).get("owner") or {}).get("id") or ""
    if not owner.startswith("u-"):
        raise SystemExit(
            f"  API key {key} is owned by {owner or 'an unknown principal'}, not a user account.\n"
            "  Log the Confluent CLI in as yourself (`confluent login`) and re-run."
        )
    return owner


def user_email(key: str, secret: str, user_id: str) -> str:
    try:
        resp = requests.get(f"{API}/iam/v2/users/{user_id}", auth=(key, secret), timeout=TIMEOUT)
        if resp.status_code != 200:
            return ""
        return str(resp.json().get("email") or "").strip()
    except (requests.RequestException, ValueError):
        return ""


def visible_environments(key: str, secret: str) -> list[Environment]:
    envs: list[Environment] = []
    url = f"{API}/org/v2/environments?page_size=100"
    while url:
        resp = requests.get(url, auth=(key, secret), timeout=TIMEOUT)
        _refuse_rejected_key(resp)
        resp.raise_for_status()
        body = resp.json()
        for env in body.get("data") or []:
            env_id, name = env.get("id"), env.get("display_name")
            if env_id and name:
                envs.append(Environment(env_id, name))
        url = (body.get("metadata") or {}).get("next") or ""
    return envs


def find_resources(key: str, secret: str, env: Environment) -> Resources:
    query = {"environment": env.id, "page_size": 100}
    clusters = _get_list(key, secret, "/cmk/v2/clusters", query)
    pools = _get_list(key, secret, "/fcpm/v2/compute-pools", query)
    registries = _get_list(key, secret, "/srcm/v3/clusters", {"environment": env.id})

    cluster = next((c for c in clusters if _spec(c).get("display_name") == CLUSTER_NAME), None)
    pool = next((p for p in pools if _spec(p).get("display_name") == POOL_NAME), None)
    registry = registries[0] if registries else None
    missing = [
        what
        for what, found in (
            (f"Kafka cluster {CLUSTER_NAME}", cluster),
            (f"Flink compute pool {POOL_NAME}", pool),
            ("Schema Registry", registry),
        )
        if found is None
    ]
    if missing:
        raise NotProvisionedError(
            f"Your platform team hasn't provisioned {env.name} ({env.id}) yet: it has no "
            f"{', no '.join(missing)}.\n"
            f"Send them this environment ID, {env.id}, and re-run once they have."
        )

    spec = _spec(cluster)
    cloud, region = str(spec.get("cloud") or ""), str(spec.get("region") or "")
    org = next(
        (m.group(1) for item in (cluster, pool, registry) if (m := _ORG_IN_CRN.search(_crn(item)))),
        "",
    )
    if not org:
        raise SystemExit(f"  Could not tell which organization {env.name} ({env.id}) belongs to.")
    return Resources(
        organization_id=org,
        cloud=cloud,
        region=region,
        cluster_id=cluster["id"],
        cluster_name=CLUSTER_NAME,
        cluster_bootstrap=str(spec.get("kafka_bootstrap_endpoint") or ""),
        cluster_rest_endpoint=str(spec.get("http_endpoint") or ""),
        compute_pool_id=pool["id"],
        schema_registry_id=registry["id"],
        schema_registry_url=str(_spec(registry).get("http_endpoint") or ""),
        flink_rest_endpoint=flink_rest_endpoint(key, secret, cloud, region),
    )


def flink_rest_endpoint(key: str, secret: str, cloud: str, region: str) -> str:
    regions = _get_list(key, secret, "/fcpm/v2/regions", {"cloud": cloud, "region_name": region})
    endpoint = next((str(r["http_endpoint"]) for r in regions if r.get("http_endpoint")), "")
    if not endpoint:
        raise SystemExit(f"  Confluent Cloud lists no Flink endpoint for {cloud} {region}.")
    return endpoint


def _get_list(key: str, secret: str, path: str, params: dict) -> list[dict]:
    resp = requests.get(f"{API}{path}", auth=(key, secret), params=params, timeout=TIMEOUT)
    _refuse_rejected_key(resp)
    resp.raise_for_status()
    return list(resp.json().get("data") or [])


def _spec(item: dict) -> dict:
    return item.get("spec") or {}


def _crn(item: dict) -> str:
    return str((item.get("metadata") or {}).get("resource_name") or "")


def _refuse_rejected_key(resp: requests.Response) -> None:
    if resp.status_code in (401, 403):
        raise SystemExit(
            f"  Confluent Cloud rejected the API key (HTTP {resp.status_code}).\n"
            "  The deploy mints your Global key with the Confluent CLI; re-run it, and if this\n"
            "  persists, check the key still exists under Console → API keys → My account."
        )
