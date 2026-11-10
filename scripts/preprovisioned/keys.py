"""The developer's three API keys: minted, waited for, checked and deleted.

- a **Global** key, minted by the Confluent CLI as whoever ran `confluent login` (an
  OrganizationAdmin can't mint keys for another user); it is also the card's Flink and
  RTCE key and authenticates everything below;
- a **Kafka** and a **Schema Registry** key, minted over REST with the Global key.

New keys are refused for a while after creation, so each one's first use is retried for
up to `PROPAGATION_TIMEOUT`.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from collections.abc import Callable

import requests

from scripts.preprovisioned.lookup import API, TIMEOUT

PROPAGATION_TIMEOUT = 180
POLL_SECONDS = 5

CLI_INSTALL = "https://docs.confluent.io/confluent-cli/current/install.html"
DESCRIPTION = "Self-Healing IT workshop: uv run deploy --track pre-provisioned"

_CAP_WORDS = re.compile(r"quota|limit|maximum|exceed", re.IGNORECASE)


class GlobalKeyCapError(SystemExit):
    """The developer already holds the 2 Global keys a user may own."""


def mint_global_key(previous: str = "") -> tuple[str, str]:
    """Mint a Global key for the logged-in CLI user: (key, secret)."""
    cmd = ["confluent", "api-key", "create", "--resource", "global", "--description", DESCRIPTION, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        raise SystemExit(f"  The Confluent CLI is not installed: {CLI_INSTALL}") from None
    except subprocess.TimeoutExpired:
        raise SystemExit("  `confluent api-key create --resource global` did not answer within 2 minutes.") from None
    error = (result.stderr or result.stdout or "").strip()
    if result.returncode != 0:
        if _CAP_WORDS.search(error):
            last = f"\n  Your last deploy minted {previous}; if it's still listed, delete that one." if previous else ""
            raise GlobalKeyCapError(
                f"  Confluent Cloud refused a new Global key: {error}\n"
                "  Each account can hold 2 Global keys. Delete one under\n"
                f"  Console → API keys → My account, then re-run.{last}"
            )
        raise SystemExit(f"  `confluent api-key create --resource global` failed: {error or 'no output'}")
    try:
        data = json.loads(result.stdout)
        return data["api_key"], data["api_secret"]
    except (ValueError, KeyError, TypeError):
        raise SystemExit(f"  Unexpected output from `confluent api-key create`: {result.stdout[:200]!r}") from None


def create_api_key(
    auth: tuple[str, str], owner_id: str, resource_id: str, environment_id: str, display_name: str
) -> tuple[str, str]:
    """Mint a key owned by `owner_id` for one cluster or Schema Registry: (key, secret)."""
    body = {
        "spec": {
            "display_name": display_name,
            "description": DESCRIPTION,
            "owner": {"id": owner_id},
            "resource": {"id": resource_id, "environment": environment_id},
        }
    }
    resp = requests.post(f"{API}/iam/v2/api-keys", auth=auth, json=body, timeout=TIMEOUT)
    if not 200 <= resp.status_code < 300:
        raise SystemExit(f"  Could not create a key for {resource_id} (HTTP {resp.status_code}): {resp.text[:500]}")
    key = resp.json()
    return key["id"], key["spec"]["secret"]


def key_exists(auth: tuple[str, str], key_id: str) -> bool:
    """Whether `key_id` exists, as far as `auth` can tell. False on 401/403/404; raises
    on other errors so a network blip is never mistaken for a revoked key."""
    resp = requests.get(f"{API}/iam/v2/api-keys/{key_id}", auth=auth, timeout=TIMEOUT)
    if resp.status_code in (401, 403, 404):
        return False
    resp.raise_for_status()
    return True


def delete_api_key(auth: tuple[str, str], key_id: str) -> str:
    """Delete `key_id`: "deleted", "gone", or "failed". Never raises."""
    try:
        resp = requests.delete(f"{API}/iam/v2/api-keys/{key_id}", auth=auth, timeout=TIMEOUT)
    except requests.RequestException:
        return "failed"
    if resp.status_code == 404:
        return "gone"
    return "deleted" if 200 <= resp.status_code < 300 else "failed"


def kafka_key_works(rest_endpoint: str, cluster_id: str, key: str, secret: str) -> bool:
    url = f"{rest_endpoint.rstrip('/')}/kafka/v3/clusters/{cluster_id}/topics"
    return _ok(url, (key, secret))


def schema_registry_key_works(registry_url: str, key: str, secret: str) -> bool:
    return _ok(f"{registry_url.rstrip('/')}/subjects", (key, secret))


def wait_until(check: Callable[[], bool], timeout: float | None = None) -> bool:
    deadline = time.monotonic() + (PROPAGATION_TIMEOUT if timeout is None else timeout)
    while True:
        try:
            if check():
                return True
        except requests.RequestException:
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(POLL_SECONDS)


def _ok(url: str, auth: tuple[str, str]) -> bool:
    try:
        return requests.get(url, auth=auth, timeout=TIMEOUT).status_code == 200
    except requests.RequestException:
        return False
