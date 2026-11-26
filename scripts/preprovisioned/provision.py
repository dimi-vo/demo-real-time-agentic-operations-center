"""`uv run deploy --track pre-provisioned` — mint the developer's keys, write the card,
and check the environment is ready.

The platform team already built the environment (cluster, Flink pool, the seven source
tables, the seven output tables, the model). This deploy runs no Terraform: it mints a
Global key with the Confluent CLI (as the logged-in developer), uses it to mint a Kafka
and a Schema Registry key over REST, writes the credential card, and verifies that all
fourteen tables and `llm_textgen_model` exist. Key IDs are recorded in deployment.env the moment
each exists; a run that stops before the card is written deletes the keys it minted, so
a premature run never burns a Global-key slot.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests
from dotenv import dotenv_values

from scripts.common import card as card_mod
from scripts.common import deployment_meta as meta
from scripts.common.credentials import set_active_card
from scripts.common.flink import FlinkSession, list_tables, model_exists
from scripts.common.login_checks import confluent_logged_in
from scripts.common.topics import OUTPUT_TOPICS, SOURCE_TOPICS
from scripts.common.ui import confirm
from scripts.preprovisioned import keys, lookup

PREFIX = "DEMO"
KEY_LOCATION = "Console → API keys → My account"
MODEL_NAME = "llm_textgen_model"
# Each table is its topic's: the generator writes the sources, and the labs fill the outputs
# with INSERT statements; the platform team's Terraform creates all fourteen (D-050, D-068,
# D-070). The lists are `reset`'s (scripts/common/topics.py).
SOURCE_TABLES = SOURCE_TOPICS
OUTPUT_TABLES = OUTPUT_TOPICS
PLATFORM_TABLES = SOURCE_TABLES + OUTPUT_TABLES

CARD_FIELDS = {
    meta.KEY_GLOBAL_KEY_ID: ("AIOPS_FLINK_API_KEY", "AIOPS_FLINK_API_SECRET"),
    meta.KEY_KAFKA_KEY_ID: ("AIOPS_KAFKA_API_KEY", "AIOPS_KAFKA_API_SECRET"),
    meta.KEY_SR_KEY_ID: ("AIOPS_SR_API_KEY", "AIOPS_SR_API_SECRET"),
}
KEY_LABELS = {
    meta.KEY_GLOBAL_KEY_ID: "Global",
    meta.KEY_KAFKA_KEY_ID: "Kafka",
    meta.KEY_SR_KEY_ID: "Schema Registry",
}


# --- re-runs: reuse the card while its keys exist -----------------------------------


def _reusable_card(root: Path) -> Path | None:
    saved = meta.load_meta(root)
    ids = meta.recorded_key_ids(root)
    card_path = root / saved[meta.KEY_CARD] if saved.get(meta.KEY_CARD) else None
    if len(ids) < len(CARD_FIELDS) or card_path is None or not card_path.is_file():
        return None
    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    if any(card.get(key) != ids[field] or not card.get(secret) for field, (key, secret) in CARD_FIELDS.items()):
        return None
    print("\nChecking the keys on your credential card...")
    auth = (card["AIOPS_FLINK_API_KEY"], card["AIOPS_FLINK_API_SECRET"])
    try:
        # Global key first: if it's gone it can vouch for nothing else.
        alive = all(keys.key_exists(auth, ids[field]) for field in reversed(meta.KEY_ID_FIELDS))
    except requests.RequestException as e:
        print(f"\nError: could not reach the Confluent Cloud API to check them: {e}")
        sys.exit(1)
    if not alive:
        print("  At least one of them no longer exists — minting new keys.")
        return None
    return card_path


# --- minting -------------------------------------------------------------------------


def _record(root: Path, minted: dict[str, str], field: str, key_id: str) -> None:
    minted[field] = key_id
    meta.save_meta(root, **{field: key_id})


def _roll_back(root: Path, auth: tuple[str, str], minted: dict[str, str], old: dict[str, str]) -> None:
    print("\nDeleting the keys this run minted (nothing else was created)...")
    for field in meta.KEY_ID_FIELDS:
        key_id = minted.get(field)
        if key_id is None:
            continue
        if keys.delete_api_key(auth, key_id) == "failed":
            print(f"  Could not delete {KEY_LABELS[field]} key {key_id}: delete it under {KEY_LOCATION}.")
        else:
            print(f"  Deleted {KEY_LABELS[field]} key {key_id}.")
    meta.drop_meta(root, *minted)
    if old:
        meta.save_meta(root, **old)


def _delete_replaced(auth: tuple[str, str], old: dict[str, str], minted: dict[str, str]) -> None:
    stale = {field: key_id for field, key_id in old.items() if minted.get(field) != key_id}
    if not stale:
        return
    print("\nDeleting the keys your previous card carried...")
    for field, key_id in stale.items():
        result = keys.delete_api_key(auth, key_id)
        if result == "deleted":
            print(f"  Deleted {KEY_LABELS[field]} key {key_id}.")
        elif result == "failed":
            print(f"  Could not delete {KEY_LABELS[field]} key {key_id}: delete it under {KEY_LOCATION}.")


def _outputs(env, found, user_id, global_key, kafka_key, sr_key) -> dict:
    return {
        "organization_id": found.organization_id,
        "environment_id": env.id,
        "environment_name": env.name,
        "cluster_id": found.cluster_id,
        "cluster_name": found.cluster_name,
        "cluster_bootstrap": found.cluster_bootstrap,
        "compute_pool_id": found.compute_pool_id,
        "flink_rest_endpoint": found.flink_rest_endpoint,
        "flink_api_key": global_key[0],
        "flink_api_secret": global_key[1],
        "rtce_api_key": global_key[0],
        "rtce_api_secret": global_key[1],
        "attendee_credentials": {
            "environment_url": f"https://confluent.cloud/environments/{env.id}",
            "cluster_id": found.cluster_id,
            "cluster_bootstrap": found.cluster_bootstrap,
            "kafka_api_key": kafka_key[0],
            "kafka_api_secret": kafka_key[1],
            "schema_registry_url": found.schema_registry_url,
            "sr_api_key": sr_key[0],
            "sr_api_secret": sr_key[1],
        },
    }


def _mint(root: Path, saved: dict, old: dict) -> Path:
    print("\nMinting your Global key with the Confluent CLI...")
    auth = keys.mint_global_key(previous=old.get(meta.KEY_GLOBAL_KEY_ID, ""))
    minted: dict[str, str] = {}
    _record(root, minted, meta.KEY_GLOBAL_KEY_ID, auth[0])
    print(f"  Global key:   {auth[0]}")
    try:
        card_path = _build(root, auth, minted, saved)
    except requests.RequestException as e:
        _roll_back(root, auth, minted, old)
        print(f"\nError: could not reach the Confluent Cloud API: {e}")
        sys.exit(1)
    except BaseException:
        _roll_back(root, auth, minted, old)
        raise
    _delete_replaced(auth, old, minted)
    return card_path


def _build(root: Path, auth: tuple[str, str], minted: dict, saved: dict) -> Path:
    print("  Waiting for it to become active (a new key can take a couple of minutes)...")
    if not keys.wait_until(lambda: keys.key_exists(auth, auth[0])):
        print(f"\nError: your new Global key still isn't accepted after {keys.PROPAGATION_TIMEOUT}s. Re-run shortly.")
        sys.exit(1)

    print("\nLooking up your account and environment with the Global key...")
    user_id = lookup.resolve_user_id(*auth)
    envs = lookup.visible_environments(*auth)
    print(f"  User:         {user_id}")
    email = lookup.user_email(*auth, user_id) or "your-email@example.com"

    choice = lookup.choose_environment(envs, email)
    env = choice.env or _pick(choice.candidates)
    print(f"  Environment:  {env.name} ({env.id})")
    meta.save_meta(
        root,
        **{
            meta.KEY_RESOLVED_PREFIX: PREFIX,
            meta.KEY_ENVIRONMENT_ID: env.id,
            meta.KEY_ENVIRONMENT_NAME: env.name,
            meta.KEY_USER_ID: user_id,
        },
    )

    print(f"\nLooking for what your platform team built in {env.name}...")
    found = lookup.find_resources(*auth, env)
    meta.save_meta(root, **{meta.KEY_REGION: found.region})
    print(f"  Cluster:      {found.cluster_name} ({found.cluster_id}), {found.cloud} {found.region}")
    print(f"  Flink pool:   {lookup.POOL_NAME} ({found.compute_pool_id})")

    print("\nMinting your Kafka and Schema Registry keys...")
    kafka = keys.create_api_key(auth, user_id, found.cluster_id, env.id, "RIVER-OOPS-DEMO-kafka-key")
    _record(root, minted, meta.KEY_KAFKA_KEY_ID, kafka[0])
    sr = keys.create_api_key(auth, user_id, found.schema_registry_id, env.id, "RIVER-OOPS-DEMO-sr-key")
    _record(root, minted, meta.KEY_SR_KEY_ID, sr[0])
    print(f"  Kafka key:    {kafka[0]}")
    print(f"  SR key:       {sr[0]}")
    print("  Waiting for them to become active (up to 3 minutes)...")
    if not keys.wait_until(lambda: keys.kafka_key_works(found.cluster_rest_endpoint, found.cluster_id, *kafka)):
        print(f"\nError: your new Kafka key still can't list topics after {keys.PROPAGATION_TIMEOUT}s. Re-run shortly.")
        sys.exit(1)
    if not keys.wait_until(lambda: keys.schema_registry_key_works(found.schema_registry_url, *sr)):
        print(f"\nError: your new Schema Registry key still can't list subjects after {keys.PROPAGATION_TIMEOUT}s.")
        sys.exit(1)

    out = _outputs(env, found, user_id, auth, kafka, sr)
    cfg = {"prefix": PREFIX, "owner_email": email}
    return card_mod.write_card(root, cfg, out, region=found.region)


def _pick(candidates: tuple[lookup.Environment, ...]) -> lookup.Environment:
    print(f"\n  Your key sees {len(candidates)} environments:")
    for i, env in enumerate(candidates, 1):
        print(f"    {i}. {env.name}  ({env.id})")
    while True:
        choice = input(f"Which one is yours? (1-{len(candidates)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(candidates):
            return candidates[int(choice) - 1]
        print(f"  Please enter a number between 1 and {len(candidates)}.")


# --- readiness check -----------------------------------------------------------------


def _verify_ready(root: Path, card_path: Path) -> bool:
    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    print("\n=== Checking your environment is ready ===")
    session = FlinkSession(card)
    ok = _check_tables(session)
    try:
        if model_exists(session, MODEL_NAME):
            print(f"  ✓ model {MODEL_NAME}")
        else:
            ok = False
            print(f"  ✗ model {MODEL_NAME} is missing — ask your platform team to re-run their apply.")
    except (requests.RequestException, RuntimeError) as e:
        ok = False
        print(f"  ? could not check {MODEL_NAME}: {e}")
    return ok


def _check_tables(session: FlinkSession) -> bool:
    # One `SHOW TABLES` through the same Flink session the model check uses; a genuine
    # RBAC or network error surfaces as `?` rather than a bogus "missing" (D-020).
    try:
        tables = list_tables(session)
    except (requests.RequestException, RuntimeError) as e:
        for table in PLATFORM_TABLES:
            print(f"  ? could not check table {table}: {e}")
        return False
    ok = True
    for table in PLATFORM_TABLES:
        if table in tables:
            print(f"  ✓ table {table}")
        else:
            ok = False
            print(f"  ✗ table {table} is missing — ask your platform team to re-run their apply.")
    return ok


# --- entry ---------------------------------------------------------------------------


def deploy(args: argparse.Namespace, root: Path) -> None:
    print("\n=== Self-Healing IT workshop — pre-provisioned deploy ===")
    if not confluent_logged_in():
        print("\nThis track mints your Global key with the Confluent CLI, as you. Run")
        print("`confluent login` in this terminal (it opens your SSO sign-in), then re-run.")
        sys.exit(1)

    card_path = _reusable_card(root)
    if card_path is not None:
        print(f"  Reusing {card_path.relative_to(root)}: its keys all still exist, so this run creates none.")
        set_active_card(root, card_path)
    else:
        saved = meta.load_meta(root)
        old = meta.recorded_key_ids(root)
        card_path = _mint(root, saved, old)
        print(f"\nCredential card: {card_path.relative_to(root)}  (recorded as AIOPS_CARD in credentials.env)")

    ready = _verify_ready(root, card_path)

    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    print("\n=== Ready ===\n")
    print(f"Flink SQL Workspace:  https://confluent.cloud/environments/{card.get('AIOPS_ENVIRONMENT_ID', '')}/flink")
    print(f"  Catalog:  {card.get('AIOPS_CATALOG', '')}   (your environment)")
    print(f"  Database: {card.get('AIOPS_DATABASE', '')}")
    print("\nStart the generator and leave it running for the whole workshop:")
    print("  uv run ops-sim")
    print("\nTear down when finished:  uv run destroy")

    if ready and not args.automated:
        if confirm("\nStart the generator (uv run ops-sim) now?", default=False):
            import subprocess

            subprocess.Popen(["uv", "run", "ops-sim"], cwd=root)
            print("  Started `uv run ops-sim`.")

    if not ready:
        sys.exit(1)
