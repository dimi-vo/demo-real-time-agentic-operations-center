"""`uv run destroy` — delete the three API keys this deploy minted, and the card.

It deletes only the developer's own keys (Global last, since a Global key authenticates
its own deletion) and the local credential card. The environment and everything in it
belong to the platform team and stay.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import dotenv_values

from scripts.common import deployment_meta as meta
from scripts.common.credentials import clear_active_card, resolve_card
from scripts.common.paths import get_project_root
from scripts.common.ui import confirm
from scripts.preprovisioned import keys

KEY_LOCATION = "Console → API keys → My account"
KEY_LABELS = {
    meta.KEY_KAFKA_KEY_ID: "Kafka",
    meta.KEY_SR_KEY_ID: "Schema Registry",
    meta.KEY_GLOBAL_KEY_ID: "Global",
}


def _global_auth(root: Path, ids: dict[str, str]) -> tuple[str, str] | None:
    """The Global key + secret from the card, to authenticate the deletions."""
    try:
        card_path = resolve_card(None, root)
    except SystemExit:
        return None
    card = {k: v or "" for k, v in dotenv_values(card_path).items()}
    key, secret = card.get("AIOPS_FLINK_API_KEY", ""), card.get("AIOPS_FLINK_API_SECRET", "")
    if key and secret and key == ids.get(meta.KEY_GLOBAL_KEY_ID):
        return key, secret
    return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Delete the developer's API keys and credential card")
    parser.add_argument("--yes", "--force", dest="yes", action="store_true", help="Don't prompt for confirmation")
    args = parser.parse_args(argv)

    root = get_project_root()
    ids = meta.recorded_key_ids(root)
    saved = meta.load_meta(root)
    if not ids:
        print("Nothing to destroy: no keys recorded in runs/pre-provisioned/deployment.env.")
        return

    env = saved.get(meta.KEY_ENVIRONMENT_NAME) or "your environment"
    print(f"This deletes these API keys (your {env} and everything in it stay):")
    for field in meta.KEY_ID_FIELDS:
        if ids.get(field):
            print(f"  - {KEY_LABELS[field]}: {ids[field]}")
    if not args.yes and not confirm("Delete them?", default=True):
        print("Cancelled.")
        return

    auth = _global_auth(root, ids)
    if auth is None:
        print("\nCouldn't read your Global key from the card, so I can't delete the keys for you.")
        print(f"Delete them yourself under {KEY_LOCATION}:")
        for field in meta.KEY_ID_FIELDS:
            if ids.get(field):
                print(f"  - {KEY_LABELS[field]}: {ids[field]}")
        sys.exit(1)

    failed = []
    for field in meta.KEY_ID_FIELDS:  # Global is last in KEY_ID_FIELDS
        key_id = ids.get(field)
        if not key_id:
            continue
        result = keys.delete_api_key(auth, key_id)
        if result == "failed":
            failed.append((field, key_id))
            print(f"  Could not delete {KEY_LABELS[field]} key {key_id}: delete it under {KEY_LOCATION}.")
        else:
            print(f"  Deleted {KEY_LABELS[field]} key {key_id} ({result}).")
        meta.drop_meta(root, field)

    # Remove the card files and the pointer.
    card_rel = saved.get(meta.KEY_CARD)
    if card_rel:
        card = root / card_rel
        for p in (card, card.with_suffix(".md")):
            p.unlink(missing_ok=True)
    clear_active_card(root)
    if not failed:
        meta.clear_meta(root)
        print("\nDone. Your keys and card are gone; your environment stays.")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
