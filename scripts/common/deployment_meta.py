"""The deployment record: runs/pre-provisioned/deployment.env.

This single-track workshop records the developer's deployment as a flat KEY=VALUE
file. It holds the resolved environment and user, the region, the card path, and the
IDs of the three keys the deploy minted (saved the moment each key exists). That
record *is* the deployment: `uv run destroy` deletes exactly the keys it names, and a
re-run of `uv run deploy` reuses the card when its keys all still exist.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

from scripts.common.paths import run_dir

DEPLOYMENT_ENV = "deployment.env"

KEY_RESOLVED_PREFIX = "AIOPS_RESOLVED_PREFIX"
KEY_ENVIRONMENT_ID = "AIOPS_ENVIRONMENT_ID"
KEY_ENVIRONMENT_NAME = "AIOPS_ENVIRONMENT_NAME"
KEY_USER_ID = "AIOPS_USER_ID"
KEY_REGION = "AIOPS_REGION"
KEY_CARD = "AIOPS_CARD_PATH"
KEY_GLOBAL_KEY_ID = "AIOPS_GLOBAL_KEY_ID"
KEY_KAFKA_KEY_ID = "AIOPS_KAFKA_KEY_ID"
KEY_SR_KEY_ID = "AIOPS_SR_KEY_ID"

# Global last: it can authenticate its own deletion, so it is deleted after the
# others (see destroy / provision roll-back).
KEY_ID_FIELDS = (KEY_KAFKA_KEY_ID, KEY_SR_KEY_ID, KEY_GLOBAL_KEY_ID)


def meta_path(root: Path) -> Path:
    return run_dir(root) / DEPLOYMENT_ENV


def load_meta(root: Path) -> dict[str, str]:
    path = meta_path(root)
    if not path.is_file():
        return {}
    return {k: v for k, v in dotenv_values(path).items() if v is not None}


def save_meta(root: Path, **values: str) -> None:
    """Merge the given keys into deployment.env (creating it if needed)."""
    path = meta_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    current = load_meta(root)
    current.update({k: str(v) for k, v in values.items()})
    _write(path, current)


def drop_meta(root: Path, *keys: str) -> None:
    path = meta_path(root)
    if not path.is_file():
        return
    current = load_meta(root)
    for k in keys:
        current.pop(k, None)
    _write(path, current)


def clear_meta(root: Path) -> None:
    meta_path(root).unlink(missing_ok=True)


def recorded_key_ids(root: Path) -> dict[str, str]:
    """The key-ID fields present in deployment.env (field -> key id)."""
    meta = load_meta(root)
    return {field: meta[field] for field in KEY_ID_FIELDS if meta.get(field)}


def is_deployed(root: Path) -> bool:
    return bool(recorded_key_ids(root))


def _write(path: Path, values: dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in values.items()]
    path.write_text("\n".join(lines) + ("\n" if lines else ""))
