"""Resolve and load the developer's credential card.

The card is `runs/pre-provisioned/credentials/DEMO.env` (a dotenv file of `AIOPS_*`
values). `credentials.env` at the repo root holds a single pointer, `AIOPS_CARD`, to
the active card, so the tools (`ops-sim`, `ops-wall`, `reset`, `setup-rtce`) find it
without a flag. An explicit `--creds` path always wins.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

from scripts.common.paths import credentials_dir, get_project_root

CREDENTIALS_ENV = "credentials.env"
CARD_POINTER = "AIOPS_CARD"


def credentials_env_path(root: Path | None = None) -> Path:
    return (root or get_project_root()) / CREDENTIALS_ENV


def _pointer(root: Path) -> str | None:
    path = credentials_env_path(root)
    if not path.is_file():
        return None
    return (dotenv_values(path).get(CARD_POINTER) or "").strip() or None


def set_active_card(root: Path, card_path: Path) -> None:
    """Point credentials.env's AIOPS_CARD at `card_path` (relative to the repo root)."""
    path = credentials_env_path(root)
    rel = str(Path(card_path).resolve().relative_to(Path(root).resolve()))
    lines = []
    if path.is_file():
        lines = [ln for ln in path.read_text().splitlines() if not ln.startswith(f"{CARD_POINTER}=")]
    lines.append(f"{CARD_POINTER}={rel}")
    path.write_text("\n".join(lines) + "\n")


def clear_active_card(root: Path) -> None:
    path = credentials_env_path(root)
    if not path.is_file():
        return
    lines = [ln for ln in path.read_text().splitlines() if not ln.startswith(f"{CARD_POINTER}=")]
    path.write_text("\n".join(lines) + ("\n" if lines else ""))


def resolve_card(explicit: str | None = None, root: Path | None = None) -> Path:
    """The card path: an explicit --creds, else the AIOPS_CARD pointer, else the single
    card under runs/pre-provisioned/credentials/. Raises SystemExit otherwise."""
    root = root or get_project_root()
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise SystemExit(f"No credential card at {p}.")
        return p
    pointer = _pointer(root)
    if pointer:
        p = (root / pointer) if not Path(pointer).is_absolute() else Path(pointer)
        if p.is_file():
            return p
    cards = sorted(credentials_dir(root).glob("*.env")) if credentials_dir(root).is_dir() else []
    if len(cards) == 1:
        return cards[0]
    if not cards:
        raise SystemExit(
            "No credential card found. Run `uv run deploy --track pre-provisioned`, "
            "or pass --creds <path>."
        )
    raise SystemExit(
        "Multiple credential cards found; pass --creds <path> or set AIOPS_CARD in credentials.env:\n  "
        + "\n  ".join(str(c) for c in cards)
    )


def load_card(explicit: str | None = None, root: Path | None = None) -> tuple[Path, dict[str, str]]:
    path = resolve_card(explicit, root)
    card = {k: (v or "") for k, v in dotenv_values(path).items()}
    return path, card
