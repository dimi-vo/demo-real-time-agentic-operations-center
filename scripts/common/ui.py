"""Small console helpers: prompts and secret masking."""

from __future__ import annotations

import getpass

_SECRET_HINTS = ("SECRET", "PASSWORD", "TOKEN")


def prompt_with_default(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    answer = input(f"{prompt}{suffix}: ").strip()
    return answer or default


def prompt_secret(prompt: str) -> str:
    return getpass.getpass(f"{prompt}: ").strip()


def confirm(prompt: str, default: bool = True) -> bool:
    d = "y" if default else "n"
    answer = input(f"{prompt} ({'Y/n' if default else 'y/N'}) [{d}]: ").strip().lower()
    if not answer:
        return default
    return answer in ("y", "yes")


def is_secret_name(name: str) -> bool:
    upper = name.upper()
    return any(h in upper for h in _SECRET_HINTS) or upper.endswith("_API_KEY")


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return f"{'*' * (len(value) - 4)}{value[-4:]}"


def display_value(name: str, value: str) -> str:
    return mask_secret(value) if is_secret_name(name) else value
