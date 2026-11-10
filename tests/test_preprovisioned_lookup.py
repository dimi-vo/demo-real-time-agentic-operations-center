"""The environment-name sanitizer and the pure environment-choice logic.

SANITIZER_EXAMPLES must stay identical to the examples in
platform-team/tests/naming.tftest.hcl (the HCL twin). If you change one, change both."""

import pytest

from scripts.preprovisioned.lookup import (
    Environment,
    NoEnvironmentError,
    choose_environment,
    sanitize_local_part,
)

# (email, expected stem). Keep in step with naming.tftest.hcl.
SANITIZER_EXAMPLES = [
    ("John.Doe@example.com", "John-Doe"),
    ("jane_smith@example.com", "jane_smith"),
    ("weird++name@example.com", "weird-name"),
]


@pytest.mark.parametrize(("email", "stem"), SANITIZER_EXAMPLES)
def test_sanitize_local_part(email, stem):
    assert sanitize_local_part(email) == stem


def test_sanitize_truncates_to_40():
    email = "a.very.long.local.part.that.exceeds.forty.characters.indeed@example.com"
    assert len(sanitize_local_part(email)) <= 40


def test_sanitize_case_preserved():
    assert sanitize_local_part("MixedCase@x.com") == "MixedCase"


def test_sanitize_rejects_empty():
    with pytest.raises(ValueError):
        sanitize_local_part("+++@example.com")


def test_choose_single_environment():
    env = Environment("env-1", "jane-doe-7f3a_env")
    assert choose_environment([env], "jane.doe@x.com").env == env


def test_choose_by_name_among_several():
    envs = [
        Environment("env-1", "someone-else-0000_env"),
        Environment("env-2", "jane-doe-7f3a_env"),
        Environment("env-3", "another-1111_env"),
    ]
    choice = choose_environment(envs, "jane.doe@example.com")
    assert choice.env == envs[1]


def test_choose_ambiguous_returns_candidates():
    envs = [Environment("env-1", "alpha_env"), Environment("env-2", "beta_env")]
    choice = choose_environment(envs, "nomatch@example.com")
    assert choice.env is None
    assert len(choice.candidates) == 2


def test_choose_none_raises():
    with pytest.raises(NoEnvironmentError):
        choose_environment([], "jane.doe@x.com")
