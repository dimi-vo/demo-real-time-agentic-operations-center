"""Version and rollback semantics (section 4.3)."""

from opsgen.deployments import DEPLOY, INITIAL, ROLLBACK, DeploymentState, next_patch_version
from opsgen.services import METRIC_SERVICES_BY_NAME


def test_next_patch_version():
    assert next_patch_version("3.4.2") == "3.4.3"
    assert next_patch_version("1.8.0") == "1.8.1"
    assert next_patch_version("2.9.4") == "2.9.5"


def _payments_state(now=0.0):
    return DeploymentState.initial(METRIC_SERVICES_BY_NAME["payments"], now)


def test_initial_state():
    s = _payments_state(100.0)
    assert s.version == "2.9.4"
    assert s.previous_version is None
    assert s.last_change == INITIAL
    assert s.deployed_at == 100.0
    assert s.desired_replicas == s.ready_replicas == 2
    assert s.tier == "CRITICAL"


def test_deploy_increments_patch_and_sets_previous():
    s = _payments_state()
    old, new = s.deploy(50.0)
    assert (old, new) == ("2.9.4", "2.9.5")
    assert s.version == "2.9.5"
    assert s.previous_version == "2.9.4"
    assert s.last_change == DEPLOY
    assert s.deployed_at == 50.0


def test_rollback_restores_prior_and_swaps_previous():
    s = _payments_state()
    s.deploy(50.0)  # 2.9.4 -> 2.9.5, prev 2.9.4
    rolled_from, restored_to = s.rollback(90.0)
    assert (rolled_from, restored_to) == ("2.9.5", "2.9.4")
    assert s.version == "2.9.4"
    assert s.previous_version == "2.9.5"
    assert s.last_change == ROLLBACK
    assert s.deployed_at == 90.0


def test_deploy_then_deploy_then_rollback():
    s = _payments_state()
    s.deploy(10.0)  # -> 2.9.5 (prev 2.9.4)
    s.deploy(20.0)  # -> 2.9.6 (prev 2.9.5)
    assert s.version == "2.9.6" and s.previous_version == "2.9.5"
    s.rollback(30.0)  # -> 2.9.5 (prev 2.9.6)
    assert s.version == "2.9.5" and s.previous_version == "2.9.6"


def test_a_deploy_after_a_rollback_skips_the_rolled_back_version():
    # D-067: a version number is never reused. The bad 2.9.6 stays bad; the next release is 2.9.7.
    s = _payments_state()
    s.deploy(10.0)  # -> 2.9.5
    s.deploy(20.0)  # -> 2.9.6 (bad)
    s.rollback(30.0)  # -> 2.9.5
    old, new = s.deploy(40.0)
    assert (old, new) == ("2.9.5", "2.9.7")
    assert s.previous_version == "2.9.5"  # a rollback of 2.9.7 goes back to what was running
    s.rollback(50.0)
    assert s.version == "2.9.5"


def test_versions_never_repeat_across_deploys_and_rollbacks():
    s = _payments_state()
    seen = [s.version]
    for k, step in enumerate("DRDDRDRRD"):  # R only when there is a previous version to return to
        if step == "D":
            seen.append(s.deploy(float(k))[1])
        elif s.previous_version is not None:
            s.rollback(float(k))
    assert len(seen) == len(set(seen))


def test_highest_version_sorts_numerically():
    s = _payments_state()
    s.version = s.highest_version = "2.9.9"
    assert s.deploy(1.0)[1] == "2.9.10"
    s.rollback(2.0)
    assert s.deploy(3.0)[1] == "2.9.11"  # 2.9.10 > 2.9.9, not lexicographic
