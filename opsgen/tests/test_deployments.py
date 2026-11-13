"""Version and rollback semantics (section 4.3)."""

from opsgen.deployments import DEPLOY, INITIAL, ROLLBACK, DeploymentState, next_patch_version
from opsgen.services import SERVICES_BY_NAME


def test_next_patch_version():
    assert next_patch_version("3.4.2") == "3.4.3"
    assert next_patch_version("1.8.0") == "1.8.1"
    assert next_patch_version("2.9.4") == "2.9.5"


def _checkout_state(now=0.0):
    return DeploymentState.initial(SERVICES_BY_NAME["checkout"], now)


def test_initial_state():
    s = _checkout_state(100.0)
    assert s.version == "4.2.1"
    assert s.previous_version is None
    assert s.last_change == INITIAL
    assert s.deployed_at == 100.0
    assert s.desired_replicas == s.ready_replicas == 3
    assert s.tier == "CRITICAL"


def test_deploy_increments_patch_and_sets_previous():
    s = _checkout_state()
    old, new = s.deploy(50.0)
    assert (old, new) == ("4.2.1", "4.2.2")
    assert s.version == "4.2.2"
    assert s.previous_version == "4.2.1"
    assert s.last_change == DEPLOY
    assert s.deployed_at == 50.0


def test_rollback_restores_prior_and_swaps_previous():
    s = _checkout_state()
    s.deploy(50.0)  # 4.2.1 -> 4.2.2, prev 4.2.1
    rolled_from, restored_to = s.rollback(90.0)
    assert (rolled_from, restored_to) == ("4.2.2", "4.2.1")
    assert s.version == "4.2.1"
    assert s.previous_version == "4.2.2"
    assert s.last_change == ROLLBACK
    assert s.deployed_at == 90.0


def test_deploy_then_deploy_then_rollback():
    s = _checkout_state()
    s.deploy(10.0)  # -> 4.2.2 (prev 4.2.1)
    s.deploy(20.0)  # -> 4.2.3 (prev 4.2.2)
    assert s.version == "4.2.3" and s.previous_version == "4.2.2"
    s.rollback(30.0)  # -> 4.2.2 (prev 4.2.3)
    assert s.version == "4.2.2" and s.previous_version == "4.2.3"
