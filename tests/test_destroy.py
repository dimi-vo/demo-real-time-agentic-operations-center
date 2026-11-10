"""destroy's safe paths: nothing-deployed is a clean no-op, and Global is deleted last."""

from scripts.common import deployment_meta as meta
from scripts.common import destroy


def test_nothing_to_destroy_is_a_clean_noop(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(destroy, "get_project_root", lambda: tmp_path)
    destroy.main([])  # no deployment.env -> returns without touching anything
    assert "Nothing to destroy" in capsys.readouterr().out


def test_global_key_is_deleted_last():
    # A Global key authenticates its own deletion, so it must be last in the order.
    assert meta.KEY_ID_FIELDS[-1] == meta.KEY_GLOBAL_KEY_ID
