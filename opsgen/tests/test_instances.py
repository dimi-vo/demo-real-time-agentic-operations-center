"""Instance naming (section 4.2): stable across restarts, changes on deploy, restores
on rollback."""

from opsgen.instances import instance_id, instance_ids, replica_suffix, version_hash


def test_version_hash_is_six_hex():
    h = version_hash("checkout", "4.2.1")
    assert len(h) == 6
    assert all(c in "0123456789abcdef" for c in h)


def test_version_hash_changes_with_version_and_restores():
    base = version_hash("checkout", "4.2.1")
    deployed = version_hash("checkout", "4.2.2")
    assert base != deployed
    # a rollback restores the old version string -> the old hash comes back
    assert version_hash("checkout", "4.2.1") == base


def test_version_hash_differs_by_service():
    assert version_hash("checkout", "1.0.0") != version_hash("payments", "1.0.0")


def test_replica_suffix_is_five_alphanumeric_and_deterministic():
    s1 = replica_suffix("checkout", "4.2.1", 0)
    s2 = replica_suffix("checkout", "4.2.1", 0)
    assert s1 == s2
    assert len(s1) == 5
    assert all(c in "0123456789abcdefghijklmnopqrstuvwxyz" for c in s1)
    assert replica_suffix("checkout", "4.2.1", 0) != replica_suffix("checkout", "4.2.1", 1)


def test_instance_id_shape():
    iid = instance_id("checkout", "4.2.1", 0)
    parts = iid.split("-")
    assert parts[0] == "checkout"
    assert parts[1] == version_hash("checkout", "4.2.1")
    assert len(parts[2]) == 5


def test_instance_ids_count_matches_replicas():
    ids = instance_ids("checkout", "4.2.1", 3)
    assert len(ids) == 3
    assert len(set(ids)) == 3  # distinct suffixes
    assert all(i.startswith("checkout-" + version_hash("checkout", "4.2.1")) for i in ids)
