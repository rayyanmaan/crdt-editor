"""The OR-Set running on three real replica processes.

Each test works out the exact internal state it expects (every tag and every
tombstone) from the replies to its own commands, and checks every replica
ends up with exactly that. Comparing replicas with each other wouldn't be
enough: they could all have lost the same add.

What these tests can't do yet is force a truly concurrent add and remove
across the network; that needs the fault proxy to hold messages back. The
unit tests in test_orset.py cover concurrency on the data structure itself.
"""

from cluster import wait_until
from config import REPLICA_IDS


def set_state_everywhere(cluster, adds, removed):
    expected = {
        "adds": {e: sorted(tags) for e, tags in sorted(adds.items())},
        "removed": sorted(removed),
    }
    return all(cluster.state(rid)["set_state"] == expected for rid in REPLICA_IDS)


def test_adds_and_removes_reach_every_replica(cluster):
    adds = {}
    for rid, element in [("A", "apple"), ("B", "banana"), ("C", "cherry")]:
        adds.setdefault(element, set()).add(cluster.add(rid, element)["tag"])
    wait_until(lambda: set_state_everywhere(cluster, adds, set()))

    # B removes the apple A added, once it has seen it.
    removed = set(cluster.remove("B", "apple")["tombstoned"])
    assert removed == adds["apple"]

    wait_until(lambda: set_state_everywhere(cluster, adds, removed))
    assert all(cluster.state(rid)["set"] == ["banana", "cherry"] for rid in REPLICA_IDS)


def test_same_element_added_on_two_replicas_needs_both_tags_removed(cluster):
    adds = {"x": {cluster.add("A", "x")["tag"], cluster.add("C", "x")["tag"]}}
    wait_until(lambda: set_state_everywhere(cluster, adds, set()))

    removed = set(cluster.remove("B", "x")["tombstoned"])
    assert removed == adds["x"]  # B had seen both adds, so it removes both

    wait_until(lambda: set_state_everywhere(cluster, adds, removed))
    assert all("x" not in cluster.state(rid)["set"] for rid in REPLICA_IDS)


def test_restarted_replica_gets_the_set_back_and_its_new_tags_are_unique(cluster):
    old_tag = cluster.add("B", "before")["tag"]
    adds = {"before": {old_tag}}
    wait_until(lambda: set_state_everywhere(cluster, adds, set()))

    cluster.kill("B")
    cluster.start("B")
    wait_until(lambda: cluster.state("B"))

    # The restarted B counts its tags from 1 again, but its incarnation is
    # new, so the tag can't clash with the one it made before the crash.
    new_tag = cluster.add("B", "after")["tag"]
    assert new_tag != old_tag
    adds["after"] = {new_tag}

    wait_until(lambda: set_state_everywhere(cluster, adds, set()))


def test_removed_element_stays_removed_after_the_remover_crashes(cluster):
    adds = {"x": {cluster.add("A", "x")["tag"]}}
    wait_until(lambda: set_state_everywhere(cluster, adds, set()))

    removed = set(cluster.remove("C", "x")["tombstoned"])
    wait_until(lambda: set_state_everywhere(cluster, adds, removed))

    # C crashes and comes back empty. The tombstone lives on in A and B, so
    # the restarted C learns "x" is removed rather than bringing it back.
    cluster.kill("C")
    cluster.start("C")
    wait_until(lambda: set_state_everywhere(cluster, adds, removed))
    assert all("x" not in cluster.state(rid)["set"] for rid in REPLICA_IDS)
