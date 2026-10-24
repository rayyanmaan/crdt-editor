"""Breaking the network on purpose and checking the system recovers.

These start the real run.py: three replica processes, and every message
between them goes through the fault-injection proxy. The checks are the
strict ones from test_cluster_text.py: every edit made is on every replica,
character by character, with nothing waiting in a buffer.
"""

import time

import pytest

from cluster import Cluster, wait_until
from config import REPLICA_IDS
from test_cluster_text import Edits, everyone_has, text_of
from test_supervisor import run_py, supervisor  # noqa: F401 (run_py is a fixture)


@pytest.fixture
def net(run_py, tmp_path):  # noqa: F811
    """Talk to the replicas run.py started (Cluster here is only used for its
    command helpers; it starts nothing)."""
    replicas = Cluster(str(tmp_path))
    wait_until(lambda: all(
        all(p["status"] == "up" for p in replicas.state(r)["peers"].values()) for r in REPLICA_IDS
    ))
    return replicas


def links():
    return supervisor({"type": "status"})["links"]


def set_link(src, dst, **settings):
    reply = supervisor({"type": "link", "from": src, "to": dst, "settings": settings})
    assert reply["type"] == "status", reply


def set_all_links(**settings):
    for name in links():
        src, dst = name.split(">")
        set_link(src, dst, **settings)


def partition(*groups):
    reply = supervisor({"type": "partition", "groups": [list(g) for g in groups]})
    assert reply["type"] == "status", reply


def heal():
    assert supervisor({"type": "heal"})["type"] == "status"


def peer_view(c, rid, peer):
    return c.state(rid)["peers"][peer]["status"]


def test_all_traffic_goes_through_the_proxy(net):
    wait_until(lambda: all(link["passed"] > 0 for link in links().values()))


def test_partition_both_sides_edit_the_same_sentence_then_heal(net):
    edits = Edits()
    edits.record(net.type("A", 0, "The cat sat."))
    wait_until(lambda: everyone_has(net, edits))

    partition("A", "BC")
    # Nothing gets through, so after a few seconds of silence each side
    # suspects the other. Neither side was told: the links just went quiet.
    wait_until(lambda: peer_view(net, "A", "B") == "suspected" and peer_view(net, "B", "A") == "suspected", timeout=8)
    assert peer_view(net, "B", "C") == "up"

    # Both sides edit the same sentence at the same time.
    edits.record(net.type("A", 4, "black "))  # The black cat sat.
    edits.record(net.type("B", 4, "fat "))    # The fat cat sat.
    # C deletes the full stop. ("last", not position 11: B's "fat " may
    # already have reached C, which would move the full stop along.)
    edits.record(net.delete("C", "last", 1))
    wait_until(lambda: text_of(net, "B") == text_of(net, "C") == "The fat cat sat")
    time.sleep(1.5)  # long enough for a sync and repair, if anything got through
    assert text_of(net, "A") == "The black cat sat."  # A sees only its own side

    heal()
    wait_until(lambda: everyone_has(net, edits))
    text = text_of(net, "A")
    assert "black " in text and "fat " in text and not text.endswith(".")
    assert peer_view(net, "A", "B") == "up"


def test_lost_messages_are_repaired_with_no_further_edits(net):
    # Everything sent to B silently vanishes, while the links stay "up".
    set_link("A", "B", drop=1.0)
    set_link("C", "B", drop=1.0)
    edits = Edits()
    edits.record(net.type("A", 0, "lost on the way"))
    wait_until(lambda: everyone_has(net, edits, replicas=["A", "C"]))
    time.sleep(1.5)
    assert text_of(net, "B") == ""

    # Nobody types anything more. Repair alone has to bring B up to date.
    heal()
    wait_until(lambda: everyone_has(net, edits))


def test_half_of_all_messages_lost_still_converges(net):
    set_all_links(drop=0.5)
    edits = Edits()
    for rid, word in [("A", "one "), ("B", "two "), ("C", "three ")]:
        edits.record(net.type(rid, "end", word))
    wait_until(lambda: everyone_has(net, edits), timeout=30)
    # Messages really were lost along the way. (Checked in total: a link
    # that only carried a few messages before everything converged can
    # pass them all by chance, about 1 time in 8 for 3 messages.)
    assert sum(link["dropped"] for link in links().values()) > 5


def test_every_message_duplicated_changes_nothing(net):
    set_all_links(duplicate=1.0)
    edits = Edits()
    edits.record(net.type("A", 0, "twice"))
    edits.record(net.delete("A", 0, 1))
    edits.record(net.type("B", "end", "!"))
    wait_until(lambda: everyone_has(net, edits))
    assert sum(link["duplicated"] for link in links().values()) > 0


def test_reordered_messages_still_converge(net):
    set_all_links(reorder_ms=400)
    edits = Edits()
    # Many separate small edits, so many separate messages to shuffle.
    for i in range(15):
        edits.record(net.type("A", "end", "abc"[i % 3]))
        edits.record(net.type("B", 0, str(i % 10)))
    wait_until(lambda: everyone_has(net, edits), timeout=20)
    assert sum(link["reordered"] for link in links().values()) > 0


def test_one_way_cut_edits_still_arrive_the_long_way_round(net):
    # A can't send to B, but B can still send to A.
    set_link("A", "B", cut=True)
    wait_until(lambda: peer_view(net, "B", "A") == "suspected", timeout=8)
    assert peer_view(net, "A", "B") == "up"  # A still hears B fine

    # A's edits reach B through C: C has them and repairs B.
    edits = Edits()
    edits.record(net.type("A", 0, "via C"))
    wait_until(lambda: everyone_has(net, edits))


def test_set_concurrent_remove_and_add_during_a_partition_add_wins(net):
    net.add("A", "x")
    wait_until(lambda: all(net.state(r)["set"] == ["x"] for r in REPLICA_IDS))

    partition("A", "BC")
    wait_until(lambda: peer_view(net, "A", "B") == "suspected", timeout=8)
    net.remove("A", "x")  # A removes x...
    net.add("B", "x")     # ...while B, cut off, adds it again
    net.increment("A", 2)
    net.increment("C", 3)
    assert net.state("A")["set"] == []

    heal()
    # B's add has a tag A never saw, so A's remove didn't cover it: add wins.
    wait_until(lambda: all(net.state(r)["set"] == ["x"] for r in REPLICA_IDS))
    wait_until(lambda: all(net.state(r)["counter"] == 5 for r in REPLICA_IDS))


def test_restart_during_a_partition_then_heal(net):
    edits = Edits()
    edits.record(net.type("A", 0, "start "))
    wait_until(lambda: everyone_has(net, edits))

    partition("A", "BC")
    edits.record(net.type("A", "end", "A-side "))
    supervisor({"type": "kill", "replica": "C"})
    edits.record(net.type("B", "end", "B-side "))
    supervisor({"type": "start", "replica": "C"})
    wait_until(lambda: net.state("C"))
    edits.record(net.type("C", "end", "C-new "))

    heal()
    wait_until(lambda: everyone_has(net, edits))


@pytest.mark.parametrize(
    "command",
    [
        {"type": "link", "from": "A", "to": "A", "settings": {"cut": True}},
        {"type": "link", "from": "A", "to": "B", "settings": {"drop": 2}},
        {"type": "link", "from": "A", "to": "B"},
        {"type": "partition", "groups": [["A"], ["B"]]},
        {"type": "partition"},
    ],
)
def test_bad_network_commands_are_refused(net, command):
    assert supervisor(command)["type"] == "error"
