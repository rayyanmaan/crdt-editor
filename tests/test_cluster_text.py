"""The shared text on three real replica processes.

Every test collects the ops each edit produced (the replies include them)
and checks each replica's whole document against them: one node per insert,
the right character, exactly the deleted ones tombstoned. Matching text
alone could hide an edit that every replica lost.
"""

import json
import time

import pytest
from websockets.sync.client import connect

from cluster import Cluster, wait_until
from config import HOST, REPLICA_IDS
from conftest import all_links_up
from test_rga import check_nothing_lost


class Edits:
    """Every op the test's edits produced, to check replicas against."""

    def __init__(self):
        self.envelopes = []

    def record(self, reply):
        assert reply["type"] == "state", reply
        self.envelopes += reply["ops"]
        return reply

    def ops(self):
        return [e["op"] for e in self.envelopes]


def everyone_has(cluster, edits, replicas=REPLICA_IDS):
    """True once every replica holds exactly these edits, with nothing waiting."""
    docs = []
    for rid in replicas:
        state = cluster.full_state(rid)
        if state["text"]["buffered"]:
            return False
        docs.append(state["text_doc"])
    if any(doc != docs[0] for doc in docs):
        return False
    try:
        check_nothing_lost(docs[0], edits.ops())
    except AssertionError:
        return False
    return True


def text_of(cluster, rid):
    return cluster.state(rid)["text"]["text"]


def test_typing_on_every_replica_reaches_every_replica(cluster):
    edits = Edits()
    edits.record(cluster.type("A", 0, "hello"))
    edits.record(cluster.type("B", 0, "world"))  # concurrent with A's, same spot
    edits.record(cluster.type("C", 0, "!"))
    wait_until(lambda: everyone_has(cluster, edits))

    text = text_of(cluster, "A")
    # Each word was typed left to right, so RGA keeps each one in one piece.
    assert "hello" in text and "world" in text and "!" in text and len(text) == 11

    edits.record(cluster.delete("B", 0, count=3))
    wait_until(lambda: everyone_has(cluster, edits))
    assert len(text_of(cluster, "C")) == 8


def test_restarted_replica_catches_up_and_types_right_away(cluster):
    edits = Edits()
    edits.record(cluster.type("B", 0, "before "))
    wait_until(lambda: everyone_has(cluster, edits))

    cluster.kill("B")
    edits.record(cluster.type("A", len(text_of(cluster, "A")), "while-down "))
    edits.record(cluster.type("C", 0, ">"))

    # B comes back empty with a new incarnation, so a new origin for its ops,
    # and types before it has caught up with anything.
    cluster.start("B")
    wait_until(lambda: cluster.state("B"))
    reply = edits.record(cluster.type("B", 0, "new"))
    assert reply["ops"][0]["origin"] != edits.envelopes[0]["origin"]

    wait_until(lambda: everyone_has(cluster, edits))


def test_ops_from_a_dead_replica_are_recovered_from_a_third(cluster):
    edits = Edits()
    edits.record(cluster.type("A", 0, "only A typed this"))
    wait_until(lambda: everyone_has(cluster, edits))

    # A and C both crash. C comes back; A doesn't. A's ops now exist only on
    # B, so C can only get them if B sends ops from other origins too.
    cluster.kill("A")
    cluster.kill("C")
    cluster.start("C")
    wait_until(lambda: everyone_has(cluster, edits, replicas=["B", "C"]))
    assert text_of(cluster, "C") == "only A typed this"


def fake_peer_sends(cluster, to, envelopes):
    """Send ops to a replica as if they came from replica B."""
    b_inc = cluster.state("B")["incarnation"]
    with connect(f"ws://{HOST}:{cluster.port(to)}") as ws:
        ws.send(json.dumps({"v": 1, "type": "ops", "from": "B", "inc": b_inc, "ops": envelopes}))


def h_then_i():
    """Two ops from an origin that isn't one of our replicas: "h", then "i" after it."""
    h = {"origin": "X#1", "seq": 1, "op": {"kind": "insert", "id": [1, "X#1"], "after": None, "char": "h"}}
    i = {"origin": "X#1", "seq": 2, "op": {"kind": "insert", "id": [2, "X#1"], "after": [1, "X#1"], "char": "i"}}
    return h, i


def test_op_that_arrives_early_waits_in_the_buffer(cluster):
    h, i = h_then_i()
    fake_peer_sends(cluster, "A", [i])  # "i" before the "h" it goes after
    wait_until(lambda: cluster.state("A")["text"]["buffered"] == ["X#1:2"])
    assert text_of(cluster, "A") == ""

    fake_peer_sends(cluster, "A", [h])
    wait_until(lambda: text_of(cluster, "A") == "hi")
    assert cluster.state("A")["text"]["buffered"] == []

    # Only A was sent these ops. B and C get them through repair.
    wait_until(lambda: all(text_of(cluster, r) == "hi" for r in REPLICA_IDS))


@pytest.fixture
def cluster_without_buffer(tmp_path):
    c = Cluster(str(tmp_path), extra_env={"CRDT_CAUSAL_BUFFER": "off"})
    try:
        c.start_all()
        wait_until(lambda: all_links_up(c))
        yield c
    finally:
        c.stop_all()


def test_without_the_causal_buffer_replicas_diverge_for_good(cluster_without_buffer):
    # The experiment from the proposal, over the real network.
    c = cluster_without_buffer
    h, i = h_then_i()
    fake_peer_sends(c, "A", [i])  # A can't place "i" and guesses
    fake_peer_sends(c, "A", [h])

    # B and C get both ops from A's repair, in order, so they're right.
    wait_until(lambda: text_of(c, "B") == "hi" and text_of(c, "C") == "hi")
    # A guessed wrong and nothing ever corrects it: the ops are already in
    # its log, so repair has nothing to send it.
    assert text_of(c, "A") == "ih"
    wait_until(lambda: c.state("A")["text"]["applied"] == {"X#1": 2})
    time.sleep(2.5)  # two more rounds of sync and repair
    assert text_of(c, "A") == "ih"


@pytest.mark.parametrize(
    "command",
    [
        {"type": "insert", "index": 5, "text": "x"},  # past the end
        {"type": "insert", "index": -1, "text": "x"},
        {"type": "insert", "index": 0, "text": ""},
        {"type": "insert", "index": 0, "text": "x" * 1001},
        {"type": "insert", "index": 0, "text": "ok\ud800"},  # a lone surrogate
        {"type": "insert", "index": True, "text": "x"},
        {"type": "delete", "index": 0, "count": 3},  # only 2 characters there
        {"type": "delete", "index": 0, "count": 0},
        {"type": "delete", "index": "0"},
    ],
)
def test_bad_edit_commands_change_nothing(cluster, command):
    cluster.type("A", 0, "ab")
    before = cluster.full_state("A")["text_doc"]
    reply = cluster.command("A", command)
    assert reply["type"] == "error"
    assert cluster.full_state("A")["text_doc"] == before
