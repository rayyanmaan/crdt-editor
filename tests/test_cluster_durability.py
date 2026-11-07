"""Crashes with saving to disk, on real replica processes.

The claim being tested: an edit a replica said it accepted is never lost,
even if that replica crashes (kill -9) before any other replica got it.
"""

import os
import random
import threading
import time

from cluster import wait_until
from config import REPLICA_IDS
from conftest import all_links_up
from test_cluster_text import Edits, everyone_has, text_of


def test_edits_made_alone_survive_a_crash(cluster):
    # A is the only one running, so nobody else can get its edits.
    cluster.kill("B")
    cluster.kill("C")
    edits = Edits()
    edits.record(cluster.type("A", 0, "alone"))
    edits.record(cluster.delete("A", 0))
    a_inc = cluster.increment("A", 2)["incarnation"]
    tag = cluster.add("A", "solo")["tag"]

    cluster.kill("A")
    cluster.start("A")
    state = wait_until(lambda: cluster.state("A"))
    assert state["incarnation"] > a_inc
    assert state["recovered_ops"] == 6
    assert state["text"]["text"] == "lone"
    assert state["slots"] == {f"A#{a_inc}": 2}
    assert state["set_state"] == {"adds": {"solo": [tag]}, "removed": []}

    # Then the others come back and get it all, along with new edits.
    cluster.start("B")
    cluster.start("C")
    edits.record(cluster.type("A", 0, "a"))
    wait_until(lambda: everyone_has(cluster, edits))
    wait_until(lambda: all(cluster.state(rid)["counter"] == 2 for rid in REPLICA_IDS))


def test_a_replica_keeps_ops_it_received_after_their_author_is_gone(cluster):
    edits = Edits()
    edits.record(cluster.type("A", 0, "from A"))
    wait_until(lambda: everyone_has(cluster, edits))

    # Everyone crashes. A never comes back; C comes back on its own.
    for rid in REPLICA_IDS:
        cluster.kill(rid)
    cluster.start("C")
    wait_until(lambda: everyone_has(cluster, edits, replicas=["C"]))


def test_kill_minus_9_mid_typing_loses_no_confirmed_edit(cluster):
    # B and C are down, so the only place A can get its edits back from
    # is its own disk.
    cluster.kill("B")
    cluster.kill("C")
    edits = Edits()
    stop = threading.Event()

    def keep_typing():
        while not stop.is_set():
            try:
                edits.record(cluster.type("A", "end", random.choice("abcdefgh")))
            except Exception:
                return  # A is dead

    typist = threading.Thread(target=keep_typing)
    typist.start()
    time.sleep(random.uniform(0.3, 0.8))
    cluster.kill("A")
    stop.set()
    typist.join(timeout=5)
    confirmed = len(edits.envelopes)
    assert confirmed > 0

    cluster.start("A")
    state = wait_until(lambda: cluster.full_state("A"))
    origin = edits.envelopes[0]["origin"]
    # Every confirmed op is there. One more may be too: saved, then killed
    # before the reply went out. That's fine; nobody was told it failed.
    assert state["text"]["have"][origin] >= confirmed
    assert state["text"]["text"].startswith("".join(e["op"]["char"] for e in edits.envelopes))

    cluster.start("B")
    cluster.start("C")
    wait_until(lambda: all_links_up(cluster))
    wait_until(lambda: len({text_of(cluster, rid) for rid in REPLICA_IDS}) == 1)


def test_a_half_written_last_op_is_ignored(cluster):
    edits = Edits()
    edits.record(cluster.type("A", 0, "ok"))
    cluster.kill("A")
    with open(os.path.join(cluster.data_dir, "A", "ops.log"), "ab") as f:
        f.write(b'{"op": {"kind": "ins')  # as if the crash came mid-write

    cluster.start("A")
    wait_until(lambda: cluster.state("A")["text"]["text"] == "ok")
    edits.record(cluster.type("A", "end", "!"))
    wait_until(lambda: everyone_has(cluster, edits))

    # And it still loads cleanly next time: the new op didn't get glued
    # onto the broken line.
    cluster.kill("A")
    cluster.start("A")
    assert wait_until(lambda: cluster.state("A"))["recovered_ops"] == 3
