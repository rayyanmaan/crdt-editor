"""Randomized tests with Hypothesis: random edits, random networks, random crashes.

The other tests each try one scenario someone thought of. These let
Hypothesis make them up: it picks a random sequence of steps (type here,
delete there, deliver this message, lose that one, deliver another twice,
crash a replica...), runs it, and checks the result. When something fails,
it shrinks the sequence down to the shortest one that still fails, which is
much easier to read than a random 40-step history.

The machine below drives three real Replica objects (replica.py, with
storage.py saving to a temporary folder) but no sockets: messages a replica
sends go into a "network" list instead, and a step takes one out and hands
it to its destination. That way Hypothesis controls the network completely,
including the order, which real sockets wouldn't let it do. The real-process
tests (test_cluster_*.py) cover the sockets.

A crash throws a Replica object away and makes a new one from the same
folder, which is exactly what a restarted process does: memory gone, disk kept.

At the end, the network heals: replicas keep syncing until nothing changes.
Then the checks are the two promises from the proposal:
1. every replica is identical (text with tombstones, counter, set), and
2. nothing anyone was told was accepted is missing.
"""

import shutil
import tempfile
from unittest import mock

import hypothesis.strategies as st
from hypothesis import HealthCheck, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import replica as replica_module
from config import REPLICA_IDS
from replica import Replica
from storage import Storage
from test_rga import check_nothing_lost

CHARS = st.sampled_from("abc")
ELEMENTS = st.sampled_from(["x", "y"])
WHO = st.sampled_from(REPLICA_IDS)


class Histories(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.folder = tempfile.mkdtemp()
        # fsync makes each save wait for the disk, which is slow over
        # thousands of steps. These tests crash a process, not the machine,
        # and a crashed process's writes are kept by the OS without fsync.
        self.no_fsync = mock.patch("os.fsync")
        self.no_fsync.start()
        self.data_dir = mock.patch.object(replica_module, "DATA_DIR", self.folder)
        self.data_dir.start()

        self.network = []  # (to, message) in flight, in no particular order
        self.replicas = {rid: self.start(rid) for rid in REPLICA_IDS}
        # Everything a replica said it accepted.
        self.accepted_ops = []
        self.accepted_increments = 0
        self.accepted_tags = set()
        self.accepted_removes = set()

    def start(self, rid):
        r = Replica(rid)
        r.connected = set(r.peers)

        def send(peer, msg, r=r):
            self.network.append((peer, {**msg, "from": r.id, "inc": r.incarnation}))

        r.send = send
        return r

    def teardown(self):
        try:
            self.heal_and_check()
        finally:
            for r in self.replicas.values():
                r.storage.close()
            self.no_fsync.stop()
            self.data_dir.stop()
            shutil.rmtree(self.folder, ignore_errors=True)

    # ---- edits ---------------------------------------------------------------------

    def accept(self, reply):
        assert reply["type"] == "state", reply
        self.accepted_ops += reply.get("ops", [])
        return reply

    def type_at(self, who, index, text):
        """Type, and check it landed where it was typed. Agreeing isn't
        enough: replicas could all agree on text nobody typed."""
        r = self.replicas[who]
        before = r.text.text()
        self.accept(r.on_command({"type": "insert", "index": index, "text": text}))
        assert r.text.text() == before[:index] + text + before[index:]

    @rule(who=WHO, char=CHARS, data=st.data())
    def type_a_character(self, who, char, data):
        r = self.replicas[who]
        self.type_at(who, data.draw(st.integers(0, len(r.text.text())), label="index"), char)

    @rule(who=WHO, word=st.text(alphabet="abc", min_size=2, max_size=3), data=st.data())
    def type_a_word(self, who, word, data):
        r = self.replicas[who]
        self.type_at(who, data.draw(st.integers(0, len(r.text.text())), label="index"), word)

    @rule(who=WHO, data=st.data())
    def type_backwards(self, who, data):
        """Type two characters, the second one *before* the first: the
        typing pattern where RGA's tie-break matters most."""
        r = self.replicas[who]
        index = data.draw(st.integers(0, len(r.text.text())), label="index")
        self.type_at(who, index, "b")
        self.type_at(who, index, "a")

    @rule(who=WHO, data=st.data())
    def delete_a_character(self, who, data):
        r = self.replicas[who]
        length = len(r.text.text())
        if length == 0:
            return
        index = data.draw(st.integers(0, length - 1), label="index")
        before = r.text.text()
        self.accept(r.on_command({"type": "delete", "index": index, "count": 1}))
        assert r.text.text() == before[:index] + before[index + 1:]

    @rule(who=WHO)
    def increment(self, who):
        self.accept(self.replicas[who].on_command({"type": "increment", "amount": 1}))
        self.accepted_increments += 1

    @rule(who=WHO, element=ELEMENTS)
    def add(self, who, element):
        self.accepted_tags.add((element, self.accept(
            self.replicas[who].on_command({"type": "add", "element": element}))["tag"]))

    @rule(who=WHO, element=ELEMENTS)
    def remove(self, who, element):
        reply = self.accept(self.replicas[who].on_command({"type": "remove", "element": element}))
        self.accepted_removes |= set(reply["tombstoned"])

    # ---- the network -----------------------------------------------------------------

    @rule(frm=WHO, to=WHO)
    def sync(self, frm, to):
        """What sync_loop does every second: full counter/set state, then the
        text version vector (which makes the receiver send back what we lack)."""
        if frm == to:
            return
        r = self.replicas[frm]
        self.network.append((to, r.sync_message(r.counter, r.set)))
        self.network.append((to, {"type": "vv", "from": frm, "inc": r.incarnation,
                                  "vv": r.text.version_vector()}))

    @precondition(lambda self: self.network)
    @rule(data=st.data())
    def deliver(self, data):
        i = data.draw(st.integers(0, len(self.network) - 1), label="message")
        to, msg = self.network.pop(i)
        self.replicas[to].on_peer_message(msg)

    @precondition(lambda self: self.network)
    @rule(data=st.data())
    def deliver_twice(self, data):
        i = data.draw(st.integers(0, len(self.network) - 1), label="message")
        to, msg = self.network[i]  # left in the network, so it can arrive again
        self.replicas[to].on_peer_message(msg)

    @precondition(lambda self: self.network)
    @rule(data=st.data())
    def lose(self, data):
        self.network.pop(data.draw(st.integers(0, len(self.network) - 1), label="message"))

    @rule(who=WHO)
    def crash_and_restart(self, who):
        old = self.replicas[who]
        old.storage.close()  # its process is gone, and so is its lock
        # Messages it hadn't sent yet die with it, and so do messages on
        # their way to it (its connections close). The worst case for us.
        self.network = [(to, msg) for to, msg in self.network if who not in (to, msg["from"])]
        self.replicas[who] = self.start(who)
        assert self.replicas[who].incarnation > old.incarnation

    # ---- checks ----------------------------------------------------------------------

    @invariant()
    def a_replica_never_has_more_than_was_made(self):
        made = {}
        for e in self.accepted_ops:
            made[e["origin"]] = max(made.get(e["origin"], 0), e["seq"])
        for r in self.replicas.values():
            for origin, n in r.text.version_vector().items():
                # An op can be on disk and not confirmed only if the
                # replica crashed in between, which this machine never does
                # mid-command. So every op anyone has was confirmed.
                assert n <= made.get(origin, 0)

    def heal_and_check(self):
        # Deliver everything still in flight, then keep syncing every pair
        # until a whole round changes nothing.
        for _ in range(50):
            before = self.snapshot()
            while self.network:
                to, msg = self.network.pop(0)
                self.replicas[to].on_peer_message(msg)
            for frm in REPLICA_IDS:
                for to in REPLICA_IDS:
                    if frm != to:
                        self.sync(frm, to)
            while self.network:
                to, msg = self.network.pop(0)
                self.replicas[to].on_peer_message(msg)
            if self.snapshot() == before:
                break

        replicas = list(self.replicas.values())
        # 1. Identical everywhere.
        first = replicas[0]
        for r in replicas[1:]:
            assert r.text.doc.state() == first.text.doc.state()
            assert r.counter.to_dict() == first.counter.to_dict()
            assert r.set.to_dict() == first.set.to_dict()
        # 2. Nothing accepted is missing, and nothing is stuck.
        for r in replicas:
            assert not r.text.buffer
            assert not r.text.rejected
        check_nothing_lost(first.text.doc.state(), [e["op"] for e in self.accepted_ops])
        assert first.counter.value() == self.accepted_increments
        adds = {(element, tag) for element, tags in first.set.to_dict()["adds"].items() for tag in tags}
        assert adds == self.accepted_tags
        assert set(first.set.to_dict()["removed"]) == self.accepted_removes

    def snapshot(self):
        return [
            (r.text.doc.state(), r.counter.to_dict(), r.set.to_dict(), sorted(r.text.buffer))
            for r in self.replicas.values()
        ]


Histories.TestCase.settings = settings(
    max_examples=300,
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
test_random_histories = Histories.TestCase


# ---- the log file on its own -----------------------------------------------------------


from hypothesis import given  # noqa: E402

JSON_VALUES = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=5),
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=3), inner, max_size=3),
    max_leaves=6,
)


@settings(max_examples=300, deadline=None)
@given(saved=st.lists(JSON_VALUES, max_size=6), junk=st.binary(max_size=30))
def test_whatever_a_crash_leaves_at_the_end_the_saved_ops_load(saved, junk):
    """A crash can leave any bytes after the last complete line. Every op
    saved before it still loads, and the log keeps working afterwards."""
    folder = tempfile.mkdtemp()
    try:
        s = Storage(folder)
        s.load_ops()
        s.append_ops(saved)
        s.ops_file.close()
        if b"\n" in junk:
            junk = junk.replace(b"\n", b"")  # a newline would make it a "complete" line
        with open(s.ops_path, "ab") as f:
            f.write(junk)

        again = Storage(folder)
        ops, _ = again.load_ops()
        assert ops == saved
        again.append_ops(["next"])
        again.ops_file.close()
        assert Storage(folder).load_ops()[0] == saved + ["next"]
    finally:
        shutil.rmtree(folder, ignore_errors=True)
