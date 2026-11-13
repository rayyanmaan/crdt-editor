"""Randomized tests with Hypothesis: random edits, random networks, random crashes.

The other tests each try one scenario someone thought of. These let
Hypothesis make them up: it picks a random sequence of steps (type here,
delete there, deliver this message, lose that one, deliver another twice,
crash a replica...), runs it, and checks the result. When something fails,
it shrinks the sequence to a simpler one that still fails, which is much
easier to read than a random 40-step history.

The machine below drives three real Replica objects (replica.py, with
storage.py saving to a temporary folder) but no sockets: messages a replica
sends go into a "network" list instead, and a step takes one out and hands
it to its destination. That way Hypothesis controls the network completely,
including the order, which real sockets wouldn't let it do. What this leaves
out is the transport itself (outbox queues, reconnecting, the proxy); the
real-process tests (test_cluster_*.py) cover that.

A crash happens between two commands, like kill -9 would: the replica's
memory is gone, and the new one only sees what had reached the operating
system (its Python file buffers are not flushed for it). Messages to and
from it that were still in flight are lost. Right after the restart, before
any peer can help, its state must match what it had before the crash.
What this doesn't cover is a crash in the middle of a write; the unit tests
in test_storage.py cover a half-written last line.

At the end, the network heals: replicas keep syncing until nothing changes.
Then the checks are the two promises from the proposal:
1. every replica holds identical state (every character with what it was
   typed after and whether it's deleted, the op logs, counter and set), and
2. nothing anyone was told was accepted is missing.
"""

import itertools
import shutil
import tempfile
from contextlib import ExitStack
from unittest import mock

import hypothesis.strategies as st
from hypothesis import given, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import replica as replica_module
from config import REPLICA_IDS
from replica import Replica
from storage import Storage
from test_rga import check_nothing_lost

CHARS = st.sampled_from("abc")
ELEMENTS = st.sampled_from(["x", "y"])
WHO = st.sampled_from(REPLICA_IDS)

HEAL_ROUNDS = 50
MAX_DELIVERIES = 100_000  # far more than any history here needs; stops a ping-pong bug hanging the test


class Histories(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.folder = tempfile.mkdtemp()
        self.patches = ExitStack()
        try:
            # fsync makes each save wait for the disk, which is slow over
            # thousands of steps. These tests crash a process, not the
            # machine, and the OS keeps a crashed process's writes anyway.
            self.patches.enter_context(mock.patch("os.fsync"))
            self.patches.enter_context(mock.patch.object(replica_module, "DATA_DIR", self.folder))
            # A clock that ticks once per call, so incarnations (and so op
            # names) are the same every time Hypothesis replays a history.
            clock = itertools.count(1_000_000)
            self.patches.enter_context(mock.patch("time.time_ns", lambda: next(clock) * 1_000_000))
            # The real causal buffer, whatever the environment says.
            self.patches.enter_context(mock.patch.object(replica_module, "CAUSAL_BUFFER", True))

            self.network = []  # (to, message) in flight, in no particular order
            self.replicas = {}
            for rid in REPLICA_IDS:
                self.replicas[rid] = self.start(rid)
        except BaseException:
            self.clean_up()
            raise
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
            self.clean_up()

    def clean_up(self):
        try:
            for r in getattr(self, "replicas", {}).values():
                r.storage.close()
        finally:
            self.patches.close()
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

    @rule(who=WHO, length=st.integers(201, 260))
    def paste_a_long_text(self, who, length):
        """More ops than fit in one message (REPAIR_BATCH), so sending and
        repairing them takes several batches."""
        self.type_at(who, len(self.replicas[who].text.text()), "z" * length)

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
        reply = self.accept(self.replicas[who].on_command({"type": "add", "element": element}))
        self.accepted_tags.add((element, reply["tag"]))

    @rule(who=WHO, element=ELEMENTS)
    def remove(self, who, element):
        """A remove tombstones exactly the tags of that element this replica
        has seen and not yet removed. Worked out here, not taken from the
        reply, so a remove that does the wrong thing can't vouch for itself."""
        r = self.replicas[who]
        state = r.set.to_dict()
        expected = set(state["adds"].get(element, [])) - set(state["removed"])
        reply = self.accept(r.on_command({"type": "remove", "element": element}))
        assert set(reply["tombstoned"]) == expected
        assert element not in r.set.elements()
        self.accepted_removes |= expected

    # ---- the network -----------------------------------------------------------------

    @rule(frm=WHO, to=WHO)
    def sync(self, frm, to):
        """One round of what sync_loop sends every second, to one peer."""
        if frm != to:
            self.replicas[frm].send_sync_round(peers=[to])

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
        # Like kill -9: the OS drops the lock, but nobody flushes Python's
        # file buffers. The new replica sees only what reached the OS.
        old.storage.lock_file.close()
        old.storage.lock_file = None
        # Messages it hadn't sent yet die with it, and so do messages on
        # their way to it (its connections close).
        self.network = [(to, msg) for to, msg in self.network if who not in (to, msg["from"])]
        new = self.start(who)
        old.storage.close()  # only now, so it can't have helped the new one
        self.replicas[who] = new

        # Before any peer can help: it has everything it had, from disk alone.
        assert new.incarnation > old.incarnation
        assert full_text_state(new) == full_text_state(old)
        assert new.counter.to_dict() == old.counter.to_dict()
        assert new.set.to_dict() == old.set.to_dict()

    # ---- checks ----------------------------------------------------------------------

    @invariant()
    def every_op_anyone_has_was_accepted(self):
        # An op can be saved and not confirmed only if a replica crashes
        # between the two, which this machine never does mid-command.
        accepted = {(e["origin"], e["seq"]) for e in self.accepted_ops}
        for r in self.replicas.values():
            for origin, ops in r.text.log.items():
                for seq in ops:
                    assert (origin, seq) in accepted

    def heal_and_check(self):
        # Deliver everything still in flight, then keep syncing every pair
        # until a whole round changes nothing.
        for _ in range(HEAL_ROUNDS):
            before = self.snapshot()
            self.deliver_everything()
            for r in self.replicas.values():
                r.send_sync_round()
            self.deliver_everything()
            if self.snapshot() == before:
                break
        else:
            raise AssertionError(f"still changing after {HEAL_ROUNDS} rounds of syncing")

        replicas = list(self.replicas.values())
        # 1. Identical everywhere.
        first = replicas[0]
        for r in replicas[1:]:
            assert full_text_state(r) == full_text_state(first)
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

    def deliver_everything(self):
        for _ in range(MAX_DELIVERIES):
            if not self.network:
                return
            to, msg = self.network.pop(0)
            self.replicas[to].on_peer_message(msg)
        raise AssertionError("messages never stop: replicas keep answering each other")

    def snapshot(self):
        return [
            (full_text_state(r), r.counter.to_dict(), r.set.to_dict(), sorted(r.text.buffer))
            for r in self.replicas.values()
        ]


def full_text_state(r):
    """Everything about the text that has to match: every character in
    document order with what it was typed after and whether it's deleted,
    and the whole op log. (Not which ops have waited, or the version
    vector, which follow from the log.)"""
    nodes = [(n.id, n.after, n.char, n.deleted) for n in r.text.doc.nodes]
    log = {origin: dict(ops) for origin, ops in r.text.log.items()}
    return nodes, log


Histories.TestCase.settings = settings(
    max_examples=300,
    stateful_step_count=40,
    deadline=None,  # some steps paste 200+ characters; time isn't what's tested here
)
test_random_histories = Histories.TestCase


# ---- the log file on its own -----------------------------------------------------------

JSON_VALUES = st.recursive(
    st.none() | st.booleans() | st.integers() | st.text(max_size=5),
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=3), inner, max_size=3),
    max_leaves=6,
)


@settings(max_examples=300, deadline=None)
@given(saved=st.lists(JSON_VALUES, max_size=6), unfinished=st.binary(max_size=30))
def test_an_unfinished_last_line_never_costs_a_saved_op(saved, unfinished):
    """A crash mid-write leaves the start of a line with no newline after
    it. Whatever those bytes are, every op saved before it still loads, and
    the log keeps working afterwards."""
    folder = tempfile.mkdtemp()
    stores = []
    try:
        s = Storage(folder)
        stores.append(s)
        s.load_ops()
        s.append_ops(saved)
        s.close()
        with open(s.ops_path, "ab") as f:
            f.write(unfinished.replace(b"\n", b""))  # a newline would finish the line

        again = Storage(folder)
        stores.append(again)
        ops, _ = again.load_ops()
        assert ops == saved
        again.append_ops(["next"])
        again.close()
        last = Storage(folder)
        stores.append(last)
        assert last.load_ops()[0] == saved + ["next"]
    finally:
        for store in stores:
            store.close()
        shutil.rmtree(folder, ignore_errors=True)
