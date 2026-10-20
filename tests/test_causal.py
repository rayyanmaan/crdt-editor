"""Unit tests for causal delivery and repair (causal.py), with no network.

A simulated network below loses, repeats and reorders messages at random.
The only thing that fills the gaps is the version-vector repair, just like
on the real network.
"""

import random

import pytest

from causal import CausalText, validate_envelope
from test_rga import check_nothing_lost


def ops_of(envelopes):
    return [e["op"] for e in envelopes]


# ---- local edits ----------------------------------------------------------------

def test_local_edits_are_numbered_and_logged():
    a = CausalText("A#1")
    envelopes = [a.insert(0, "h"), a.insert(1, "i"), a.delete(0)]
    assert [(e["origin"], e["seq"]) for e in envelopes] == [("A#1", 1), ("A#1", 2), ("A#1", 3)]
    assert a.text() == "i"
    assert a.version_vector() == {"A#1": 3}
    assert a.summary()["buffered"] == []


# ---- ops arriving out of order -----------------------------------------------------

def test_ops_in_order_apply_at_once():
    a, b = CausalText("A#1"), CausalText("B#1")
    for e in [a.insert(0, "h"), a.insert(1, "i")]:
        assert b.receive(e) == 1
    assert b.text() == "hi"


def test_later_op_waits_for_the_earlier_one_from_the_same_origin():
    a, b = CausalText("A#1"), CausalText("B#1")
    first, second, third = a.insert(0, "a"), a.insert(1, "b"), a.insert(2, "c")

    assert b.receive(third) == 0
    assert b.receive(second) == 0
    assert b.summary()["buffered"] == ["A#1:2", "A#1:3"]
    assert b.text() == ""

    assert b.receive(first) == 3  # unblocks the two waiting ops
    assert b.text() == "abc"
    assert b.summary()["buffered"] == []


def test_version_vector_counts_only_ops_with_no_gap_before_them():
    a, b = CausalText("A#1"), CausalText("B#1")
    ops = [a.insert(i, ch) for i, ch in enumerate("abc")]
    b.receive(ops[0])
    b.receive(ops[2])  # op 2 is missing
    assert b.version_vector() == {"A#1": 1}  # not 3: the gap stays visible
    assert [e["seq"] for e in a.missing_for(b.version_vector(), 10)] == [2, 3]


def test_delete_waits_for_the_insert_it_deletes():
    a, b, c = CausalText("A#1"), CausalText("B#1"), CausalText("C#1")
    insert = a.insert(0, "x")
    b.receive(insert)
    delete = b.delete(0)  # B deletes A's character

    assert c.receive(delete) == 0  # C hasn't got the "x" yet
    assert c.summary()["buffered"] == ["B#1:1"]
    assert c.receive(insert) == 2
    assert c.text() == "" and c.summary()["tombstones"] == 1


def test_insert_waits_for_a_character_from_another_origin():
    a, b, c = CausalText("A#1"), CausalText("B#1"), CausalText("C#1")
    hello = a.insert(0, "h")
    b.receive(hello)
    after_it = b.insert(1, "i")  # B's op refers to A's character

    assert c.receive(after_it) == 0
    assert c.receive(hello) == 2
    assert c.text() == "hi"


def test_duplicates_change_nothing():
    a, b = CausalText("A#1"), CausalText("B#1")
    ops = [a.insert(0, "a"), a.insert(1, "b"), a.delete(0)]
    for e in ops + ops + ops:
        b.receive(e)
    assert b.doc.state() == a.doc.state()
    assert b.summary()["logged"] == 3


def test_conflicting_duplicate_is_refused():
    a, b = CausalText("A#1"), CausalText("B#1")
    real = a.insert(0, "a")
    fake = {**real, "op": {**real["op"], "char": "z"}}
    b.receive(real)
    with pytest.raises(ValueError):
        b.receive(fake)
    assert b.text() == "a"


# ---- repair ---------------------------------------------------------------------------

def test_repair_sends_exactly_what_is_missing_including_other_origins_ops():
    a, b, c = CausalText("A#1"), CausalText("B#1"), CausalText("C#1")
    for e in [a.insert(0, "a"), a.insert(1, "b")]:
        b.receive(e)
    b.insert(2, "c")
    # C has nothing. B has all of A's ops plus its own.
    missing = b.missing_for(c.version_vector(), limit=100)
    assert [(e["origin"], e["seq"]) for e in missing] == [("A#1", 1), ("A#1", 2), ("B#1", 1)]
    for e in missing:
        c.receive(e)
    assert c.text() == "abc"
    assert b.missing_for(c.version_vector(), limit=100) == []


def test_repair_comes_in_batches():
    a, b = CausalText("A#1"), CausalText("B#1")
    for i in range(25):
        a.insert(i, "x")
    batches = 0
    while True:
        batch = a.missing_for(b.version_vector(), limit=10)
        if not batch:
            break
        batches += 1
        for e in batch:
            b.receive(e)
    assert batches == 3 and b.text() == "x" * 25


# ---- a random, unreliable network ---------------------------------------------------

def simulate(seed, causal=True, steps=150, replicas=3):
    """Replicas edit while a network loses, repeats and reorders messages.

    Each local op is broadcast once over the lossy network. Now and then a
    replica runs repair with a peer (sends what the version vector says is
    missing), also over the lossy network. At the end the network calms
    down and repair runs until nothing is missing.
    Returns (replicas, every op anyone made).
    """
    rng = random.Random(seed)
    docs = [CausalText(f"{name}#1", causal=causal) for name in "ABC"[:replicas]]
    in_flight = []  # (destination index, envelope)
    made = []

    def send(dest, envelope):
        if rng.random() < 0.3:
            return  # lost
        in_flight.append((dest, envelope))
        if rng.random() < 0.2:
            in_flight.append((dest, envelope))  # duplicated

    for _ in range(steps):
        roll = rng.random()
        i = rng.randrange(len(docs))
        doc = docs[i]
        if roll < 0.45:
            if len(doc.text()) and rng.random() < 0.3:
                e = doc.delete(rng.randrange(len(doc.text())))
            else:
                e = doc.insert(rng.randint(0, len(doc.text())), rng.choice("abcdefgh"))
            made.append(e)
            for j in range(len(docs)):
                if j != i:
                    send(j, e)
        elif roll < 0.8 and in_flight:
            # deliver a random message: arrival order is scrambled
            dest, e = in_flight.pop(rng.randrange(len(in_flight)))
            docs[dest].receive(e)
        else:
            j = rng.randrange(len(docs))
            if j != i:
                for e in doc.missing_for(docs[j].version_vector(), limit=5):
                    send(j, e)

    # The network heals: deliver what's in flight, then repair until done.
    rng.shuffle(in_flight)
    for dest, e in in_flight:
        docs[dest].receive(e)
    for _ in range(100):
        moved = False
        for i, src in enumerate(docs):
            for j, dst in enumerate(docs):
                if i != j:
                    for e in src.missing_for(dst.version_vector(), limit=20):
                        dst.receive(e)
                        moved = True
        if not moved:
            break
    return docs, made


@pytest.mark.parametrize("seed", range(200))
def test_unreliable_network_converges_and_loses_nothing(seed):
    docs, made = simulate(seed)
    states = [d.doc.state() for d in docs]
    assert states[0] == states[1] == states[2]
    check_nothing_lost(states[0], ops_of(made))
    assert all(d.summary()["buffered"] == [] for d in docs)


def test_without_the_causal_buffer_replicas_diverge():
    # The experiment from the proposal: same random histories, buffer off.
    diverged = 0
    for seed in range(50):
        docs, made = simulate(seed, causal=False)
        texts = {d.text() for d in docs}
        if len(texts) > 1:
            diverged += 1
    assert diverged > 25  # most histories end with replicas disagreeing for good


def test_without_the_buffer_a_specific_out_of_order_delivery_goes_wrong():
    a = CausalText("A#1")
    h, i = a.insert(0, "h"), a.insert(1, "i")
    good, bad = CausalText("B#1"), CausalText("C#1", causal=False)
    for doc in (good, bad):
        doc.receive(i)  # arrives before the "h" it goes after
        doc.receive(h)
    assert good.text() == "hi"
    assert bad.text() == "ih"  # guessed wrong, and never fixes itself
    bad.receive(i)  # even a repeat doesn't help: it's a duplicate now
    assert bad.text() == "ih"


# ---- validation ------------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad",
    [
        "nope",
        {"seq": 1, "op": {"kind": "delete", "target": [1, "A#1"]}},
        {"origin": "", "seq": 1, "op": {"kind": "delete", "target": [1, "A#1"]}},
        {"origin": "A#1", "seq": 0, "op": {"kind": "delete", "target": [1, "A#1"]}},
        {"origin": "A#1", "seq": True, "op": {"kind": "delete", "target": [1, "A#1"]}},
        {"origin": "A#1", "seq": 1, "op": {"kind": "fly"}},
        # an insert claiming to be from A#1 but with B#1's id
        {"origin": "A#1", "seq": 1, "op": {"kind": "insert", "id": [1, "B#1"], "after": None, "char": "x"}},
    ],
)
def test_malformed_envelopes_are_rejected(bad):
    with pytest.raises(ValueError):
        validate_envelope(bad)
    with pytest.raises(ValueError):
        CausalText("Z#1").receive(bad)


# ---- added after breaking the layer on purpose ------------------------------------------

def test_an_op_that_unblocks_another_releases_it_even_if_that_one_sorts_first():
    # The buffer is checked in (origin, seq) order. Here the waiting op is
    # from "A#1", which sorts before the "B#1" op it waits for, so one pass
    # over the buffer isn't enough: after B#1:1 is applied, it must look again.
    b, a, c = CausalText("B#1"), CausalText("A#1"), CausalText("C#1")
    h = b.insert(0, "h")
    a.receive(h)
    i = a.insert(1, "i")  # A's op goes after B's character

    c.receive(i)
    assert c.summary()["buffered"] == ["A#1:1"]
    c.receive(h)
    assert c.text() == "hi"
    assert c.summary()["buffered"] == []


def test_ops_from_one_replica_are_applied_in_the_order_it_made_them():
    # A types "x", then "y" at the very start. "y" doesn't refer to "x", so
    # RGA alone could apply it first. But a reader should never see A's later
    # edit without its earlier one, and "applied A#1: n" promises that ops
    # 1..n are all applied.
    a, b = CausalText("A#1"), CausalText("B#1")
    x = a.insert(0, "x")
    y = a.insert(0, "y")

    b.receive(y)
    assert b.text() == ""  # waits for x, although it could technically go in
    assert b.summary()["applied"] == {}
    b.receive(x)
    assert b.text() == "yx" and b.summary()["applied"] == {"A#1": 2}
