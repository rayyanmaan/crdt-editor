"""Unit tests for the RGA text CRDT on its own, with no network.

The convergence tests don't just compare replicas with each other. They also
check that every insert anyone made is in the final document and that exactly
the deleted characters are tombstones, so "they all lost the same edit"
can't pass.
"""

import random

import pytest

from rga import ROOT, RGA, MissingDependency, decode_id, validate_op


def type_text(doc, text, at=None):
    """Type `text` one character at a time, like a person would. Returns the ops."""
    at = len(doc) if at is None else at
    return [doc.insert_local(at + i, ch) for i, ch in enumerate(text)]


def apply_all(doc, ops):
    for op in ops:
        doc.apply(op)


# ---- typing on one replica ----------------------------------------------------

def test_typing_and_deleting():
    doc = RGA("A#1")
    type_text(doc, "helo")
    doc.insert_local(3, "l")  # fix the typo in the middle
    assert doc.text() == "hello"
    doc.delete_local(0)
    assert doc.text() == "ello"
    # the deleted "h" is still there as a tombstone
    assert [(char, deleted) for _, char, deleted in doc.state()][0] == ("h", True)


def test_positions_out_of_range_are_rejected():
    doc = RGA("A#1")
    type_text(doc, "ab")
    with pytest.raises(IndexError):
        doc.insert_local(3, "x")
    with pytest.raises(IndexError):
        doc.delete_local(2)


def test_insert_ids_count_up():
    doc = RGA("A#1")
    ops = type_text(doc, "abc")
    assert [op["id"] for op in ops] == [[1, "A#1"], [2, "A#1"], [3, "A#1"]]


def test_clock_jumps_past_ids_seen_from_others():
    a, b = RGA("A#1"), RGA("B#1")
    apply_all(b, type_text(a, "abcde"))  # B sees lamports 1..5
    op = b.insert_local(5, "!")
    assert op["id"] == [6, "B#1"]  # bigger than everything B had seen


# ---- the worked examples from rga.py's docstring --------------------------------

def test_concurrent_inserts_at_the_same_spot_order_the_same_everywhere():
    a, b = RGA("A#1"), RGA("B#1")
    apply_all(b, type_text(a, "ac"))

    x = a.insert_local(1, "x")  # id (3, A#1)
    y = b.insert_local(1, "y")  # id (3, B#1): same lamport, "B#1" > "A#1"

    a.apply(y)
    b.apply(x)
    assert a.text() == b.text() == "ayxc"  # the bigger id goes first
    assert a.state() == b.state()


def test_concurrent_delete_and_insert_after_the_deleted_character():
    a, b = RGA("A#1"), RGA("B#1")
    apply_all(b, type_text(a, "abc"))

    delete_b = a.delete_local(1)
    insert_x = b.insert_local(2, "x")  # right after "b"

    a.apply(insert_x)  # "b" is a tombstone on A, but x still has a place to go
    b.apply(delete_b)
    assert a.text() == b.text() == "axc"
    assert a.state() == b.state()


def test_insert_after_a_character_that_is_already_deleted_here():
    a, b = RGA("A#1"), RGA("B#1")
    apply_all(b, type_text(a, "abc"))
    b_char = a.visible()[1].id
    a.delete_local(1)
    # An insert from someone who hadn't seen the delete, placed after "b".
    a.apply({"kind": "insert", "id": [9, "B#1"], "after": list(b_char), "char": "x"})
    assert a.text() == "axc"


def test_two_people_typing_words_at_the_same_spot_keep_their_words_together():
    # Each typed character goes right after the previous one, so RGA keeps
    # each word in one piece. (Typing *backwards* is different: Milestone 4.)
    a, b = RGA("A#1"), RGA("B#1")
    hello = type_text(a, "hello")
    world = type_text(b, "world")
    apply_all(a, world)
    apply_all(b, hello)
    assert a.text() == b.text()
    assert a.text() in ("helloworld", "worldhello")


# ---- duplicates and ops that arrive too early -----------------------------------

def test_applying_the_same_op_twice_changes_nothing():
    a, b = RGA("A#1"), RGA("B#1")
    ops = type_text(a, "abc") + [a.delete_local(1)]
    apply_all(b, ops)
    before = b.state()
    apply_all(b, ops)  # every message delivered a second time
    assert b.state() == before == a.state()


def test_insert_before_the_character_it_follows_is_held_back():
    a, b = RGA("A#1"), RGA("B#1")
    first, second = type_text(a, "ab")
    assert not b.can_apply(second)
    with pytest.raises(MissingDependency):
        b.apply(second)
    assert b.state() == []  # nothing half-applied
    b.apply(first)
    assert b.can_apply(second)
    b.apply(second)
    assert b.text() == "ab"


def test_delete_before_the_insert_it_deletes_is_held_back():
    a, b = RGA("A#1"), RGA("B#1")
    insert = a.insert_local(0, "a")
    delete = a.delete_local(0)
    assert not b.can_apply(delete)
    with pytest.raises(MissingDependency):
        b.apply(delete)
    b.apply(insert)
    b.apply(delete)
    assert b.state() == a.state()


# ---- every delivery order gives the same document --------------------------------
#
# "Every order" here means every order in which each op arrives after the
# character it refers to. That's a superset of truly causal orders (it
# ignores the order one replica made its own ops in), which is fine: this
# data structure only needs the reference to be there.

def causal_orders(ops):
    """Every order of `ops` in which each op arrives after what it refers to.

    References to characters outside `ops` are assumed to be there already.
    """
    made_by = {tuple(op["id"]): i for i, op in enumerate(ops) if op["kind"] == "insert"}

    def needs(op):
        ref = decode_id(op["after"] if op["kind"] == "insert" else op["target"])
        return made_by.get(ref)  # None if it's the root or was there before

    def extend(order, used):
        if len(order) == len(ops):
            yield list(order)
            return
        for i, op in enumerate(ops):
            dep = needs(op)
            if i not in used and (dep is None or dep in used):
                order.append(i)
                used.add(i)
                yield from extend(order, used)
                order.pop()
                used.discard(i)

    yield from extend([], set())


def test_every_reference_respecting_order_gives_the_same_document():
    # Three replicas start from the same "ab", then edit concurrently:
    # inserts at the same spot, an insert after a character someone else
    # deletes, and a chain of typing.
    base = RGA("A#1")
    base_ops = type_text(base, "ab")
    a, b, c = RGA("A#1"), RGA("B#1"), RGA("C#1")
    for doc in (a, b, c):
        apply_all(doc, base_ops)
    a.clock = b.clock = c.clock = base.clock

    concurrent = (
        type_text(a, "xy", at=1)       # A types "xy" between a and b
        + [a.delete_local(0)]          # ...and deletes "a"
        + [b.insert_local(1, "z")]     # B types "z" in the same spot
        + [b.insert_local(3, "!")]     # ...and types after "b"
        + [c.delete_local(1)]          # C deletes "b"
        + [c.insert_local(0, "q")]     # ...and types at the very start
    )

    results = set()
    count = 0
    for order in causal_orders(concurrent):
        doc = RGA("Z#1")
        apply_all(doc, base_ops)
        apply_all(doc, [concurrent[i] for i in order])
        results.add(repr(doc.state()))
        count += 1

    # 7 ops where only "y" must come after "x": 7!/2 = 2520 orders.
    assert count == 2520
    assert len(results) == 1, "different delivery orders gave different documents"
    check_nothing_lost(doc.state(), concurrent, already=base_ops)  # and it's complete


# ---- random editing histories ------------------------------------------------------

def random_history(seed, replicas=3, steps=60):
    """Replicas type, delete and sync with each other at random.

    Returns (replicas, the ops each one applied in order, every op anyone
    made). A sync sends one replica all the ops another has applied, in the
    order it applied them, which always respects what each op refers to.
    """
    rng = random.Random(seed)
    docs = [RGA(f"{name}#1") for name in "ABC"[:replicas]]
    logs = [[] for _ in docs]  # ops each replica has applied, in order
    all_ops = []
    for _ in range(steps):
        i = rng.randrange(len(docs))
        doc = docs[i]
        roll = rng.random()
        if roll < 0.55 or len(doc) == 0:
            op = doc.insert_local(rng.randint(0, len(doc)), rng.choice("abcdefgh"))
        elif roll < 0.75:
            op = doc.delete_local(rng.randrange(len(doc)))
        else:
            # Replica i hears everything replica j has, in j's order.
            j = rng.randrange(len(docs))
            for op in logs[j]:
                if op not in logs[i]:
                    docs[i].apply(op)
                    logs[i].append(op)
            continue
        logs[i].append(op)
        all_ops.append(op)
    return docs, logs, all_ops


def deliver_in_random_causal_order(doc, ops, rng):
    pending = list(ops)
    while pending:
        ready = [op for op in pending if doc.can_apply(op)]
        assert ready, "no op could be applied: delivery is stuck"
        op = rng.choice(ready)
        doc.apply(op)
        pending.remove(op)


def check_nothing_lost(state, all_ops, already=()):
    """The document holds exactly one node per insert, with the right
    character, and exactly the deleted ones are tombstones."""
    ops = list(already) + list(all_ops)
    deleted = {tuple(op["target"]) for op in ops if op["kind"] == "delete"}
    expected = {
        tuple(op["id"]): (op["char"], tuple(op["id"]) in deleted)
        for op in ops if op["kind"] == "insert"
    }
    actual = {tuple(i): (char, d) for i, char, d in state}
    assert len(state) == len(actual), "an id appears twice"
    assert actual == expected


@pytest.mark.parametrize("seed", range(300))
def test_random_histories_converge_and_lose_nothing(seed):
    docs, logs, all_ops = random_history(seed)
    rng = random.Random(seed)

    # Let every replica catch up on whatever it missed, in a random causal order.
    for doc, log in zip(docs, logs):
        deliver_in_random_causal_order(doc, [op for op in all_ops if op not in log], rng)

    states = [doc.state() for doc in docs]
    assert states[0] == states[1] == states[2]
    check_nothing_lost(states[0], all_ops)

    # A brand-new replica that receives everything in some other order agrees too.
    fresh = RGA("Z#1")
    deliver_in_random_causal_order(fresh, all_ops, rng)
    assert fresh.state() == states[0]


# ---- validation ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "bad",
    [
        "nope",
        {"kind": "move"},
        {"kind": "insert", "id": [1, "A"], "after": None, "char": "ab"},  # two characters
        {"kind": "insert", "id": [1, "A"], "after": None, "char": ""},
        {"kind": "insert", "id": [0, "A"], "after": None, "char": "x"},  # lamport 0 is the root's
        {"kind": "insert", "id": [1, ""], "after": None, "char": "x"},
        {"kind": "insert", "id": [True, "A"], "after": None, "char": "x"},
        {"kind": "insert", "id": "1A", "after": None, "char": "x"},
        {"kind": "insert", "id": [2, "A"], "after": [2, "B"], "char": "x"},  # lamport not bigger
        {"kind": "insert", "id": [2, "A"], "after": [5, "B"], "char": "x"},
        {"kind": "delete", "target": None},  # can't delete the root
        {"kind": "delete", "target": [1]},
    ],
)
def test_malformed_ops_are_rejected(bad):
    with pytest.raises(ValueError):
        validate_op(bad)


def test_rejected_op_leaves_the_document_untouched():
    doc = RGA("A#1")
    type_text(doc, "ab")
    before = doc.state()
    with pytest.raises(ValueError):
        doc.apply({"kind": "insert", "id": [9, "B#1"], "after": None, "char": "xy"})
    assert doc.state() == before


def test_root_is_never_visible():
    doc = RGA("A#1")
    assert doc.text() == "" and doc.state() == [] and ROOT not in [tuple(i) for i, _, _ in doc.state()]


# ---- more edge cases ----------------------------------------------------------------------

def test_three_inserts_at_the_same_spot_in_every_order():
    replicas = [RGA(f"{name}#1") for name in "ABC"]
    ops = [doc.insert_local(0, ch) for doc, ch in zip(replicas, "abc")]  # all id lamport 1
    results = {repr(apply_all_and_state(order, ops)) for order in causal_orders(ops)}
    assert len(results) == 1
    doc = RGA("Z#1")
    apply_all(doc, ops)
    assert doc.text() == "cba"  # biggest id first: (1,C#1) > (1,B#1) > (1,A#1)


def apply_all_and_state(order, ops):
    doc = RGA("Z#1")
    apply_all(doc, [ops[i] for i in order])
    return doc.state()


def test_replica_far_ahead_on_the_clock_still_converges():
    a, b = RGA("A#1"), RGA("B#1")
    type_text(a, "x" * 50)  # A's clock is at 50, B's at 0
    a_ops = [{"kind": "insert", "id": [n, "A#1"], "after": None if n == 1 else [n - 1, "A#1"],
              "char": "x"} for n in range(1, 51)]
    b_op = b.insert_local(0, "y")  # id (1, B#1): concurrent with all of A's typing
    apply_all(b, a_ops)
    a.apply(b_op)
    assert a.state() == b.state()
    assert a.text() == "y" + "x" * 50  # (1,B#1) beats (1,A#1) at the start


def test_backward_typing_example_converges():
    # A types "b", then "a" in front of it (typing backwards). B types "x" at
    # the start at the same time. Ids: b=(1,A), a=(2,A), x=(1,B).
    a, b = RGA("A#1"), RGA("B#1")
    a_ops = [a.insert_local(0, "b"), a.insert_local(0, "a")]
    x = b.insert_local(0, "x")
    apply_all(b, a_ops)
    a.apply(x)
    assert a.text() == b.text() == "axb"
    # x lands between A's two characters. For two people each typing a word
    # backwards, this is how RGA can interleave them (Milestone 4).


def test_insert_without_an_after_field_is_rejected_everywhere():
    op = {"kind": "insert", "id": [1, "A#1"], "char": "x"}
    doc = RGA("Z#1")
    for call in (validate_op, doc.apply, doc.can_apply):
        with pytest.raises(ValueError):
            call(op)


def test_conflicting_duplicate_is_refused():
    x = {"kind": "insert", "id": [1, "A#1"], "after": None, "char": "x"}
    y = {"kind": "insert", "id": [1, "A#1"], "after": None, "char": "y"}
    moved = {"kind": "insert", "id": [1, "A#1"], "after": None, "char": "x"}
    doc = RGA("Z#1")
    doc.apply(x)
    doc.apply(moved)  # an identical duplicate is fine
    with pytest.raises(ValueError):
        doc.apply(y)  # same id, different character: never silently pick one
    assert doc.text() == "x"
    assert doc.can_apply(y)  # nothing to wait for; apply() decides


@pytest.mark.parametrize("char", ["\ud800", "\udfff"])
def test_lone_surrogates_are_not_characters(char):
    with pytest.raises(ValueError):
        validate_op({"kind": "insert", "id": [1, "A#1"], "after": None, "char": char})


def test_characters_outside_the_basic_plane_are_one_character():
    doc = RGA("A#1")
    type_text(doc, "a😀b")
    assert doc.text() == "a😀b" and len(doc) == 3


def test_rejected_local_insert_does_not_move_the_clock():
    doc = RGA("A#1")
    with pytest.raises(ValueError):
        doc.insert_local(0, "xy")
    assert doc.clock == 0
