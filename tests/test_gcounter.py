"""Unit tests for the G-Counter on its own, with no network."""

import itertools

import pytest

from gcounter import GCounter

# A few counters that overlap in different ways, to check the merge laws on.
EXAMPLES = [
    GCounter(),
    GCounter({"A#1": 3}),
    GCounter({"A#1": 1, "B#1": 2}),
    GCounter({"B#1": 5, "C#1": 1}),
    GCounter({"A#1": 2, "C#1": 4, "C#2": 1}),
]


def test_value_is_sum_of_slots():
    c = GCounter()
    c.increment("A#1", 3)
    c.increment("B#1")
    assert c.value() == 4


@pytest.mark.parametrize("amount", [-1, 0, 2.5, True, "3"])
def test_increment_rejects_anything_but_a_positive_whole_number(amount):
    with pytest.raises(ValueError):
        GCounter().increment("A#1", amount)


@pytest.mark.parametrize("a,b", itertools.product(EXAMPLES, repeat=2))
def test_merge_is_commutative(a, b):
    assert a.merge(b) == b.merge(a)


@pytest.mark.parametrize("a,b,c", itertools.product(EXAMPLES, repeat=3))
def test_merge_is_associative(a, b, c):
    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@pytest.mark.parametrize("a", EXAMPLES)
def test_merge_is_idempotent(a):
    assert a.merge(a) == a


def test_merge_keeps_the_larger_count_per_slot():
    old = GCounter({"A#1": 2})
    new = GCounter({"A#1": 5})
    assert old.merge(new).value() == 5
    assert new.merge(old).value() == 5


def test_restart_with_new_incarnation_does_not_hide_increments():
    # Replica C counted to 5, and its peer A saw that.
    a = GCounter({"C#1": 5})
    # C restarts empty with a new incarnation and counts once more.
    c = GCounter()
    c.increment("C#2")
    assert a.merge(c).value() == 6

    # If C had reused its old slot name, max(5, 1) would hide the new increment.
    reused = GCounter({"C#1": 1})
    assert a.merge(reused).value() == 5


@pytest.mark.parametrize(
    "bad", ["nope", {"A#1": -1}, {"A#1": 0}, {"A#1": "3"}, {"A#1": True}, {"": 1}, {1: 1}]
)
def test_from_dict_rejects_malformed_state(bad):
    with pytest.raises(ValueError):
        GCounter.from_dict(bad)


@pytest.mark.parametrize("a,b", itertools.product(EXAMPLES, repeat=2))
def test_merge_does_not_change_its_inputs(a, b):
    before_a, before_b = a.to_dict(), b.to_dict()
    a.merge(b)
    assert a.to_dict() == before_a and b.to_dict() == before_b


def test_same_state_encodes_the_same_way():
    one = GCounter({"B#1": 2, "A#1": 1})
    two = GCounter({"A#1": 1, "B#1": 2})
    assert list(one.to_dict()) == list(two.to_dict()) == ["A#1", "B#1"]
