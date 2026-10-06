"""Unit tests for the OR-Set on its own, with no network."""

import itertools

import pytest

from orset import ORSet


def make(adds, removed=()):
    return ORSet(adds, removed)


# Sets that overlap in different ways, including tombstones, to check the
# merge laws on.
EXAMPLES = [
    ORSet(),
    make({"x": {"A:1"}}),
    make({"x": {"A:1"}}, {"A:1"}),
    make({"x": {"A:1", "B:1"}, "y": {"B:2"}}, {"A:1"}),
    make({"y": {"B:2", "C:1"}}, {"C:1"}),
]


def test_add_and_remove():
    s = ORSet()
    s.add("x", "A:1")
    assert "x" in s
    assert s.remove("x") == {"A:1"}
    assert "x" not in s
    assert s.elements() == []


def test_removing_something_absent_does_nothing():
    s = ORSet()
    assert s.remove("x") == set()
    assert s == ORSet()


def test_re_adding_after_a_remove_brings_it_back():
    s = ORSet()
    s.add("x", "A:1")
    s.remove("x")
    s.add("x", "A:2")  # a new tag, not the dead one
    assert "x" in s


@pytest.mark.parametrize("a,b", itertools.product(EXAMPLES, repeat=2))
def test_merge_is_commutative(a, b):
    assert a.merge(b) == b.merge(a)


@pytest.mark.parametrize("a,b,c", itertools.product(EXAMPLES, repeat=3))
def test_merge_is_associative(a, b, c):
    assert a.merge(b).merge(c) == a.merge(b.merge(c))


@pytest.mark.parametrize("a", EXAMPLES)
def test_merge_is_idempotent(a):
    assert a.merge(a) == a


def test_concurrent_add_and_remove_add_wins():
    # Both replicas start with "x" (tag A:1).
    a = make({"x": {"A:1"}})
    b = make({"x": {"A:1"}})

    # At the same time: A removes "x", B adds "x" again (new tag B:1).
    a.remove("x")
    b.add("x", "B:1")

    # A's remove only covered the tag it had seen (A:1), so B's add survives,
    # whichever way round they merge.
    assert "x" in a.merge(b)
    assert "x" in b.merge(a)


def test_remove_after_seeing_the_add_wins():
    # Not concurrent this time: A has seen B's add before removing.
    a = make({"x": {"A:1"}})
    b = make({"x": {"A:1"}})
    b.add("x", "B:1")
    a = a.merge(b)

    a.remove("x")  # covers both A:1 and B:1
    assert "x" not in a.merge(b)


def test_stale_state_does_not_bring_a_removed_element_back():
    old = make({"x": {"A:1"}})  # a copy from before the remove
    current = make({"x": {"A:1"}})
    current.remove("x")

    # The old copy arrives late. Without the tombstone for A:1, merging it
    # would resurrect "x".
    assert "x" not in current.merge(old)
    assert "x" not in old.merge(current)


def test_round_trip_through_dict():
    s = make({"x": {"A:1", "B:1"}, "y": {"C:1"}}, {"C:1"})
    assert ORSet.from_dict(s.to_dict()) == s


@pytest.mark.parametrize(
    "bad",
    [
        "nope",
        {},
        {"adds": {}},  # no tombstones
        {"adds": [], "removed": []},
        {"adds": {"x": "A:1"}, "removed": []},  # tags not a list
        {"adds": {"x": [1]}, "removed": []},
        {"adds": {}, "removed": [1]},
    ],
)
def test_from_dict_rejects_malformed_state(bad):
    with pytest.raises(ValueError):
        ORSet.from_dict(bad)
