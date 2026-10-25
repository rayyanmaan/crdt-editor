"""Unit tests for the proxy's decisions (proxy.py), with no network."""

import random

import pytest

from proxy import CLEAN, Network, plan_delivery, validate_settings


def plan(**settings):
    return plan_delivery({**CLEAN, **settings}, random.Random(1))


def test_clean_link_delivers_once_at_once():
    assert plan() == [0.0]


def test_cut_and_full_drop_deliver_nothing():
    assert plan(cut=True) == []
    assert plan(drop=1.0) == []


def test_full_duplicate_delivers_twice():
    assert len(plan(duplicate=1.0)) == 2


def test_delay_and_reorder_jitter():
    rng = random.Random(7)
    delays = [plan_delivery({**CLEAN, "delay_ms": 100, "reorder_ms": 50}, rng)[0] for _ in range(200)]
    assert all(0.1 <= d <= 0.15 for d in delays)
    assert len(set(delays)) > 100  # really random, so messages overtake each other


def test_drop_rate_is_roughly_what_was_asked():
    rng = random.Random(3)
    delivered = sum(bool(plan_delivery({**CLEAN, "drop": 0.3}, rng)) for _ in range(5000))
    assert 0.65 < delivered / 5000 < 0.75


@pytest.mark.parametrize(
    "bad",
    [
        {"drop": 1.5}, {"drop": -0.1}, {"drop": True}, {"duplicate": "half"},
        {"cut": 1}, {"delay_ms": -1}, {"delay_ms": 0.5}, {"reorder_ms": 10_001},
        {"teleport": True}, "cut",
    ],
)
def test_bad_settings_are_rejected(bad):
    with pytest.raises(ValueError):
        validate_settings(bad)


def test_partition_cuts_exactly_the_links_between_groups():
    net = Network(seed=1)
    net.partition([["A"], ["B", "C"]])
    cut = {name for name, link in net.status().items() if link["cut"]}
    assert cut == {"A>B", "B>A", "A>C", "C>A"}  # B<->C still fine


def test_heal_resets_every_link():
    net = Network(seed=1)
    net.link("A", "B").update({"drop": 0.5, "delay_ms": 300})
    net.partition([["A"], ["B", "C"]])
    net.heal()
    assert all(
        {k: link[k] for k in CLEAN} == CLEAN for link in net.status().values()
    )


@pytest.mark.parametrize(
    "groups",
    [[["A"], ["B"]], [["A", "B"], ["B", "C"]], [["A", "B", "C", "D"]], "A|BC", [["A"], "BC"]],
)
def test_partition_must_name_every_replica_once(groups):
    with pytest.raises(ValueError):
        Network(seed=1).partition(groups)


def test_a_new_partition_replaces_the_old_one():
    net = Network(seed=1)
    net.link("A", "B").update({"drop": 0.5})
    net.partition([["A"], ["B", "C"]])
    net.partition([["B"], ["A", "C"]])  # "cut off A", then "cut off B"
    cut = {name for name, link in net.status().items() if link["cut"]}
    assert cut == {"A>B", "B>A", "B>C", "C>B"}  # only B is alone now
    assert net.status()["A>B"]["drop"] == 0.5  # other settings kept


def test_delay_and_reorder_together_are_capped():
    net = Network(seed=1)
    net.link("A", "B").update({"delay_ms": 8000})
    with pytest.raises(ValueError):
        net.link("A", "B").update({"reorder_ms": 3000})
    assert net.status()["A>B"]["reorder_ms"] == 0  # the refused change left nothing behind
