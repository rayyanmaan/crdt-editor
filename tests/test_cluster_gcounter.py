"""The G-Counter running on three real replica processes.

These tests check that every increment arrives everywhere, not just that the
replicas agree: three replicas that all lost the same increment would agree
too. Each test works out the exact slots it expects and checks every replica
has exactly those.
"""

from websockets.sync.client import connect

from cluster import wait_until
from config import HOST, REPLICA_IDS


def slots_everywhere(cluster, expected):
    return all(cluster.state(rid)["slots"] == expected for rid in REPLICA_IDS)


def test_increments_reach_every_replica(cluster):
    expected = {}
    for rid, amount in [("A", 3), ("B", 2), ("C", 1)]:
        reply = cluster.increment(rid, amount)
        expected[f"{rid}#{reply['incarnation']}"] = amount

    wait_until(lambda: slots_everywhere(cluster, expected))
    assert all(cluster.state(rid)["counter"] == 6 for rid in REPLICA_IDS)


def test_killed_replica_is_suspected(cluster):
    cluster.kill("C")
    wait_until(lambda: cluster.state("A")["peers"]["C"]["status"] == "suspected", timeout=6)
    assert cluster.state("A")["peers"]["B"]["status"] == "up"


def test_restarted_replica_catches_up_and_keeps_counting(cluster):
    # B counts to 5 and A and C both see it.
    old_b = cluster.increment("B", 5)["incarnation"]
    expected = {f"B#{old_b}": 5}
    wait_until(lambda: slots_everywhere(cluster, expected))

    # B crashes. A keeps counting while B is down.
    cluster.kill("B")
    a = cluster.increment("A", 1)["incarnation"]
    expected[f"A#{a}"] = 1

    # B comes back empty, with a new incarnation, and counts again right away,
    # before it has caught up.
    cluster.start("B")
    reply = wait_until(lambda: cluster.increment("B", 1))
    new_b = reply["incarnation"]
    assert new_b != old_b
    expected[f"B#{new_b}"] = 1

    # Nothing is lost: B's old count came back from its peers, and its new
    # count didn't get hidden under the old one.
    wait_until(lambda: slots_everywhere(cluster, expected))
    assert all(cluster.state(rid)["counter"] == 7 for rid in REPLICA_IDS)


def test_bad_message_does_not_crash_a_replica(cluster):
    with connect(f"ws://{HOST}:{cluster.port('A')}") as ws:
        ws.send("this is not json")
        ws.send('{"v": 1, "type": "sync", "from": "Z", "inc": 1, "counter": {}}')
        ws.send('{"v": 1, "type": "sync", "from": "B", "inc": 1, "counter": {"B#1": -5}}')
    assert cluster.state("A")["counter"] == 0
