"""How a replica treats messages from its peers' past runs.

Uses the Replica class directly: no network needed to check the decision.
"""

from replica import Replica


def hello(sender, inc):
    return {"v": 1, "type": "hello", "from": sender, "inc": inc}


def test_a_late_message_from_a_dead_run_is_not_a_sign_of_life():
    a = Replica("A")
    a.on_peer_message(hello("B", 100))
    a.on_peer_message(hello("B", 200))  # B restarted
    a.last_heard["B"] -= 60  # and then went quiet a minute ago
    assert a.peer_status("B") == "suspected"

    # A message from B's old run (100) finally arrives, delayed. It must not
    # make the dead B look alive, or roll back which run A thinks is current.
    a.on_peer_message(hello("B", 100))
    assert a.peer_status("B") == "suspected"
    assert a.peer_incarnation["B"] == 200


def test_its_data_is_still_used():
    a = Replica("A")
    a.on_peer_message(hello("B", 200))
    old_sync = {"v": 1, "type": "sync", "from": "B", "inc": 100,
                "counter": {"B#100": 4}, "set": {"adds": {}, "removed": []}}
    a.on_peer_message(old_sync)
    assert a.counter.value() == 4  # the CRDT state is valid whoever delivers it
