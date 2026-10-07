"""A replica refuses edits it couldn't replicate.

Peers reject messages over MAX_MESSAGE_BYTES. If a replica accepted an edit
that made its full state bigger than that, every sync from then on would be
rejected and it would silently stop replicating. These use the Replica class
directly; no network is needed to check the decision.
"""

from config import MAX_MESSAGE_BYTES
from protocol import encode
from replica import Replica


def test_add_that_would_make_state_too_big_is_refused():
    r = Replica("A")
    big = "x" * (MAX_MESSAGE_BYTES // 3)

    assert r.on_command({"type": "add", "element": big + "1"})["type"] == "state"
    assert r.on_command({"type": "add", "element": big + "2"})["type"] == "state"
    reply = r.on_command({"type": "add", "element": big + "3"})

    assert reply["type"] == "error"
    assert r.set.elements() == [big + "1", big + "2"]  # the refused add left no trace
    assert r.tags_made == 2
    assert len(encode(r.sync_message(r.counter, r.set))) <= MAX_MESSAGE_BYTES


def test_size_check_measures_the_whole_message():
    r = Replica("A")
    message = r.sync_message(r.counter, r.set)
    assert message["from"] == "A" and message["inc"] == r.incarnation
