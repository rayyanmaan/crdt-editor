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


def fill_to_just_under_the_limit(r, margin):
    """Leave r's sync message `margin` bytes under the limit, with "small" in the set."""
    r.on_command({"type": "add", "element": "small"})
    # Measure the size with a one-character element, then grow that element
    # to use up all but `margin` bytes.
    probe = Replica(r.id)
    probe.incarnation, probe.slot = r.incarnation, r.slot
    probe.on_command({"type": "add", "element": "small"})
    probe.on_command({"type": "add", "element": "x"})
    one_char = len(encode(probe.sync_message(probe.counter, probe.set)))
    big = "x" * (1 + MAX_MESSAGE_BYTES - one_char - margin)
    assert r.on_command({"type": "add", "element": big})["type"] == "state"


def test_remove_that_would_make_state_too_big_is_refused():
    # A remove adds a tombstone, so it grows the state as well.
    r = Replica("A")
    fill_to_just_under_the_limit(r, margin=5)

    reply = r.on_command({"type": "remove", "element": "small"})

    assert reply["type"] == "error"
    assert "small" in r.set  # the refused remove left no trace
    assert len(encode(r.sync_message(r.counter, r.set))) <= MAX_MESSAGE_BYTES


def test_remove_with_room_to_spare_still_works():
    r = Replica("A")
    fill_to_just_under_the_limit(r, margin=500)
    assert r.on_command({"type": "remove", "element": "small"})["type"] == "state"
    assert "small" not in r.set
