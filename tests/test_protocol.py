import json

import pytest

from protocol import BadMessage, decode, encode


def test_round_trip():
    msg = {"type": "sync", "from": "A", "inc": 7, "counter": {}}
    assert decode(encode(msg)) == {"v": 1, **msg}


def test_commands_need_no_sender():
    assert decode(encode({"type": "get_state"}))["type"] == "get_state"


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[1, 2]",
        json.dumps({"type": "sync", "from": "A", "inc": 1}),  # no version
        json.dumps({"v": 2, "type": "sync", "from": "A", "inc": 1}),  # wrong version
        json.dumps({"v": 1, "type": "sync", "inc": 1}),  # no sender
        json.dumps({"v": 1, "type": "sync", "from": "A"}),  # no incarnation
        json.dumps({"v": 1, "type": "sync", "from": "A", "inc": True}),
        json.dumps({"v": 1, "type": "launch_rockets"}),
    ],
)
def test_bad_messages_are_rejected(raw):
    with pytest.raises(BadMessage):
        decode(raw)
