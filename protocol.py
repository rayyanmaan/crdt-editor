"""The format of every message on the wire.

Every message is one JSON object in one websocket message, with a version
number and a type:

    {"v": 1, "type": "sync", "from": "A", "inc": 1729500000123, ...}

Two kinds of message arrive at a replica:
- From other replicas ("hello", "sync", "vv", "ops"). These also say which replica sent
  them ("from") and which run of that replica ("inc", the incarnation).
- From the dashboard or the tests ("increment", "add", "remove", "insert",
  "delete", "get_state"). These get a reply on the same connection.

The supervisor (run.py) uses the same format for its own commands ("start",
"kill", "status").

Anything that doesn't fit is rejected with BadMessage, so a malformed message
gets logged and ignored and can't crash a replica.

Replicas trust each other: this system handles replicas that crash or lose
messages, not replicas that lie (Byzantine faults).
"""

import json

VERSION = 1

PEER_TYPES = {"hello", "sync", "vv", "ops"}
COMMAND_TYPES = {"increment", "add", "remove", "insert", "delete", "get_state"}
SUPERVISOR_TYPES = {"start", "kill", "status"}


class BadMessage(Exception):
    pass


def encode(msg):
    return json.dumps({"v": VERSION, **msg})


def is_int(value):
    """True for real integers. bool is a subclass of int in Python, so rule it out."""
    return isinstance(value, int) and not isinstance(value, bool)


def decode(raw, commands=COMMAND_TYPES):
    """Parse and check one message. `commands` is which commands the receiver
    accepts: replicas take COMMAND_TYPES, the supervisor SUPERVISOR_TYPES."""
    try:
        msg = json.loads(raw)
    except (ValueError, RecursionError):  # RecursionError: absurdly deep nesting
        raise BadMessage("not valid JSON")
    if not isinstance(msg, dict):
        raise BadMessage("not a JSON object")
    if not is_int(msg.get("v")) or msg["v"] != VERSION:
        raise BadMessage(f"unsupported version {msg.get('v')!r}")

    kind = msg.get("type")
    if not isinstance(kind, str):
        raise BadMessage("missing message type")
    if kind in commands:
        return msg
    if kind in PEER_TYPES:
        if not isinstance(msg.get("from"), str):
            raise BadMessage("missing sender")
        if not is_int(msg.get("inc")):
            raise BadMessage("missing incarnation")
        return msg
    raise BadMessage(f"unknown message type {kind!r}")
