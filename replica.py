"""One replica of the shared state, running as its own process.

Usage:  python replica.py A

How replicas talk to each other
-------------------------------
Every replica runs a websocket *server* that other replicas send messages to.
For every peer, it also opens an outgoing *client* connection and uses it only
for sending. So between A and B there are two one-way links, A->B and B->A.

One-way links are on purpose: later, the fault-injection proxy sits on a link
and can break one direction without the other (A hears B, but B can't hear A),
which is a realistic kind of network failure.

What they say
-------------
Every SYNC_INTERVAL, each replica sends its whole state (counter and set) to
every peer, and a peer that receives it merges it into its own. This is
anti-entropy: no message has to arrive, because a later sync carries
everything an earlier one did. The same message works as a heartbeat for the
failure detector.

The text works differently, because sending a whole document every second
would be wasteful. Each edit is sent once as an op ("ops" message). The sync
also carries the text's version vector, and a peer that sees we're missing
ops sends them back ("ops" again, in batches). So a lost op is repaired at
the next sync, as long as some live replica has it. See causal.py.

The dashboard and the tests also connect to the server, to send commands
("increment", "add", "remove", "insert", "delete", "get_state") and read the
replies.
"""

import asyncio
import logging
import sys
import time

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from causal import CausalText
from config import (
    ALLOWED_ORIGINS, CAUSAL_BUFFER, HOST, MAX_INSERT_CHARS, MAX_MESSAGE_BYTES, OUTBOX_LIMIT,
    RECONNECT_DELAY, REPAIR_BATCH, REPLICAS, SUSPECT_AFTER, SYNC_INTERVAL,
)
from gcounter import GCounter
from orset import ORSet
from protocol import COMMAND_TYPES, BadMessage, decode, encode


class Replica:
    def __init__(self, replica_id):
        self.id = replica_id
        self.port = REPLICAS[replica_id]
        self.peers = [p for p in REPLICAS if p != replica_id]

        # Which run of this replica this is. A restarted replica gets a new
        # incarnation, so nothing it creates can clash with what its previous
        # run created. It only has to be unique per replica: slots are named
        # "A#...", "B#...", so A and B may share a number. Milliseconds since
        # 1970 is unique enough here, but it relies on the clock never going
        # backwards. (Once state is saved to disk, this becomes a saved counter.)
        self.incarnation = time.time_ns() // 1_000_000
        self.slot = f"{self.id}#{self.incarnation}"

        self.counter = GCounter()
        self.set = ORSet()
        # Tags for set adds are "<slot>:<n>". The slot includes the
        # incarnation, so a restarted replica counting n from 1 again still
        # makes tags nobody has used.
        self.tags_made = 0
        # The shared text. Its ops are named after this run (the slot), so a
        # restarted replica starts a fresh sequence of op numbers.
        self.text = CausalText(self.slot, causal=CAUSAL_BUFFER)

        # Messages waiting to go out on each outgoing link. Filled in by run():
        # on Python 3.9 a Queue must be created inside the running event loop.
        self.outbox = {}

        # Peers whose outgoing link is currently open.
        self.connected = set()

        # When we last received anything from each peer, and which run of it
        # that was. Uses time.monotonic(), which never jumps backwards the way
        # the wall clock can.
        self.last_heard = {}
        self.peer_incarnation = {}

        self.log = logging.getLogger(replica_id)

    # ---- failure detector ------------------------------------------------

    def peer_status(self, peer):
        """'up', 'suspected' or 'unknown'.

        'suspected' is a guess: a silent peer might have crashed, or the
        network between us might be slow or broken. From here we can't tell.
        """
        if peer not in self.last_heard:
            return "unknown"
        if time.monotonic() - self.last_heard[peer] > SUSPECT_AFTER:
            return "suspected"
        return "up"

    # ---- receiving -------------------------------------------------------

    async def handle_incoming(self, ws):
        """Runs once per incoming connection, for as long as it stays open."""
        try:
            async for raw in ws:
                try:
                    msg = decode(raw)
                except BadMessage as e:
                    self.log.warning("ignoring bad message: %s", e)
                    continue
                if msg["type"] in COMMAND_TYPES:
                    await ws.send(encode(self.on_command(msg)))
                else:
                    self.on_peer_message(msg)
        except websockets.ConnectionClosed:
            pass  # the sender went away; it will reconnect when it can

    def on_peer_message(self, msg):
        sender = msg["from"]
        if sender not in self.peers:
            self.log.warning("ignoring message from unknown replica %r", sender)
            return

        self.last_heard[sender] = time.monotonic()
        if self.peer_incarnation.get(sender) != msg["inc"]:
            if sender in self.peer_incarnation:
                self.log.info("%s restarted (new incarnation %d)", sender, msg["inc"])
            self.peer_incarnation[sender] = msg["inc"]

        if msg["type"] == "hello":
            self.log.info("%s says hello", sender)
        elif msg["type"] == "sync":
            # Check both parts before merging either, so a half-bad message
            # changes neither the counter nor the set. (It still counts as
            # hearing from the peer.)
            try:
                their_counter = GCounter.from_dict(msg.get("counter"))
                their_set = ORSet.from_dict(msg.get("set"))
            except ValueError as e:
                self.log.warning("bad sync from %s: %s", sender, e)
                return
            their_vector = msg.get("vv")
            if not valid_vector(their_vector):
                self.log.warning("bad version vector from %s", sender)
                return
            self.counter = self.counter.merge(their_counter)
            self.set = self.set.merge(their_set)
            # Repair: send the peer text ops it doesn't have. They go out on
            # our own link to that peer; incoming links are receive-only.
            missing = self.text.missing_for(their_vector, REPAIR_BATCH)
            if missing:
                self.send(sender, {"type": "ops", "ops": missing})
        elif msg["type"] == "ops":
            ops = msg.get("ops")
            if not isinstance(ops, list) or len(ops) > REPAIR_BATCH:
                self.log.warning("bad ops message from %s", sender)
                return
            for envelope in ops:
                try:
                    self.text.receive(envelope)
                except ValueError as e:
                    self.log.warning("bad op from %s: %s", sender, e)

    def on_command(self, msg):
        kind = msg["type"]
        if kind == "increment":
            counter = self.counter.merge(GCounter())  # a copy to try it on
            try:
                counter.increment(self.slot, msg.get("amount", 1))
            except ValueError as e:
                return {"type": "error", "error": str(e)}
            if self.too_big_to_sync(counter, self.set):
                return {"type": "error", "error": "state is too big to sync"}
            self.counter = counter
            return self.state()

        if kind in ("add", "remove"):
            element = msg.get("element")
            if not isinstance(element, str) or not element:
                return {"type": "error", "error": "element must be a non-empty string"}
            if kind == "add":
                tag = f"{self.slot}:{self.tags_made + 1}"
                new_set = self.set.merge(ORSet())  # a copy to try it on
                new_set.add(element, tag)
                # Refuse an edit we couldn't replicate, rather than accept it
                # and have every peer reject our syncs from then on.
                if self.too_big_to_sync(self.counter, new_set):
                    return {"type": "error", "error": "state is too big to sync"}
                self.tags_made += 1
                self.set = new_set
                return {**self.state(), "tag": tag}
            # A remove adds tombstones, so it makes the state bigger too.
            new_set = self.set.merge(ORSet())
            tombstoned = new_set.remove(element)
            if self.too_big_to_sync(self.counter, new_set):
                return {"type": "error", "error": "state is too big to sync"}
            self.set = new_set
            return {**self.state(), "tombstoned": sorted(tombstoned)}

        if kind == "insert":
            return self.on_insert(msg)
        if kind == "delete":
            return self.on_delete(msg)

        return self.state(full=msg.get("full") is True)

    def on_insert(self, msg):
        """Type `text` starting at position `index`, one character at a time."""
        index, text = msg.get("index"), msg.get("text")
        if not isinstance(text, str) or not 1 <= len(text) <= MAX_INSERT_CHARS:
            return {"type": "error", "error": f"text must be 1 to {MAX_INSERT_CHARS} characters"}
        if not is_index(index) or index > len(self.text.text()):
            return {"type": "error", "error": "index out of range"}
        if any(0xD800 <= ord(ch) <= 0xDFFF for ch in text):
            return {"type": "error", "error": "text contains a lone surrogate"}
        # Checked everything first, so the edit happens completely or not at all.
        envelopes = [self.text.insert(index + i, ch) for i, ch in enumerate(text)]
        self.broadcast_ops(envelopes)
        return {**self.state(), "ops": envelopes}

    def on_delete(self, msg):
        """Delete `count` characters starting at position `index`."""
        index, count = msg.get("index"), msg.get("count", 1)
        if not is_index(index) or not is_index(count) or count < 1:
            return {"type": "error", "error": "index and count must be whole numbers, count >= 1"}
        if index + count > len(self.text.text()):
            return {"type": "error", "error": "range out of the text"}
        envelopes = [self.text.delete(index) for _ in range(count)]
        self.broadcast_ops(envelopes)
        return {**self.state(), "ops": envelopes}

    def broadcast_ops(self, envelopes):
        """Send new ops to every peer once. If a message is lost, the peer's
        next sync shows the gap and we send them again (repair)."""
        for start in range(0, len(envelopes), REPAIR_BATCH):
            batch = envelopes[start:start + REPAIR_BATCH]
            for peer in self.peers:
                self.send(peer, {"type": "ops", "ops": batch})

    def sync_message(self, counter, set_):
        """The exact message a sync sends, including the envelope fields."""
        return {
            "type": "sync", "from": self.id, "inc": self.incarnation,
            "counter": counter.to_dict(), "set": set_.to_dict(),
            "vv": self.text.version_vector(),
        }

    def too_big_to_sync(self, counter, set_):
        return len(encode(self.sync_message(counter, set_))) > MAX_MESSAGE_BYTES

    def state(self, full=False):
        """Everything the dashboard and tests need to see about this replica.

        full=True adds the whole text document, tombstones included, which
        tests use to check nothing was lost. The dashboard doesn't need it.
        """
        state = {
            "type": "state",
            "id": self.id,
            "incarnation": self.incarnation,
            "counter": self.counter.value(),
            "slots": self.counter.to_dict(),
            "set": self.set.elements(),
            "set_state": self.set.to_dict(),
            "peers": {
                p: {
                    "status": self.peer_status(p),
                    "link_up": p in self.connected,
                    "incarnation": self.peer_incarnation.get(p),
                    # Seconds since we last heard from them, for the dashboard.
                    "last_heard_ago": (
                        round(time.monotonic() - self.last_heard[p], 1)
                        if p in self.last_heard else None
                    ),
                }
                for p in self.peers
            },
            "text": self.text.summary(),
        }
        if full:
            state["text_doc"] = self.text.doc.state()
        return state

    # ---- sending ---------------------------------------------------------

    def send(self, peer, msg):
        """Queue a message for a peer. Only sent if the link is up."""
        if peer not in self.connected:
            return
        try:
            self.outbox[peer].put_nowait({**msg, "from": self.id, "inc": self.incarnation})
        except asyncio.QueueFull:
            # The link is backed up, so this message is dropped. That's safe
            # only because every sync carries the full state.
            pass

    async def keep_link_open(self, peer):
        """Keep an outgoing link to one peer alive, reconnecting forever.

        If a send fails, that message is lost. That's fine: the next sync
        carries the full state again.
        """
        url = f"ws://{HOST}:{REPLICAS[peer]}"
        while True:
            try:
                async with connect(url) as ws:
                    self.connected.add(peer)
                    self.log.info("link to %s is up", peer)
                    await ws.send(encode({"type": "hello", "from": self.id, "inc": self.incarnation}))
                    while True:
                        msg = await self.outbox[peer].get()
                        await ws.send(encode(msg))
            # OSError: peer not listening. TimeoutError: the connection attempt
            # stalled (on Python 3.9 asyncio's TimeoutError is not an OSError).
            # WebSocketException: the link closed, or the handshake failed.
            except (OSError, asyncio.TimeoutError, websockets.WebSocketException):
                if peer in self.connected:
                    self.log.warning("link to %s is down, retrying", peer)
                self.connected.discard(peer)
                # Drop anything queued for the dead link.
                self.outbox[peer] = asyncio.Queue(OUTBOX_LIMIT)
                await asyncio.sleep(RECONNECT_DELAY)

    async def sync_loop(self):
        """Send our full state to every peer, forever.

        Regular sends are also how we notice a dead link quickly: a send on a
        closed connection fails at once. (The websockets library also pings in
        the background, but only every 20 seconds.)
        """
        while True:
            sync = self.sync_message(self.counter, self.set)
            # Full state only grows (tombstones are never deleted). Edits that
            # would make it too big are refused, but merging two peers' states
            # can still push it over. A peer rejects messages over
            # MAX_MESSAGE_BYTES, so say so loudly rather than fail quietly.
            if self.too_big_to_sync(self.counter, self.set):
                self.log.error("state is too big to sync (over %d bytes)", MAX_MESSAGE_BYTES)
            for peer in self.peers:
                self.send(peer, sync)
            await asyncio.sleep(SYNC_INTERVAL)

    # ---- main ------------------------------------------------------------

    async def run(self):
        self.outbox = {peer: asyncio.Queue(OUTBOX_LIMIT) for peer in self.peers}
        async with serve(
            self.handle_incoming, HOST, self.port,
            max_size=MAX_MESSAGE_BYTES, origins=ALLOWED_ORIGINS,
        ):
            self.log.info("listening on port %d (incarnation %d)", self.port, self.incarnation)
            await asyncio.gather(
                self.sync_loop(),
                *(self.keep_link_open(peer) for peer in self.peers),
            )


def is_index(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def valid_vector(vector):
    """A version vector from a peer: {origin: count of ops with no gap}."""
    return isinstance(vector, dict) and all(
        isinstance(origin, str) and is_index(count) for origin, count in vector.items()
    )


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in REPLICAS:
        sys.exit(f"usage: python replica.py <{'|'.join(REPLICAS)}>")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)  # hide library chatter
    try:
        asyncio.run(Replica(sys.argv[1]).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
