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

The dashboard and the tests also connect to the server, to send commands
("increment", "add", "remove", "get_state") and read the replies.
"""

import asyncio
import logging
import sys
import time

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from config import (
    HOST, MAX_MESSAGE_BYTES, OUTBOX_LIMIT, RECONNECT_DELAY, REPLICAS, SUSPECT_AFTER, SYNC_INTERVAL,
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
            # changes nothing.
            try:
                their_counter = GCounter.from_dict(msg.get("counter"))
                their_set = ORSet.from_dict(msg.get("set"))
            except ValueError as e:
                self.log.warning("bad sync from %s: %s", sender, e)
                return
            self.counter = self.counter.merge(their_counter)
            self.set = self.set.merge(their_set)

    def on_command(self, msg):
        kind = msg["type"]
        if kind == "increment":
            try:
                self.counter.increment(self.slot, msg.get("amount", 1))
            except ValueError as e:
                return {"type": "error", "error": str(e)}
            return self.state()

        if kind in ("add", "remove"):
            element = msg.get("element")
            if not isinstance(element, str) or not element:
                return {"type": "error", "error": "element must be a non-empty string"}
            if kind == "add":
                self.tags_made += 1
                tag = f"{self.slot}:{self.tags_made}"
                self.set.add(element, tag)
                return {**self.state(), "tag": tag}
            tombstoned = self.set.remove(element)
            return {**self.state(), "tombstoned": sorted(tombstoned)}

        return self.state()

    def state(self):
        """Everything the dashboard and tests need to see about this replica."""
        return {
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
                }
                for p in self.peers
            },
        }

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
            sync = {"type": "sync", "counter": self.counter.to_dict(), "set": self.set.to_dict()}
            # Full state only grows (tombstones are never deleted). A peer
            # rejects messages over MAX_MESSAGE_BYTES, so say so loudly rather
            # than failing quietly on every sync.
            if len(encode(sync)) > MAX_MESSAGE_BYTES:
                self.log.error("state is too big to sync (over %d bytes)", MAX_MESSAGE_BYTES)
            for peer in self.peers:
                self.send(peer, sync)
            await asyncio.sleep(SYNC_INTERVAL)

    # ---- main ------------------------------------------------------------

    async def run(self):
        self.outbox = {peer: asyncio.Queue(OUTBOX_LIMIT) for peer in self.peers}
        async with serve(self.handle_incoming, HOST, self.port, max_size=MAX_MESSAGE_BYTES):
            self.log.info("listening on port %d (incarnation %d)", self.port, self.incarnation)
            await asyncio.gather(
                self.sync_loop(),
                *(self.keep_link_open(peer) for peer in self.peers),
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
