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
Every SYNC_INTERVAL, each replica sends its whole counter to every peer, and
a peer that receives it merges it into its own. This is anti-entropy: no
message has to arrive, because a later sync carries everything an earlier one
did. The same message works as a heartbeat for the failure detector.

The dashboard and the tests also connect to the server, to send commands
("increment", "get_state") and read the replies.
"""

import asyncio
import logging
import sys
import time

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from config import HOST, OUTBOX_LIMIT, RECONNECT_DELAY, REPLICAS, SUSPECT_AFTER, SYNC_INTERVAL
from gcounter import GCounter
from protocol import COMMAND_TYPES, BadMessage, decode, encode


class Replica:
    def __init__(self, replica_id):
        self.id = replica_id
        self.port = REPLICAS[replica_id]
        self.peers = [p for p in REPLICAS if p != replica_id]

        # Which run of this replica this is. A restarted replica gets a new
        # incarnation, so nothing it creates can clash with what its previous
        # run created. Milliseconds since 1970 is unique enough on one laptop.
        # (Once state is saved to disk, this becomes a saved counter.)
        self.incarnation = time.time_ns() // 1_000_000
        self.slot = f"{self.id}#{self.incarnation}"

        self.counter = GCounter()

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
            try:
                theirs = GCounter.from_dict(msg.get("counter"))
            except ValueError as e:
                self.log.warning("bad counter from %s: %s", sender, e)
                return
            self.counter = self.counter.merge(theirs)

    def on_command(self, msg):
        if msg["type"] == "increment":
            amount = msg.get("amount", 1)
            if not isinstance(amount, int) or isinstance(amount, bool) or amount < 1:
                return {"type": "error", "error": "amount must be a positive integer"}
            self.counter.increment(self.slot, amount)
        return self.state()

    def state(self):
        """Everything the dashboard and tests need to see about this replica."""
        return {
            "type": "state",
            "id": self.id,
            "incarnation": self.incarnation,
            "counter": self.counter.value(),
            "slots": self.counter.to_dict(),
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
            pass  # the link is backed up; a later sync will carry the same state

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
            except (OSError, websockets.ConnectionClosed):
                if peer in self.connected:
                    self.log.warning("link to %s is down, retrying", peer)
                self.connected.discard(peer)
                # Drop anything queued for the dead link.
                self.outbox[peer] = asyncio.Queue(OUTBOX_LIMIT)
                await asyncio.sleep(RECONNECT_DELAY)

    async def sync_loop(self):
        """Send our full state to every peer, forever.

        This is also what makes a dead link visible: a link only reports that
        it's broken when we try to send on it.
        """
        while True:
            for peer in self.peers:
                self.send(peer, {"type": "sync", "counter": self.counter.to_dict()})
            await asyncio.sleep(SYNC_INTERVAL)

    # ---- main ------------------------------------------------------------

    async def run(self):
        self.outbox = {peer: asyncio.Queue(OUTBOX_LIMIT) for peer in self.peers}
        async with serve(self.handle_incoming, HOST, self.port):
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
