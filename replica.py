"""One replica of the shared document, running as its own process.

Usage:  python replica.py A

How replicas talk to each other
-------------------------------
Every replica runs a websocket *server* that other replicas send messages to.
For every peer, it also opens an outgoing *client* connection and uses it only
for sending. So between A and B there are two one-way links, A->B and B->A.

One-way links are on purpose: later, the fault-injection proxy sits on a link
and can break one direction without the other (A hears B, but B can't hear A),
which is a realistic kind of network failure.

Right now the only messages are "hello" and "heartbeat". The CRDT comes next.
"""

import asyncio
import json
import logging
import sys
import time

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from config import HEARTBEAT_INTERVAL, HOST, RECONNECT_DELAY, REPLICAS


class Replica:
    def __init__(self, replica_id):
        self.id = replica_id
        self.port = REPLICAS[replica_id]
        self.peers = [p for p in REPLICAS if p != replica_id]

        # Messages waiting to go out on each outgoing link. Filled in by run():
        # on Python 3.9 a Queue must be created inside the running event loop.
        self.outbox = {}

        # Peers whose outgoing link is currently open.
        self.connected = set()

        # When we last received anything from each peer. A peer that has gone
        # quiet might have crashed, or the network might just be slow; from
        # here we can't tell the difference.
        self.last_heard = {}

        self.log = logging.getLogger(replica_id)

    # ---- receiving -------------------------------------------------------

    async def handle_incoming(self, ws):
        """Runs once per incoming connection, for as long as it stays open."""
        try:
            async for raw in ws:
                self.on_message(json.loads(raw))
        except websockets.ConnectionClosed:
            pass  # the sender went away; it will reconnect when it can

    def on_message(self, msg):
        sender = msg["from"]
        self.last_heard[sender] = time.time()
        if msg["type"] == "hello":
            self.log.info("%s says hello", sender)

    # ---- sending ---------------------------------------------------------

    def send(self, peer, msg):
        """Queue a message for a peer. Only sent if the link is up."""
        if peer in self.connected:
            self.outbox[peer].put_nowait({**msg, "from": self.id})

    async def keep_link_open(self, peer):
        """Keep an outgoing link to one peer alive, reconnecting forever.

        If the send fails the message is lost. That's fine for now: once
        replicas exchange edits, catch-up after reconnecting will fill in
        anything that was lost here.
        """
        url = f"ws://{HOST}:{REPLICAS[peer]}"
        while True:
            try:
                async with connect(url) as ws:
                    self.connected.add(peer)
                    self.log.info("link to %s is up", peer)
                    await ws.send(json.dumps({"type": "hello", "from": self.id}))
                    while True:
                        msg = await self.outbox[peer].get()
                        await ws.send(json.dumps(msg))
            except (OSError, websockets.ConnectionClosed):
                if peer in self.connected:
                    self.log.warning("link to %s is down, retrying", peer)
                self.connected.discard(peer)
                # Drop anything queued for the dead link.
                self.outbox[peer] = asyncio.Queue()
                await asyncio.sleep(RECONNECT_DELAY)

    async def heartbeat(self):
        """Ping every peer regularly.

        Besides letting peers know we're alive, this is how we notice a dead
        link: a link only reports that it's broken when we try to send on it.
        """
        while True:
            for peer in self.peers:
                self.send(peer, {"type": "heartbeat"})
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    # ---- main ------------------------------------------------------------

    async def run(self):
        self.outbox = {peer: asyncio.Queue() for peer in self.peers}
        async with serve(self.handle_incoming, HOST, self.port):
            self.log.info("listening on port %d", self.port)
            await asyncio.gather(
                self.heartbeat(),
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
