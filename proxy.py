"""The fault-injection proxy: a fake network between the replicas.

Every one-way link (A->B, B->A, A->C, ...) goes through its own little
server here. Replica A sends to B by connecting to the A->B link's port; the
link connects on to B and passes each message along, unless it has been told
to misbehave:

    cut          nothing gets through (the connection stays open, like a
                 real network failure: the sender isn't told)
    drop         this fraction of messages silently vanish (0.0 to 1.0)
    duplicate    this fraction of messages arrive twice
    delay_ms     every message arrives this much later
    reorder_ms   each message gets an extra random delay of up to this much,
                 so messages overtake each other

Faults apply to whole messages. TCP, underneath, never loses or reorders
bytes on its own, so "a lost message" has to be something a proxy like this
does on purpose.

The links run inside run.py (the supervisor), which controls them on request
from the dashboard and tests. Without run.py, replicas connect directly.
"""

import asyncio
import logging
import random

import websockets
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from config import ALLOWED_ORIGINS, HOST, LINK_PORTS, MAX_MESSAGE_BYTES, REPLICAS

log = logging.getLogger("proxy")

CLEAN = {"cut": False, "drop": 0.0, "duplicate": 0.0, "delay_ms": 0, "reorder_ms": 0}
MAX_DELAY_MS = 10_000


def validate_settings(changes):
    """Check a partial update to a link's settings. Returns it cleaned up."""
    if not isinstance(changes, dict) or not set(changes) <= set(CLEAN):
        raise ValueError(f"link settings may only contain {sorted(CLEAN)}")
    for key, value in changes.items():
        if key == "cut":
            if not isinstance(value, bool):
                raise ValueError("cut must be true or false")
        elif key in ("drop", "duplicate"):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
                raise ValueError(f"{key} must be between 0 and 1")
        else:
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_DELAY_MS:
                raise ValueError(f"{key} must be a whole number of ms from 0 to {MAX_DELAY_MS}")
    return {k: float(v) if k in ("drop", "duplicate") else v for k, v in changes.items()}


def plan_delivery(settings, rng):
    """Decide what happens to one message: a list of delays in seconds, one
    per copy to deliver. [] means it's dropped."""
    if settings["cut"] or rng.random() < settings["drop"]:
        return []
    copies = 2 if rng.random() < settings["duplicate"] else 1
    return [
        (settings["delay_ms"] + rng.uniform(0, settings["reorder_ms"])) / 1000
        for _ in range(copies)
    ]


class Link:
    """One direction of one connection, e.g. A->B."""

    def __init__(self, src, dst, rng):
        self.src, self.dst = src, dst
        self.port = LINK_PORTS[(src, dst)]
        self.settings = dict(CLEAN)
        self.rng = rng
        self.counts = {"passed": 0, "dropped": 0, "duplicated": 0, "reordered": 0}

    @property
    def name(self):
        return f"{self.src}>{self.dst}"

    def update(self, changes):
        self.settings.update(validate_settings(changes))

    def status(self):
        return {"from": self.src, "to": self.dst, **self.settings, **self.counts}

    async def handle(self, incoming):
        """The sender connected to us. Connect on to the receiver and pass
        messages along until either side goes away."""
        try:
            async with connect(f"ws://{HOST}:{REPLICAS[self.dst]}", max_size=MAX_MESSAGE_BYTES) as outgoing:
                await self.pump(incoming, outgoing)
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException):
            pass  # the receiver is down, or went away: drop the sender too
        finally:
            await incoming.close()  # so the sender notices and reconnects

    async def pump(self, incoming, outgoing):
        # Delayed messages are delivered by their own small tasks. They put
        # the message on this queue when their time comes; one writer sends
        # from the queue, so sends never overlap.
        ready = asyncio.Queue()
        sent_count = 0
        last_delivered = 0

        async def deliver_later(number, message, delay):
            await asyncio.sleep(delay)
            if not self.settings["cut"]:  # cut while it was in flight: lost
                await ready.put((number, message))

        async def writer():
            nonlocal last_delivered
            while True:
                number, message = await ready.get()
                if number < last_delivered:
                    self.counts["reordered"] += 1
                last_delivered = max(last_delivered, number)
                await outgoing.send(message)

        writing = asyncio.ensure_future(writer())
        pending = set()
        try:
            async for message in incoming:
                sent_count += 1
                delays = plan_delivery(self.settings, self.rng)
                if not delays:
                    self.counts["dropped"] += 1
                    continue
                self.counts["passed"] += 1
                if len(delays) > 1:
                    self.counts["duplicated"] += 1
                for delay in delays:
                    if delay == 0:
                        await ready.put((sent_count, message))
                    else:
                        task = asyncio.ensure_future(deliver_later(sent_count, message, delay))
                        pending.add(task)
                        task.add_done_callback(pending.discard)
                if writing.done():  # the receiver went away
                    break
        finally:
            writing.cancel()
            for task in pending:
                task.cancel()


class Network:
    """All six links, plus partitions."""

    def __init__(self, seed=None):
        rng = random.Random(seed)
        self.links = {
            (src, dst): Link(src, dst, random.Random(rng.random()))
            for (src, dst) in LINK_PORTS
        }

    def link(self, src, dst):
        if (src, dst) not in self.links:
            raise ValueError(f"no link {src!r} -> {dst!r}")
        return self.links[(src, dst)]

    def partition(self, groups):
        """Cut every link between different groups, in both directions.
        Links inside a group are left as they are."""
        if (not isinstance(groups, list) or not all(isinstance(g, list) for g in groups)
                or sorted(r for g in groups for r in g) != sorted(REPLICAS)):
            raise ValueError("groups must split every replica into lists, each exactly once")
        side = {r: i for i, group in enumerate(groups) for r in group}
        for (src, dst), link in self.links.items():
            if side[src] != side[dst]:
                link.update({"cut": True})

    def heal(self):
        """Every link back to normal."""
        for link in self.links.values():
            link.settings = dict(CLEAN)

    def status(self):
        return {link.name: link.status() for link in self.links.values()}

    async def serve_all(self, stack):
        """Start a server for every link. `stack` is an AsyncExitStack that
        keeps them running."""
        for link in self.links.values():
            await stack.enter_async_context(
                serve(link.handle, HOST, link.port, max_size=MAX_MESSAGE_BYTES, origins=ALLOWED_ORIGINS)
            )
