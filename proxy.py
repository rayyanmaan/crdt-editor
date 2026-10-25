"""The fault-injection proxy: a fake network between the replicas.

Every one-way link (A->B, B->A, A->C, ...) goes through its own little
server here. Replica A sends to B by connecting to the A->B link's port; the
link connects on to B and passes each message along, unless it has been told
to misbehave:

    cut          nothing gets through, including messages already in flight
                 (the connection stays open, like a real network failure:
                 the sender isn't told)
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
MAX_DELAY_MS = 10_000  # for delay and reorder together

# Most messages one link holds back at once (waiting out a delay, or for a
# slow receiver). Beyond this, new messages are dropped rather than held.
MAX_IN_FLIGHT = 1000


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
        # Bumped every time the link is cut. A message stamped with an older
        # number was in flight across a cut, and is lost even if the link
        # has been healed by the time its delay is up.
        self.cut_number = 0
        self.counts = {"passed": 0, "dropped": 0, "duplicated": 0, "reordered": 0}

    @property
    def name(self):
        return f"{self.src}>{self.dst}"

    def update(self, changes):
        changes = validate_settings(changes)
        merged = {**self.settings, **changes}
        if merged["delay_ms"] + merged["reorder_ms"] > MAX_DELAY_MS:
            raise ValueError(f"delay plus reorder can be at most {MAX_DELAY_MS} ms")
        if changes.get("cut") and not self.settings["cut"]:
            self.cut_number += 1
        if merged != self.settings:
            changed = {k: v for k, v in merged.items() if v != self.settings[k]}
            log.info("link %s: %s", self.name, changed)  # every fault, in run.py's output
        self.settings = merged

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
        """Pass messages from `incoming` to `outgoing`, applying the faults.

        Three things run at once, and the first to stop ends all of them:
        reading from the sender, writing to the receiver, and watching the
        receiver's connection (so a receiver that dies is noticed at once,
        even if the sender has nothing to send).
        """
        # Copies whose delay is up wait here for the one writer, so sends
        # never overlap. Each entry: (send number, cut number, message).
        ready = asyncio.Queue()
        timers = set()  # copies still waiting out their delay
        sent = 0
        highest_delivered = 0

        def in_flight():
            return ready.qsize() + len(timers)

        def lost_to_a_cut(cut_number):
            return self.settings["cut"] or cut_number != self.cut_number

        async def wait_then_queue(entry, delay):
            await asyncio.sleep(delay)
            await ready.put(entry)

        async def read():
            nonlocal sent
            async for message in incoming:
                sent += 1
                delays = plan_delivery(self.settings, self.rng)
                if not delays or in_flight() >= MAX_IN_FLIGHT:
                    # Dropped by the settings, or too much is already held
                    # back (a slow receiver, or long delays): drop rather
                    # than hold an unbounded amount in memory.
                    self.counts["dropped"] += 1
                    continue
                if len(delays) > 1:
                    self.counts["duplicated"] += 1
                for delay in delays:
                    entry = (sent, self.cut_number, message)
                    if delay == 0:
                        await ready.put(entry)
                    else:
                        timer = asyncio.ensure_future(wait_then_queue(entry, delay))
                        timers.add(timer)
                        timer.add_done_callback(timers.discard)

        async def write():
            nonlocal highest_delivered
            while True:
                number, cut_number, message = await ready.get()
                if lost_to_a_cut(cut_number):
                    # Cut while it was in flight (or queued): lost, even if
                    # the link has since been healed.
                    self.counts["dropped"] += 1
                    continue
                await outgoing.send(message)
                self.counts["passed"] += 1
                # "Reordered" = delivered after a message that was sent later.
                if number < highest_delivered:
                    self.counts["reordered"] += 1
                highest_delivered = max(highest_delivered, number)

        tasks = [
            asyncio.ensure_future(read()),
            asyncio.ensure_future(write()),
            asyncio.ensure_future(outgoing.wait_closed()),
        ]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # Whatever stopped first, stop the rest and wait for them, so no
            # task outlives the connection and no error goes unreported.
            for task in tasks + list(timers):
                task.cancel()
            results = await asyncio.gather(*tasks, *timers, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(
                    result, (asyncio.CancelledError, websockets.WebSocketException, OSError)
                ):
                    log.error("link %s failed: %r", self.name, result)


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
        """Cut every link between different groups, in both directions, and
        reconnect every link within a group. Replaces any earlier partition
        ("cut off A" then "cut off B" leaves B alone, not everyone), but
        keeps drop, duplicate, delay and reorder settings."""
        if (not isinstance(groups, list) or not all(isinstance(g, list) for g in groups)
                or sorted(r for g in groups for r in g) != sorted(REPLICAS)):
            raise ValueError("groups must split every replica into lists, each exactly once")
        side = {r: i for i, group in enumerate(groups) for r in group}
        for (src, dst), link in self.links.items():
            link.update({"cut": side[src] != side[dst]})

    def heal(self):
        """Every link's settings back to normal. (Message counts are kept,
        and anything that was in flight across a cut stays lost.)"""
        for link in self.links.values():
            link.update(dict(CLEAN))

    def status(self):
        return {link.name: link.status() for link in self.links.values()}

    async def serve_all(self, stack):
        """Start a server for every link. `stack` is an AsyncExitStack that
        keeps them running."""
        for link in self.links.values():
            await stack.enter_async_context(
                serve(link.handle, HOST, link.port, max_size=MAX_MESSAGE_BYTES, origins=ALLOWED_ORIGINS)
            )
