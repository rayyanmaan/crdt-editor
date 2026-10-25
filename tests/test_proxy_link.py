"""One proxy link with real websockets, in one process: a sender, the link,
and a receiver that records what arrives. Checks the link's timing rules,
which the system-level tests can't pin down."""

import asyncio
import random

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

import proxy
from config import HOST

DEST_PORT, LINK_PORT = 9301, 9302


class Receiver:
    def __init__(self):
        self.got = []
        self.server = None

    async def handle(self, ws):
        async for message in ws:
            self.got.append(message)

    async def start(self):
        self.server = await serve(self.handle, HOST, DEST_PORT).__aenter__()

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()


@pytest.fixture(autouse=True)
def one_link_only(monkeypatch):
    # Point the A->B link at our receiver. monkeypatch puts the real values
    # back after each test, so other tests aren't affected.
    monkeypatch.setattr(proxy, "REPLICAS", {"A": 0, "B": DEST_PORT})
    monkeypatch.setattr(proxy, "LINK_PORTS", {("A", "B"): LINK_PORT})


async def with_link(scenario, **settings):
    link = proxy.Link("A", "B", random.Random(1))
    link.update(settings)
    receiver = Receiver()
    await receiver.start()
    server = await serve(link.handle, HOST, LINK_PORT).__aenter__()
    try:
        async with connect(f"ws://{HOST}:{LINK_PORT}") as sender:
            return await scenario(link, sender, receiver)
    finally:
        server.close()
        await server.wait_closed()
        await receiver.stop()


async def until(check, timeout=3.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if check():
            return True
        await asyncio.sleep(0.02)
    return False


def run(coro):
    return asyncio.run(coro)


def test_a_clean_link_delivers_everything_in_order():
    async def scenario(link, sender, receiver):
        for i in range(200):
            await sender.send(str(i))
        await until(lambda: len(receiver.got) == 200)
        return receiver.got
    assert run(with_link(scenario)) == [str(i) for i in range(200)]


def test_delayed_messages_are_lost_if_the_link_is_cut_while_they_are_in_flight():
    async def scenario(link, sender, receiver):
        await sender.send("in flight")
        await asyncio.sleep(0.1)
        link.update({"cut": True})
        await asyncio.sleep(0.6)
        return receiver.got
    assert run(with_link(scenario, delay_ms=400)) == []


def test_a_cut_then_heal_while_in_flight_still_loses_them():
    # The message was on the wire when the link was cut. Healing later
    # doesn't bring it back: a partition loses what was in transit.
    async def scenario(link, sender, receiver):
        await sender.send("in flight")
        await asyncio.sleep(0.1)
        link.update({"cut": True})
        await asyncio.sleep(0.1)
        link.update({"cut": False})
        await asyncio.sleep(0.6)
        return receiver.got
    assert run(with_link(scenario, delay_ms=400)) == []


def test_sender_is_disconnected_promptly_when_the_receiver_dies_even_if_idle():
    async def scenario(link, sender, receiver):
        await sender.send("hello")
        await until(lambda: receiver.got == ["hello"])
        await receiver.stop()  # the receiving replica crashes
        # The sender sends nothing more. It should still find out soon.
        return await until(lambda: sender.state.name == "CLOSED", timeout=3)
    assert run(with_link(scenario)) is True


def test_counts_describe_what_was_delivered():
    async def scenario(link, sender, receiver):
        await sender.send("in flight")
        await asyncio.sleep(0.1)
        link.update({"cut": True})
        await asyncio.sleep(0.5)
        return dict(link.counts)
    counts = run(with_link(scenario, delay_ms=300))
    assert counts["passed"] == 0 and counts["dropped"] == 1  # it never arrived


def test_a_link_holds_back_a_bounded_number_of_messages(monkeypatch):
    monkeypatch.setattr(proxy, "MAX_IN_FLIGHT", 10)

    async def scenario(link, sender, receiver):
        for i in range(50):
            await sender.send(str(i))
        await until(lambda: link.counts["dropped"] == 40)
        await until(lambda: len(receiver.got) == 10)
        return link.counts["dropped"], len(receiver.got)
    assert run(with_link(scenario, delay_ms=300)) == (40, 10)


def test_reordered_counts_messages_that_arrive_after_a_later_one():
    async def scenario(link, sender, receiver):
        for i in range(30):
            await sender.send(str(i))
        await until(lambda: len(receiver.got) == 30)
        inversions = sum(
            1 for k, m in enumerate(receiver.got) if int(m) < max(int(x) for x in receiver.got[:k + 1])
        )
        return inversions, link.counts["reordered"]
    inversions, counted = run(with_link(scenario, reorder_ms=300))
    assert counted == inversions > 0
