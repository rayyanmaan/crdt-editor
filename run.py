"""Start every replica as a separate process and keep control of them.

Usage:  python run.py        (Ctrl-C stops everything)

Each replica is a real OS process with its own memory, so the only way they
can share anything is over the network. You can also start them one by one
in separate terminals with `python replica.py A` and so on.

run.py is a supervisor: besides starting the replicas, it listens on
SUPERVISOR_PORT for commands, so the dashboard and tests can crash and restart
replicas on demand. Open http://localhost:<SUPERVISOR_PORT>/ (8000 by default)
in a browser for the dashboard, which this same port serves.

It also runs the fault-injection proxy (proxy.py): every link between
replicas goes through it, so the network can be broken on demand.

Commands:

    {"v": 1, "type": "kill",  "replica": "B"}   SIGKILL, like `kill -9`
    {"v": 1, "type": "start", "replica": "B"}   start it again (new incarnation)
    {"v": 1, "type": "link", "from": "A", "to": "B", "settings": {"drop": 0.5}}
                                                change one one-way link
    {"v": 1, "type": "partition", "groups": [["A"], ["B", "C"]]}
                                                cut every link between groups
    {"v": 1, "type": "heal"}                    every link back to normal
    {"v": 1, "type": "status"}                  replicas and links

The supervisor and proxy are test tools. Replicas never talk to the
supervisor, and without run.py they connect to each other directly.
"""

import asyncio
import contextlib
import logging
import os
import signal
import sys

import websockets
from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response

from config import ALLOWED_ORIGINS, HOST, REPLICAS, SUPERVISOR_PORT
from protocol import SUPERVISOR_TYPES, BadMessage, decode, encode
from proxy import Network

ROOT = os.path.dirname(os.path.abspath(__file__))

# Read once at startup, so serving it never blocks on disk. Edit the file?
# Restart run.py.
with open(os.path.join(ROOT, "dashboard.html"), "rb") as f:
    DASHBOARD_PAGE = f.read()

log = logging.getLogger("supervisor")


class Supervisor:
    def __init__(self):
        self.procs = {}
        # One lock per replica, so start and kill for the same replica happen
        # one at a time. Without it, two "start" commands arriving together
        # could both see "not running" while the first process was still
        # launching, and start two copies.
        self.locks = {rid: asyncio.Lock() for rid in REPLICAS}
        # The fake network. CRDT_PROXY_SEED makes its random choices repeatable.
        seed = os.environ.get("CRDT_PROXY_SEED")
        self.network = Network(seed=int(seed) if seed else None)

    def running(self, rid):
        proc = self.procs.get(rid)
        return proc is not None and proc.returncode is None

    async def start(self, rid):
        async with self.locks[rid]:
            if self.running(rid):
                return
            # cwd=ROOT so this works no matter which folder run.py is started from.
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "replica.py", rid, cwd=ROOT,
                env={**os.environ, "CRDT_PROXY": "on"},  # send through the proxy
            )
            self.procs[rid] = proc
            log.info("started replica %s (pid %d)", rid, proc.pid)
            asyncio.ensure_future(self.report_exit(rid, proc))

    async def report_exit(self, rid, proc):
        code = await proc.wait()
        log.info("replica %s (pid %d) exited with code %d", rid, proc.pid, code)

    async def kill(self, rid):
        """SIGKILL: the replica gets no chance to clean up, like a real crash."""
        async with self.locks[rid]:
            if not self.running(rid):
                return
            self.procs[rid].kill()
            await self.procs[rid].wait()

    def status(self):
        """Which replicas have a live process.

        "running" means the process exists, not that it's ready: a replica
        that can't get its port exits a moment later. Ask the replica itself
        (get_state) to know it's answering.
        """
        return {
            "type": "status",
            "replicas": {
                rid: {
                    "running": self.running(rid),
                    "pid": self.procs[rid].pid if self.running(rid) else None,
                    "port": REPLICAS[rid],  # so the dashboard can connect to it
                }
                for rid in REPLICAS
            },
            "links": self.network.status(),
        }

    async def handle(self, ws):
        """Answer commands from the dashboard or tests."""
        try:
            async for raw in ws:
                try:
                    msg = decode(raw, commands=SUPERVISOR_TYPES)
                except BadMessage as e:
                    await ws.send(encode({"type": "error", "error": str(e)}))
                    continue
                if msg["type"] not in SUPERVISOR_TYPES:  # e.g. a replica's sync
                    await ws.send(encode({"type": "error", "error": "not a supervisor command"}))
                    continue
                if msg["type"] in ("start", "kill"):
                    rid = msg.get("replica")
                    if not isinstance(rid, str) or rid not in REPLICAS:
                        await ws.send(encode({"type": "error", "error": f"no replica {rid!r}"}))
                        continue
                    await (self.start(rid) if msg["type"] == "start" else self.kill(rid))
                try:
                    if msg["type"] == "link":
                        self.network.link(msg.get("from"), msg.get("to")).update(msg.get("settings"))
                    elif msg["type"] == "partition":
                        self.network.partition(msg.get("groups"))
                    elif msg["type"] == "heal":
                        self.network.heal()
                except (ValueError, TypeError) as e:
                    await ws.send(encode({"type": "error", "error": str(e)}))
                    continue
                await ws.send(encode(self.status()))
        except websockets.ConnectionClosed:
            pass

    async def run(self):
        # Ctrl-C (SIGINT) and `kill` (SIGTERM) both lead to a clean shutdown
        # that stops every replica, instead of leaving them running.
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        try:
            async with contextlib.AsyncExitStack() as links:
                # The network first, so replicas have something to connect to.
                await self.network.serve_all(links)
                for rid in REPLICAS:
                    await self.start(rid)
                await self.serve_commands(stop)
        finally:
            for proc in self.procs.values():
                if proc.returncode is None:
                    proc.terminate()
            for proc in self.procs.values():
                await proc.wait()

    async def serve_commands(self, stop):
        """Listen for commands (and serve the dashboard) until told to stop."""
        async with serve(
            self.handle, HOST, SUPERVISOR_PORT,
            process_request=serve_dashboard, origins=ALLOWED_ORIGINS,
        ):
            log.info("dashboard: http://%s:%d/  (Ctrl-C to stop)", HOST, SUPERVISOR_PORT)
            await stop.wait()


def serve_dashboard(connection, request):
    """Answer a plain browser request for "/" with the dashboard page.

    The same port handles websockets (the dashboard's commands) and this one
    page. Returning None lets a websocket handshake go ahead as normal.
    """
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    if request.path.split("?")[0] != "/":
        return connection.respond(404, "Not found\n")
    body = DASHBOARD_PAGE
    headers = Headers([("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
    return Response(200, "OK", headers, body)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S"
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    asyncio.run(supervise())


async def supervise():
    # Created here, inside the running event loop: on Python 3.9 an
    # asyncio.Lock made outside it belongs to the wrong loop and fails the
    # first time two commands actually wait on it.
    await Supervisor().run()


if __name__ == "__main__":
    main()
