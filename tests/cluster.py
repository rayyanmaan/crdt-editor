"""Start real replica processes for a test, talk to them, and clean up.

The tests in test_cluster_*.py don't simulate anything: every replica is a
separate OS process talking over real sockets, exactly as in the demo. They
run on ports 9001+ so they don't clash with a demo running on 8001+.
"""

import json
import os
import subprocess
import sys
import time

from websockets.exceptions import WebSocketException
from websockets.sync.client import connect

from config import HOST, REPLICA_IDS

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_BASE_PORT = 9001


class Cluster:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.procs = {}
        self.env = {**os.environ, "CRDT_BASE_PORT": str(TEST_BASE_PORT)}

    def port(self, rid):
        return TEST_BASE_PORT + REPLICA_IDS.index(rid)

    def start(self, rid):
        if rid in self.procs and self.procs[rid].poll() is None:
            raise RuntimeError(f"replica {rid} is already running")
        # Each run of a replica gets its own log file, kept for debugging.
        # The child process keeps its own handle, so ours can be closed.
        with open(os.path.join(self.log_dir, f"{rid}-{time.time_ns()}.log"), "w") as log:
            self.procs[rid] = subprocess.Popen(
                [sys.executable, "replica.py", rid],
                cwd=ROOT, env=self.env, stdout=log, stderr=subprocess.STDOUT,
            )

    def all_alive(self):
        """Fails loudly if a replica exited, e.g. because its port was taken."""
        for rid, proc in self.procs.items():
            if proc.poll() is not None:
                raise AssertionError(f"replica {rid} exited with code {proc.returncode}")
        return True

    def start_all(self):
        for rid in REPLICA_IDS:
            self.start(rid)

    def kill(self, rid):
        """SIGKILL, like `kill -9`: the process gets no chance to clean up."""
        self.procs[rid].kill()
        self.procs[rid].wait()

    def stop_all(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.kill()
        for proc in self.procs.values():
            proc.wait()

    def command(self, rid, msg):
        """Send one command to a replica and return its reply."""
        with connect(f"ws://{HOST}:{self.port(rid)}", open_timeout=1) as ws:
            ws.send(json.dumps({"v": 1, **msg}))
            return json.loads(ws.recv(timeout=2))

    def state(self, rid):
        return self.command(rid, {"type": "get_state"})

    def increment(self, rid, amount=1):
        return self.command(rid, {"type": "increment", "amount": amount})


def wait_until(check, timeout=10.0, interval=0.1):
    """Retry `check` until it returns something truthy, or fail after `timeout`.

    Waiting for a condition instead of sleeping a fixed time makes the tests
    both faster and more reliable: they move on as soon as the system is ready.
    A replica that isn't listening yet counts as "not yet".
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (OSError, TimeoutError, WebSocketException):
            last = "replica not reachable"
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last result: {last!r})")
