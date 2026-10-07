"""run.py as a supervisor: start, crash and restart replicas on command.

These start the real run.py, which starts the real replicas.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from websockets.sync.client import connect

from cluster import ROOT, TEST_BASE_PORT, send_command, wait_until
from config import HOST, REPLICA_IDS

SUPERVISOR = TEST_BASE_PORT - 1


def supervisor(msg):
    return send_command(SUPERVISOR, msg)


def replica(rid, msg):
    return send_command(TEST_BASE_PORT + REPLICA_IDS.index(rid), msg)


def pid_alive(pid):
    try:
        os.kill(pid, 0)  # signal 0 checks the process exists without touching it
        return True
    except ProcessLookupError:
        return False


def all_running(proc):
    """The supervisor's status once every replica is running, else None.

    Also checks that the run.py we started is still alive: if it couldn't get
    its port, a leftover supervisor from another run might be answering.
    """
    if proc.poll() is not None:
        raise AssertionError(f"run.py exited with code {proc.returncode}")
    status = supervisor({"type": "status"})
    if all(r["running"] for r in status["replicas"].values()):
        return status
    return None


def child_pids(proc):
    out = subprocess.run(["pgrep", "-P", str(proc.pid)], capture_output=True, text=True).stdout
    return [int(p) for p in out.split()]


@pytest.fixture
def run_py(tmp_path):
    env = {**os.environ, "CRDT_BASE_PORT": str(TEST_BASE_PORT)}
    with open(tmp_path / "run.log", "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "run.py"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
        )
    try:
        wait_until(lambda: all_running(proc))
        yield proc
    finally:
        # Note every child now, including ones restarted during the test,
        # so they can be force-killed if run.py fails to stop them.
        children = child_pids(proc) if proc.poll() is None else []
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for pid in children:
            if pid_alive(pid):
                os.kill(pid, signal.SIGKILL)


def test_supervisor_starts_all_replicas(run_py):
    status = supervisor({"type": "status"})
    assert set(status["replicas"]) == set(REPLICA_IDS)
    assert all(r["running"] for r in status["replicas"].values())


def test_kill_crashes_a_replica_and_start_brings_it_back(run_py):
    wait_until(lambda: replica("A", {"type": "get_state"})["peers"]["B"]["status"] == "up")
    old_pid = supervisor({"type": "status"})["replicas"]["B"]["pid"]
    old_inc = replica("B", {"type": "get_state"})["incarnation"]

    status = supervisor({"type": "kill", "replica": "B"})
    assert status["replicas"]["B"]["running"] is False
    assert not pid_alive(old_pid)
    wait_until(lambda: replica("A", {"type": "get_state"})["peers"]["B"]["status"] == "suspected", timeout=6)

    status = supervisor({"type": "start", "replica": "B"})
    assert status["replicas"]["B"]["running"] is True
    new_inc = wait_until(lambda: replica("B", {"type": "get_state"})["incarnation"])
    assert new_inc != old_inc
    wait_until(lambda: replica("A", {"type": "get_state"})["peers"]["B"]["status"] == "up")


def test_bad_supervisor_commands_get_an_error(run_py):
    assert supervisor({"type": "kill", "replica": "Z"})["type"] == "error"
    assert supervisor({"type": "increment"})["type"] == "error"


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["kill", "ctrl-c"])
def test_stopping_run_py_stops_every_replica(run_py, sig):
    pids = [r["pid"] for r in supervisor({"type": "status"})["replicas"].values()]
    run_py.send_signal(sig)
    run_py.wait(timeout=10)
    wait_until(lambda: not any(pid_alive(p) for p in pids), timeout=5)


def children_running(run_py, rid):
    """PIDs of run.py's own child processes that are replica `rid`."""
    out = subprocess.run(
        ["pgrep", "-P", str(run_py.pid), "-f", f"replica.py {rid}"],
        capture_output=True, text=True,
    ).stdout
    return [int(p) for p in out.split()]


def test_simultaneous_starts_launch_only_one_replica(run_py):
    supervisor({"type": "kill", "replica": "B"})

    # Several dashboards press "start" at the same moment.
    with ThreadPoolExecutor(10) as pool:
        replies = list(pool.map(lambda _: supervisor({"type": "start", "replica": "B"}), range(10)))

    assert all(r["replicas"]["B"]["running"] for r in replies)
    wait_until(lambda: replica("B", {"type": "get_state"}))
    # Exactly one B, and it's the one the supervisor is tracking.
    tracked = supervisor({"type": "status"})["replicas"]["B"]["pid"]
    assert children_running(run_py, "B") == [tracked]


def test_bad_supervisor_input_keeps_the_connection_working(run_py):
    bad_messages = [
        {"type": "kill", "replica": []},  # not a string (and can't be a dict key)
        {"type": "kill", "replica": {"x": 1}},
        {"type": "sync", "from": "A", "inc": 1},  # a replica message, not a supervisor command
        {"type": "get_state"},  # a replica command
    ]
    with connect(f"ws://{HOST}:{SUPERVISOR}") as ws:
        for bad in bad_messages:
            ws.send(json.dumps({"v": 1, **bad}))
            assert json.loads(ws.recv(timeout=2))["type"] == "error", bad
            ws.send(json.dumps({"v": 1, "type": "status"}))
            assert json.loads(ws.recv(timeout=2))["type"] == "status", bad


def test_two_starts_at_once_inside_the_supervisor(monkeypatch):
    """The same race, made deterministic: call start() twice concurrently.

    start() checks "is it running?" and then waits for the process to launch.
    Without a lock, the second call can pass the check during that wait.
    """
    monkeypatch.setenv("CRDT_BASE_PORT", str(TEST_BASE_PORT))  # children use test ports
    from run import Supervisor

    def my_b_children():
        out = subprocess.run(
            ["pgrep", "-P", str(os.getpid()), "-f", "replica.py B"],
            capture_output=True, text=True,
        ).stdout
        return [int(p) for p in out.split()]

    async def scenario():
        sup = Supervisor()
        try:
            await asyncio.gather(sup.start("B"), sup.start("B"))
            return my_b_children(), sup.procs["B"].pid
        finally:
            await sup.kill("B")

    try:
        children, tracked = asyncio.run(scenario())
        assert children == [tracked]
    finally:
        for pid in my_b_children():  # anything the supervisor lost track of
            os.kill(pid, signal.SIGKILL)
