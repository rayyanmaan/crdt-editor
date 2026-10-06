"""run.py as a supervisor: start, crash and restart replicas on command.

These start the real run.py, which starts the real replicas.
"""

import os
import signal
import subprocess
import sys

import pytest

from cluster import ROOT, TEST_BASE_PORT, send_command, wait_until
from config import REPLICA_IDS

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


def all_running():
    """The supervisor's status once every replica is running, else None."""
    status = supervisor({"type": "status"})
    if all(r["running"] for r in status["replicas"].values()):
        return status
    return None


@pytest.fixture
def run_py(tmp_path):
    env = {**os.environ, "CRDT_BASE_PORT": str(TEST_BASE_PORT)}
    with open(tmp_path / "run.log", "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "run.py"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
        )
    pids = []
    try:
        status = wait_until(all_running)
        pids = [r["pid"] for r in status["replicas"].values()]
        yield proc
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=10)
        for pid in pids:  # belt and braces: nothing may be left running
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


def test_stopping_run_py_stops_every_replica(run_py):
    pids = [r["pid"] for r in supervisor({"type": "status"})["replicas"].values()]
    run_py.send_signal(signal.SIGTERM)
    run_py.wait(timeout=10)
    wait_until(lambda: not any(pid_alive(p) for p in pids), timeout=5)
