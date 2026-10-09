"""Only the dashboard's own page may talk to the servers from a browser.

A browser lets any website open a websocket to localhost. Without a check,
a page on some other site could send "kill" to the supervisor or edit the
replicas. Browsers always send an Origin header saying which site the page
came from, so the servers only accept the dashboard's origin, plus no
origin at all (Python clients and other replicas don't send one).

This only guards against web pages. A program running on the same machine
can send whatever Origin it likes.
"""

import json

import pytest
from websockets.exceptions import InvalidStatus
from websockets.sync.client import connect

from cluster import TEST_BASE_PORT
from config import HOST

SUPERVISOR = TEST_BASE_PORT - 1
DASHBOARD = f"http://localhost:{SUPERVISOR}"

FOREIGN = ["http://evil.example", "null", f"http://localhost:{SUPERVISOR + 100}"]


def get_state(port, origin):
    with connect(f"ws://{HOST}:{port}", origin=origin, open_timeout=2) as ws:
        ws.send(json.dumps({"v": 1, "type": "get_state"}))
        return json.loads(ws.recv(timeout=2))


@pytest.mark.parametrize("origin", FOREIGN)
def test_replica_rejects_other_websites(cluster, origin):
    with pytest.raises(InvalidStatus) as refused:
        get_state(cluster.port("A"), origin)
    assert refused.value.response.status_code == 403


@pytest.mark.parametrize("origin", [None, DASHBOARD, f"http://127.0.0.1:{SUPERVISOR}"])
def test_replica_accepts_the_dashboard_and_non_browser_clients(cluster, origin):
    assert get_state(cluster.port("A"), origin)["type"] == "state"
