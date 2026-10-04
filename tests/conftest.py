import pytest

from cluster import Cluster, wait_until
from config import REPLICA_IDS


def all_links_up(cluster):
    """Every replica we started is alive and has heard from every peer recently.

    Checking our own processes are alive matters: if one couldn't bind its
    port, a leftover replica from another run might be answering instead.
    """
    cluster.all_alive()
    for rid in REPLICA_IDS:
        peers = cluster.state(rid)["peers"]
        if any(p["status"] != "up" for p in peers.values()):
            return False
    return True


@pytest.fixture
def cluster(tmp_path):
    """Three running replicas, all connected. Killed after the test."""
    c = Cluster(str(tmp_path))
    try:
        c.start_all()
        wait_until(lambda: all_links_up(c))
        yield c
    finally:
        c.stop_all()
