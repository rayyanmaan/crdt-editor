import pytest

from cluster import Cluster, wait_until
from config import REPLICA_IDS


def all_links_up(cluster):
    """Every replica has heard from every peer recently."""
    for rid in REPLICA_IDS:
        peers = cluster.state(rid)["peers"]
        if any(p["status"] != "up" for p in peers.values()):
            return False
    return True


@pytest.fixture
def cluster(tmp_path):
    """Three running replicas, all connected. Killed after the test."""
    c = Cluster(str(tmp_path))
    c.start_all()
    try:
        wait_until(lambda: all_links_up(c))
        yield c
    finally:
        c.stop_all()
