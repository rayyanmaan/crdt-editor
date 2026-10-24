"""Who is in the cluster and where each replica listens.

Every replica reads this same table, so they all agree on the membership.
Keeping membership fixed is a simplification: adding or removing replicas
while the system runs is a hard problem of its own, and not what this project
is about.
"""

import os

HOST = "localhost"

REPLICA_IDS = ["A", "B", "C"]

# Replica A listens on BASE_PORT, B on BASE_PORT + 1, and so on. The tests set
# CRDT_BASE_PORT to a different number so they don't clash with a running demo.
BASE_PORT = int(os.environ.get("CRDT_BASE_PORT", "8001"))
REPLICAS = {rid: BASE_PORT + i for i, rid in enumerate(REPLICA_IDS)}

# How often a replica sends its full state to each peer, in seconds. This
# message doubles as the heartbeat.
SYNC_INTERVAL = 1.0

# A peer we haven't heard from for this long is suspected to be down, in seconds.
SUSPECT_AFTER = 3.0

# How long to wait before retrying a peer we can't reach, in seconds.
RECONNECT_DELAY = 1.0

# Largest message a replica accepts. This is the websockets library's default
# (1 MiB), written out so the limit is a deliberate choice. Full-state syncs
# grow with every set tombstone, so a very long-running demo could reach it.
MAX_MESSAGE_BYTES = 2**20

# Port of the supervisor (run.py), which starts and kills replicas on request.
# It also serves the dashboard page.
SUPERVISOR_PORT = BASE_PORT - 1

# The fault-injection proxy (proxy.py) has one port per one-way link, keyed
# by (from, to): A sends to B through LINK_PORTS[("A", "B")]. run.py starts
# the proxy and sets CRDT_PROXY=on for its replicas. Replicas started on
# their own (python replica.py A, or the tests' Cluster) connect directly.
LINK_PORTS = {
    (src, dst): BASE_PORT + 10 + i
    for i, (src, dst) in enumerate(
        (s, d) for s in REPLICA_IDS for d in REPLICA_IDS if s != d
    )
}
USE_PROXY = os.environ.get("CRDT_PROXY", "off") == "on"

# Which web pages may open a websocket to a replica or the supervisor.
# Browsers let any website connect to localhost, but they always say which
# site the page came from (the Origin header), so we accept only the dashboard.
# None means "no Origin header": Python clients and other replicas.
# This stops other websites, not other programs on this machine.
ALLOWED_ORIGINS = [None] + [
    f"http://{host}:{SUPERVISOR_PORT}" for host in ("localhost", "127.0.0.1", "[::1]")
]

# Most text ops in one message, when sending new edits or repairing a peer.
REPAIR_BATCH = 200

# Most characters one "insert" command may type at once.
MAX_INSERT_CHARS = 1000

# The causal buffer holds back text ops that arrive before what they depend
# on. CRDT_CAUSAL_BUFFER=off turns it off, for the experiment that shows
# replicas diverging without it. Never turn it off otherwise.
CAUSAL_BUFFER = os.environ.get("CRDT_CAUSAL_BUFFER", "on") != "off"

# Most messages that can wait on one outgoing link. When it's full, new
# messages are dropped. Nothing is lost for good: every sync carries the full
# counter and set, and a dropped text op shows up as a gap in our version
# vector at the peer, which then gets it re-sent by repair.
OUTBOX_LIMIT = 100
