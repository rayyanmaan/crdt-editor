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
SUPERVISOR_PORT = BASE_PORT - 1

# Most messages that can wait on one outgoing link. When it's full, new
# messages are dropped. Every sync carries the full state, so that loses
# nothing that a later sync won't bring.
OUTBOX_LIMIT = 100
