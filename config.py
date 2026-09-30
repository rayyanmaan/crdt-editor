"""Who is in the cluster and where each replica listens.

Every replica reads this same table, so they all agree on the membership.
Keeping membership fixed is a simplification: adding or removing replicas
while the system runs is a hard problem of its own, and not what this project
is about.
"""

HOST = "localhost"

# replica id -> port its websocket server listens on
REPLICAS = {
    "A": 8001,
    "B": 8002,
    "C": 8003,
}

# How often a replica says "I'm still here" to each peer, in seconds.
HEARTBEAT_INTERVAL = 1.0

# How long to wait before retrying a peer we can't reach, in seconds.
RECONNECT_DELAY = 1.0
