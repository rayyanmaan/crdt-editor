"""Causal delivery for the shared text: the layer between the network and RGA.

The network can lose, repeat and reorder messages. RGA needs each op to
arrive after the character it refers to. This layer sits in between:

- Every op gets an identity, (origin, seq): "the 7th op made by B#1001".
  The origin includes the incarnation, so a restarted replica starts a new
  sequence instead of reusing old numbers.
- Every op received from anyone is kept in a log, forever. That's what lets
  a replica hand a peer the ops it missed, including ops a third replica made.
- An op that can't be applied yet waits in a buffer: either an earlier op
  from the same origin is missing, or the character it refers to isn't here.
  Each time something is applied, the buffer is checked again.
- The version vector says how far we've got with each origin, counting only
  ops with no gaps before them: {"A#1": 5} means A#1's ops 1 to 5 are all
  here. If op 7 arrives before op 6, the vector stays at 5, so the gap is
  still visible and a peer will fill it in.

"Causal" here means two things: a replica's ops are applied in the order it
made them, and an op waits for the character it refers to. That's what RGA
needs. It is not full causal order across all replicas (that would need each
op to carry a summary of everything its author had seen).

Repair: replicas swap version vectors every second, and each sends back what
the other is missing, in batches, taking ops from each origin in turn. A
lost op is sent again as long as some live replica still has it *and every
earlier op from the same origin*, and the two replicas can eventually reach
each other. If a replica crashes before anyone got its op 5, its ops 6 and up
can never be applied anywhere: they wait in buffers for good. Saving ops to
disk before sending them (Milestone 3) is what closes that gap.

An op on the wire (an "envelope"):
    {"origin": "B#1001", "seq": 7, "op": {...an RGA op...}}

`causal=False` turns the buffer off, for the experiment in the proposal:
ops are applied as they arrive. An insert whose character is missing is
guessed into the start of the document, and a delete whose character is
missing is dropped. A replica that gets ops in the wrong order can then end
up with different text from the others, and never recover.
"""

from rga import RGA, ROOT, encode_id, valid_replica_name, validate_op


class CausalText:
    def __init__(self, origin, causal=True):
        self.origin = origin
        self.causal = causal
        self.doc = RGA(origin)
        self.seq = 0  # how many ops this origin has made
        self.log = {}  # origin -> {seq: envelope}, every op we've received or made
        self.applied = {}  # origin -> highest seq applied with no gap before it
        self.buffer = {}  # (origin, seq) -> envelope, received but not applied yet
        # Origins we've stopped applying because one of their ops conflicted
        # with what we already have. Shown on the dashboard, so it's visible.
        self.rejected = {}  # origin -> reason
        # How many ops have ever had to wait in the buffer, so you can see
        # it doing its job (e.g. on the dashboard while links reorder).
        self.waited = 0

    # ---- local edits -----------------------------------------------------------

    def insert(self, index, char):
        return self._local(self.doc.insert_local(index, char))

    def delete(self, index):
        return self._local(self.doc.delete_local(index))

    def _local(self, op):
        # Our own op is already applied by RGA, and it can't depend on anything
        # we don't have, so it never needs the buffer.
        self.seq += 1
        envelope = {"origin": self.origin, "seq": self.seq, "op": op}
        self.log.setdefault(self.origin, {})[self.seq] = envelope
        self.applied[self.origin] = self.seq
        return envelope

    # ---- ops from other replicas ------------------------------------------------

    def receive(self, envelope):
        """Take an op from the network. Duplicates are ignored. Returns how
        many ops got applied as a result (this one and any it unblocked)."""
        validate_envelope(envelope)
        origin, seq = envelope["origin"], envelope["seq"]
        known = self.log.get(origin, {}).get(seq)
        if known is not None:
            if known != envelope:
                # The same (origin, seq) with different content: a bug or a
                # misbehaving peer. Replicas would disagree about which is real.
                raise ValueError(f"conflicting duplicate of op {origin}:{seq}")
            return 0
        self.log.setdefault(origin, {})[seq] = envelope

        if not self.causal:
            self._apply_without_waiting(envelope)
            return 1
        self.buffer[(origin, seq)] = envelope
        applied = self._drain()
        if (origin, seq) in self.buffer:
            self.waited += 1  # it couldn't go in straight away
        return applied

    def _ready(self, envelope):
        origin, seq = envelope["origin"], envelope["seq"]
        return seq == self.applied.get(origin, 0) + 1 and self.doc.can_apply(envelope["op"])

    def _drain(self):
        """Apply every buffered op that has become ready, until none is."""
        count = 0
        progress = True
        while progress:
            progress = False
            for key in sorted(self.buffer):
                envelope = self.buffer[key]
                if self._ready(envelope):
                    del self.buffer[key]
                    try:
                        self.doc.apply(envelope["op"])
                    except ValueError as e:
                        # The op conflicts with one we already have (same
                        # character id, different content). This origin
                        # stops here, and its later ops stay buffered: they
                        # can't be trusted either. Recorded so it's visible.
                        self.rejected[envelope["origin"]] = str(e)
                        raise
                    self.applied[envelope["origin"]] = envelope["seq"]
                    count += 1
                    progress = True
        return count

    def _apply_without_waiting(self, envelope):
        """What happens with no causal delivery: apply now, or guess."""
        op = envelope["op"]
        origin, seq = envelope["origin"], envelope["seq"]
        self.applied[origin] = max(self.applied.get(origin, 0), seq)
        if self.doc.can_apply(op):
            self.doc.apply(op)
        elif op["kind"] == "insert":
            # The character it goes after isn't here: guess, at the start.
            self.doc.apply({**op, "after": encode_id(ROOT)})
        # A delete of a character that isn't here yet is simply lost.

    # ---- repair -------------------------------------------------------------------

    def version_vector(self):
        """For each origin, how many of its ops we have with no gaps.

        Counts ops we've *received*, applied or still buffered: a buffered op
        is safe in the log, so there's no point asking for it again. What it's
        waiting for shows up as a gap in some other origin's count, and that's
        what a peer will send.
        """
        vector = {}
        for origin, ops in self.log.items():
            n = 0
            while n + 1 in ops:
                n += 1
            vector[origin] = n
        return vector

    def missing_for(self, their_vector, limit):
        """Ops we have that a peer with `their_vector` doesn't, at most `limit`.

        Includes ops from every origin, not just ours. Takes one op from each
        origin in turn (each origin's in seq order), so one origin with a big
        backlog, or a gap nobody can fill, can't starve the others.
        """
        queues = []
        for origin in sorted(self.log):
            have = their_vector.get(origin, 0)
            seqs = sorted(s for s in self.log[origin] if s > have)
            if seqs:
                queues.append([self.log[origin][s] for s in seqs])
        missing = []
        depth = 0
        while len(missing) < limit and any(depth < len(q) for q in queues):
            for queue in queues:
                if depth < len(queue):
                    missing.append(queue[depth])
                    if len(missing) >= limit:
                        break
            depth += 1
        return missing

    # ---- for the dashboard and tests ---------------------------------------------

    def text(self):
        return self.doc.text()

    def summary(self):
        return {
            "text": self.doc.text(),
            "have": self.version_vector(),  # received, applied or buffered
            "applied": dict(sorted(self.applied.items())),
            "rejected": dict(sorted(self.rejected.items())),
            "buffered": sorted(f"{o}:{s}" for o, s in self.buffer),
            "waited": self.waited,
            "logged": sum(len(ops) for ops in self.log.values()),
            "tombstones": sum(1 for n in self.doc.nodes[1:] if n.deleted),
        }


def validate_envelope(envelope):
    """Exactly {origin, seq, op}, with nothing extra. Together with the limits
    in validate_op, that bounds an envelope's size, so a batch of
    REPAIR_BATCH of them always fits in one message."""
    if not isinstance(envelope, dict):
        raise ValueError("op envelope must be an object")
    if set(envelope) != {"origin", "seq", "op"}:
        raise ValueError("op envelope must have exactly origin, seq and op")
    if not valid_replica_name(envelope["origin"]):
        raise ValueError("op envelope needs an origin")
    seq = envelope.get("seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise ValueError("op envelope needs a seq >= 1")
    validate_op(envelope.get("op"))
    op = envelope["op"]
    if op["kind"] == "insert" and op["id"][1] != envelope["origin"]:
        # An insert's id names the replica that made it; it must match.
        raise ValueError("insert id doesn't match the envelope's origin")
