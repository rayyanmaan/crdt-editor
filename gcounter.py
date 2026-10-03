"""G-Counter: a counter that can only go up, replicated without coordination.

The trick is that no two replicas ever write to the same number. The counter
is a set of slots, one per writer, and each replica only increments its own:

    {"A#1001": 3, "B#2002": 2, "C#3003": 1}   -> value 6

To merge two copies, take the larger number in every slot. That works because
a slot only ever grows and only its owner writes it, so the larger number is
always the more up-to-date one.

Merging is commutative (merge(a, b) == merge(b, a)), associative (the grouping
doesn't matter) and idempotent (merging the same state twice changes nothing).
Together, these mean replicas that have seen the same updates end up equal,
no matter what order the updates arrived in or how often they were repeated.
That's strong eventual consistency for a state-based CRDT.

Slots are named "<replica>#<incarnation>", not just "<replica>". A replica
that restarts starts counting from 0 again. If its slot were just "C", its new
count (say 1) would be smaller than the old one its peers remember (say 5), so
max() would hide the new increments. A new incarnation gets a fresh slot.
"""


class GCounter:
    def __init__(self, slots=None):
        self.slots = dict(slots or {})

    def increment(self, slot, amount=1):
        if amount < 1:
            raise ValueError("a G-Counter can only go up")
        self.slots[slot] = self.slots.get(slot, 0) + amount

    def value(self):
        return sum(self.slots.values())

    def merge(self, other):
        """Return a new counter that has seen everything both have seen."""
        merged = dict(self.slots)
        for slot, count in other.slots.items():
            merged[slot] = max(merged.get(slot, 0), count)
        return GCounter(merged)

    def to_dict(self):
        return dict(self.slots)

    @classmethod
    def from_dict(cls, data):
        """Build a counter from a peer's message, rejecting anything malformed."""
        if not isinstance(data, dict):
            raise ValueError("counter state must be an object")
        for slot, count in data.items():
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValueError(f"bad count for slot {slot!r}")
        return cls(data)

    def __eq__(self, other):
        return isinstance(other, GCounter) and self.slots == other.slots

    def __repr__(self):
        return f"GCounter({self.slots})"
