"""OR-Set (observed-remove set): a set you can add to and remove from,
replicated without coordination.

The problem: replica A removes "x" while replica B adds "x" at the same time.
After they sync, is "x" in the set or not? A plain set can't answer, because
"x was removed" and "x was added" are just two facts with no order.

The trick: every add creates a unique *tag*, and the set stores tags, not
just elements:

    adds    = {"x": {"A#1001:1", "B#2002:1"}}
    removed = {"A#1001:1"}                      # tombstones

"x" is in the set if it has at least one tag that hasn't been removed.
A remove only removes the tags the remover has *seen* (observed). So if B adds
"x" while A removes it, B's new tag isn't one A saw, it survives the merge,
and "x" stays in. Concurrent add and remove: the add wins. If A had seen B's
add first, its remove would have covered that tag too and "x" would be gone.

Merging two copies is a union of the tags and a union of the tombstones.
Union is commutative, associative and idempotent, just like max in the
G-Counter, so replicas that have seen the same updates end up equal.

Why keep tombstones at all? Without them, a stale copy of the state arriving
late would still contain "x" with its old tag, and merging it would bring "x"
back. The tombstone says "this tag is dead", and tags never come back to life.
The cost is that tombstones are never deleted, so the state only grows.
"""


class ORSet:
    def __init__(self, adds=None, removed=None):
        self.adds = {element: set(tags) for element, tags in (adds or {}).items()}
        self.removed = set(removed or ())

    def add(self, element, tag):
        """Add an element with a tag nobody has used before."""
        self.adds.setdefault(element, set()).add(tag)

    def remove(self, element):
        """Remove the element by tombstoning every tag of it we've seen.

        Returns the tags that were tombstoned (empty if it wasn't in the set).
        """
        observed = self.adds.get(element, set()) - self.removed
        self.removed |= observed
        return observed

    def __contains__(self, element):
        return bool(self.adds.get(element, set()) - self.removed)

    def elements(self):
        return sorted(e for e in self.adds if e in self)

    def merge(self, other):
        """Return a new set that has seen everything both have seen."""
        adds = {e: set(tags) for e, tags in self.adds.items()}
        for element, tags in other.adds.items():
            adds.setdefault(element, set()).update(tags)
        return ORSet(adds, self.removed | other.removed)

    def to_dict(self):
        # Sorted lists, so the same state always encodes the same way.
        return {
            "adds": {e: sorted(tags) for e, tags in sorted(self.adds.items())},
            "removed": sorted(self.removed),
        }

    @classmethod
    def from_dict(cls, data):
        """Build a set from a peer's message, rejecting anything malformed."""
        if not isinstance(data, dict):
            raise ValueError("set state must be an object")
        adds, removed = data.get("adds"), data.get("removed")
        if not isinstance(adds, dict) or not isinstance(removed, list):
            raise ValueError("set state needs 'adds' and 'removed'")
        for tags in adds.values():
            if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
                raise ValueError("tags must be a list of strings")
        if not all(isinstance(t, str) for t in removed):
            raise ValueError("tombstones must be strings")
        return cls(adds, removed)

    def __eq__(self, other):
        return isinstance(other, ORSet) and self.to_dict() == other.to_dict()

    def __repr__(self):
        return f"ORSet({self.to_dict()})"
