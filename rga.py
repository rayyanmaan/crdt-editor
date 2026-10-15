"""RGA (Replicated Growable Array): shared text that several replicas can edit
at once and still end up identical. Roh et al. (2011).

This file is the data structure only. Sending ops between replicas, and
making sure each one arrives after what it depends on, comes later. The
rules below are written out with worked examples in NOTES.md.

The ideas, in short:
- Every character gets a permanent ID, (lamport, replica). Positions like
  "character 5" shift when someone else types; IDs never do.
- An insert says "put this character right after the character with ID x",
  not "at position 5".
- A deleted character stays in the list as an invisible tombstone, so a
  later insert that says "after it" still has somewhere to go.
- Inserts at the same spot are ordered by ID, biggest first. Every replica
  compares IDs the same way, so every replica picks the same order.

Ops are plain dicts so they can be sent as JSON later:
    {"kind": "insert", "id": [3, "A#1001"], "after": [2, "A#1001"], "char": "x"}
    {"kind": "insert", "id": [1, "A#1001"], "after": null, "char": "h"}   # at the start
    {"kind": "delete", "target": [3, "A#1001"]}
"""


class MissingDependency(Exception):
    """An op refers to a character this replica doesn't have yet.

    Applying it now would mean guessing where it goes. The caller should hold
    it back until the character arrives (causal delivery).
    """


class Node:
    __slots__ = ("id", "char", "deleted")

    def __init__(self, id, char):
        self.id = id
        self.char = char
        self.deleted = False


ROOT = (0, "")  # the invisible start of the document; real IDs have lamport >= 1


class RGA:
    def __init__(self, replica):
        self.replica = replica  # "<replica>#<incarnation>", unique per run
        self.clock = 0  # Lamport clock: bigger than every lamport we've seen
        root = Node(ROOT, "")
        self.nodes = [root]  # document order, tombstones included
        self.by_id = {ROOT: root}

    # ---- reading ---------------------------------------------------------

    def text(self):
        return "".join(n.char for n in self.nodes[1:] if not n.deleted)

    def visible(self):
        return [n for n in self.nodes[1:] if not n.deleted]

    def __len__(self):
        return len(self.visible())

    # ---- local edits (this replica's own typing) --------------------------

    def insert_local(self, index, char):
        """Insert `char` so it ends up at visible position `index`.

        Returns the op, to be sent to the other replicas.
        """
        visible = self.visible()
        if not 0 <= index <= len(visible):
            raise IndexError(f"insert position {index} out of range")
        after = visible[index - 1].id if index > 0 else ROOT
        self.clock += 1
        op = {"kind": "insert", "id": [self.clock, self.replica], "after": encode_id(after), "char": char}
        self.apply(op)
        return op

    def delete_local(self, index):
        """Delete the character at visible position `index`. Returns the op."""
        visible = self.visible()
        if not 0 <= index < len(visible):
            raise IndexError(f"delete position {index} out of range")
        op = {"kind": "delete", "target": encode_id(visible[index].id)}
        self.apply(op)
        return op

    # ---- applying any op, ours or another replica's -------------------------

    def can_apply(self, op):
        """True if everything the op refers to is already here."""
        validate_op(op)
        needed = op["after"] if op["kind"] == "insert" else op["target"]
        return decode_id(needed) in self.by_id

    def apply(self, op):
        """Apply an op. Applying the same op twice changes nothing."""
        validate_op(op)
        if op["kind"] == "insert":
            self._insert(decode_id(op["id"]), decode_id(op["after"]), op["char"])
        else:
            self._delete(decode_id(op["target"]))

    def _insert(self, id, after, char):
        if id in self.by_id:
            return  # seen it already
        if after not in self.by_id:
            raise MissingDependency(f"insert {id} goes after {after}, which isn't here yet")

        # Keep the clock ahead of every ID we've seen, so our next insert gets
        # a bigger lamport number than anything we know about.
        self.clock = max(self.clock, id[0])

        # Start just after `after`, skip every node with a bigger ID, and stop
        # at the first one with a smaller ID. The skipped nodes are inserts at
        # this same spot that win the tie-break, plus everything typed after
        # them (which has even bigger IDs). See NOTES.md for why this gives
        # the same order whatever order the ops arrive in.
        i = self.nodes.index(self.by_id[after]) + 1
        while i < len(self.nodes) and self.nodes[i].id > id:
            i += 1

        node = Node(id, char)
        self.nodes.insert(i, node)
        self.by_id[id] = node

    def _delete(self, target):
        if target not in self.by_id or target == ROOT:
            raise MissingDependency(f"delete of {target}, which isn't here yet")
        self.by_id[target].deleted = True  # a tombstone: it keeps its place

    # ---- for tests and the dashboard -----------------------------------------

    def state(self):
        """Every node in order, tombstones included. Equal on two replicas
        means they hold exactly the same document, not just the same text."""
        return [(list(n.id), n.char, n.deleted) for n in self.nodes[1:]]


# ---- IDs on the wire --------------------------------------------------------

def encode_id(id):
    return None if id == ROOT else [id[0], id[1]]


def decode_id(value):
    return ROOT if value is None else (value[0], value[1])


def is_id(value):
    return (
        isinstance(value, list) and len(value) == 2
        and isinstance(value[0], int) and not isinstance(value[0], bool) and value[0] >= 1
        and isinstance(value[1], str) and value[1] != ""
    )


def validate_op(op):
    """Reject anything that isn't a well-formed op, before it touches the list."""
    if not isinstance(op, dict):
        raise ValueError("op must be an object")
    if op.get("kind") == "insert":
        if not is_id(op.get("id")):
            raise ValueError("insert needs an id [lamport >= 1, replica]")
        if op.get("after") is not None and not is_id(op.get("after")):
            raise ValueError("insert's 'after' must be an id or null")
        if not isinstance(op.get("char"), str) or len(op["char"]) != 1:
            raise ValueError("insert's 'char' must be a single character")
        if op.get("after") is not None and op["after"][0] >= op["id"][0]:
            # Rule 2: an insert's lamport is bigger than the one it follows,
            # because its author had seen that character. The ordering
            # argument in NOTES.md depends on this, so reject ops that break it.
            raise ValueError("an insert's lamport must be bigger than the one it goes after")
    elif op.get("kind") == "delete":
        if not is_id(op.get("target")):
            raise ValueError("delete needs a target id")
    else:
        raise ValueError(f"unknown op kind {op.get('kind')!r}")
