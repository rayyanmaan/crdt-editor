"""Saving a replica's state to disk, so a crash doesn't lose accepted edits.

Before this, a restarted replica came back empty and got everything back from
its peers. That works only if every edit had already reached a peer. An edit
made while cut off, or a second before a crash, existed only in the crashed
process's memory and was gone for good.

The rule now is: an edit is written to disk before it counts as accepted. A
replica writes it, waits for the disk to confirm (fsync), and only then
applies it for good, sends it to peers and replies "done" to whoever made it.
So anything a user was told was accepted survives a crash, even if nobody
else ever got it. This is a write-ahead log.

Three files per replica, in data/<replica>/:

- ops.log: every text op this replica has made or received, one JSON
  envelope per line, only ever appended to. On restart the replica feeds
  them all back through the causal layer, which rebuilds the document.
- state.json: the counter and set. These are small and merged as whole
  states anyway, so we save the whole thing each time it changes, by writing
  a new file and renaming it over the old one (a rename is all-or-nothing,
  so a crash leaves either the old file or the new one, never half of each).
- incarnation: the number of the last run, so the next run gets a bigger one
  even if the wall clock jumped backwards.

A crash in the middle of appending can leave half a line at the end of
ops.log. That op was never confirmed, so nobody was told it was accepted:
we cut it off and carry on. A broken line *before* the end is different: a
crash can't cause that (we only ever add to the end), so it means the file
was damaged, and later lines may be confirmed edits. We refuse to start
rather than throw them away.

Only one process may use a folder at a time. A lock file makes a second
copy of the same replica (or `run.py --fresh` while it's running) stop with
an error instead of both writing to, or deleting, the same files.

What fsync protects against: the process being killed (kill -9) is covered
even without it, since the operating system already has the data. fsync is
for the machine itself crashing or losing power. On macOS, plain fsync can
still leave data in the drive's own cache; F_FULLFSYNC would close that gap
but is much slower, and a crashed process is the failure this project tests.
"""

import fcntl
import json
import os


class CorruptLog(Exception):
    """ops.log has a broken line before its end, so it was damaged."""


class InUse(Exception):
    """Another process is already using this replica's folder."""


class Storage:
    def __init__(self, directory):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        # A new folder is an entry in its parent folder: make that durable too.
        fsync_folder(os.path.dirname(os.path.abspath(directory)))
        self.ops_path = os.path.join(directory, "ops.log")
        self.state_path = os.path.join(directory, "state.json")
        self.incarnation_path = os.path.join(directory, "incarnation")
        self.ops_file = None  # opened for appending once we've read the log
        self.lock_file = None

    def lock(self):
        """Claim the folder for this process, until it exits or calls close().

        The operating system drops the lock when the process dies, kill -9
        included, so a crash never leaves the folder locked.
        """
        self.lock_file = open(os.path.join(self.dir, "lock"), "w")
        try:
            fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock_file.close()
            self.lock_file = None
            raise InUse(f"{self.dir} is in use by another process")

    def close(self):
        """Close the files and let go of the lock (tests use this to stand
        in for a process exiting)."""
        for f in (self.ops_file, self.lock_file):
            if f is not None:
                f.close()
        self.ops_file = self.lock_file = None

    # ---- incarnation ---------------------------------------------------------

    def next_incarnation(self, now_ms):
        """A number bigger than any earlier run's, saved before it's used.

        Usually it's the start time in milliseconds (so it still goes up if
        this folder is deleted), but never less than the last one plus one,
        so a clock that jumped backwards can't make us reuse an old number.
        """
        last = 0
        if os.path.exists(self.incarnation_path):
            with open(self.incarnation_path) as f:
                last = int(f.read())
        incarnation = max(last + 1, now_ms)
        write_atomically(self.incarnation_path, str(incarnation))
        return incarnation

    # ---- counter and set -----------------------------------------------------

    def load_state(self):
        """The saved counter and set, or None if nothing was saved yet."""
        if not os.path.exists(self.state_path):
            return None
        with open(self.state_path) as f:
            return json.load(f)

    def save_state(self, state):
        write_atomically(self.state_path, json.dumps(state, sort_keys=True))

    # ---- text ops ------------------------------------------------------------

    def load_ops(self):
        """Every op saved so far, in the order they were saved, and how many
        bytes of unfinished last line were cut off the end (0 normally).

        The part after the last newline is a write that never finished, and
        is cut off so new ops don't get glued onto it. Any complete line that
        isn't valid JSON means the file was damaged: CorruptLog.
        """
        data = b""
        if os.path.exists(self.ops_path):
            with open(self.ops_path, "rb") as f:
                data = f.read()
        envelopes = []
        lines = data.split(b"\n")[:-1]  # the part after the last \n is unfinished
        for number, line in enumerate(lines, 1):
            try:
                envelopes.append(json.loads(line))
            except ValueError:
                raise CorruptLog(f"{self.ops_path} line {number} is damaged") from None
        good_bytes = sum(len(line) + 1 for line in lines)
        cut = len(data) - good_bytes
        if cut:
            with open(self.ops_path, "r+b") as f:
                f.truncate(good_bytes)
                f.flush()
                os.fsync(f.fileno())
        self.ops_file = open(self.ops_path, "ab")
        fsync_folder(self.dir)  # in case that just created the file
        return envelopes, cut

    def append_ops(self, envelopes):
        """Add ops to the log and wait until the disk has them."""
        if not envelopes:
            return
        lines = b"".join(json.dumps(e, sort_keys=True).encode() + b"\n" for e in envelopes)
        self.ops_file.write(lines)
        self.ops_file.flush()  # from Python's buffer to the operating system
        os.fsync(self.ops_file.fileno())  # from the operating system to the disk


def write_atomically(path, text):
    """Replace a file's contents so a crash leaves the old or the new version."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    # The rename itself is recorded in the folder, so make that durable too.
    fsync_folder(os.path.dirname(path))


def fsync_folder(path):
    """Make a folder's list of files durable (new files, renames)."""
    folder = os.open(path or ".", os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


def in_use(directory):
    """True if some process holds the lock on this replica folder."""
    if not os.path.isdir(directory):
        return False
    with open(os.path.join(directory, "lock"), "w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(f, fcntl.LOCK_UN)
        return False
