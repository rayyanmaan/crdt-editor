"""Start every replica as a separate process, with their logs in one terminal.

Usage:  python run.py        (Ctrl-C stops everything)

Each replica is a real OS process with its own memory, so the only way they
can share anything is over the network. You can also start them one by one
in separate terminals with `python replica.py A` and so on.
"""

import os
import signal
import subprocess
import sys
import time
from functools import partial

from config import REPLICAS

ROOT = os.path.dirname(os.path.abspath(__file__))

# Print immediately even when output goes to a file or pipe.
print = partial(print, flush=True)


def main():
    # `kill <pid of run.py>` sends SIGTERM, which would end this script without
    # running the cleanup below and leave the replicas running. Turning it into
    # a normal exit makes the `finally` block stop them.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    procs = {}
    try:
        for rid in REPLICAS:
            # cwd=ROOT so this works no matter which folder it's started from.
            procs[rid] = subprocess.Popen([sys.executable, "replica.py", rid], cwd=ROOT)
            print(f"started replica {rid} (pid {procs[rid].pid})")
        print("kill one with `kill <pid>` to watch the others notice. Ctrl-C to stop.")

        reported = set()
        while True:
            time.sleep(1)
            for rid, p in procs.items():
                if p.poll() is not None and rid not in reported:
                    print(f"replica {rid} exited (code {p.returncode})")
                    reported.add(rid)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs.values():
            p.terminate()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
