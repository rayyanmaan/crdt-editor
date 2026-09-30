"""Start every replica as a separate process, with their logs in one terminal.

Usage:  python run.py        (Ctrl-C stops everything)

Each replica is a real OS process with its own memory, so the only way they
can share anything is over the network. You can also start them one by one
in separate terminals with `python replica.py A` and so on.
"""

import subprocess
import sys
import time

from config import REPLICAS


def main():
    procs = {}
    for rid in REPLICAS:
        procs[rid] = subprocess.Popen([sys.executable, "replica.py", rid])
        print(f"started replica {rid} (pid {procs[rid].pid})")
    print("kill one with `kill <pid>` to watch the others notice. Ctrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs.values():
            p.terminate()
        for p in procs.values():
            p.wait()


if __name__ == "__main__":
    main()
