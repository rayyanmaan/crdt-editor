# crdt-editor

A shared text editor where three replicas can edit the same document at the same time, even while the network between them is broken, and still end up with the same text. There is no server that decides the final version. Each replica keeps its own copy, applies edits right away, and sends them to the others.

I'm building this for IL18105 (Distributed Systems) at Minerva. The text is stored in a CRDT called RGA (Roh et al., 2011).

## Running it

You need Python 3.9 or newer, on macOS or Linux.

```
git clone https://github.com/rayyanmaan/crdt-editor.git
cd crdt-editor
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```

Then open http://localhost:8000/ in a browser. Ctrl-C in the terminal stops everything.

`run.py` starts three replicas (A, B and C), each as its own process, plus a fake network that sits between them. It uses ports 8000 to 8016 on localhost.

## What you can do on the dashboard

Each panel is one replica, showing its own view of things:

- **Text.** Type into any replica with "Add to end", or delete the last character with ⌫. The line under the text shows how many edits it has applied from each replica, how many are waiting in its buffer, and how many deleted characters (tombstones) it is keeping.
- **Counter and set.** A shared counter (+1) and a shared set of words (Add, and × to remove). These were the first things I built, before the text.
- **Peers.** What this replica thinks of the other two: "up", or "suspected" if it hasn't heard from them for 3 seconds.
- **Kill / Restart.** Kill crashes the replica with SIGKILL. Restart brings it back empty, and it catches up from the others.

The network section at the top controls the links between replicas. Every message goes through it.

- **Cut off A / B / C** splits one replica away from the other two. **Heal network** puts everything back.
- Each one-way link (A → B, B → A, and so on) can also be cut on its own, or set to drop, duplicate, delay or reorder messages. The counts show what happened to the messages on that link.

## Things to try

1. Type "The cat sat" in A. It shows up in B and C. Add "apple" to the set in B. It shows up in all three.
2. Click "Cut off A". After a few seconds, A and the other two mark each other as "suspected". Nobody gets an error; the links just go quiet.
3. While A is cut off, add " on the mat" in A. In B, delete "sat" (⌫ three times) and add "slept". Then in A, remove "apple" (×) and click +1 twice. In B, add "apple" again and click +1 once.
4. Click "Heal network". All three end up with "The cat slept on the mat" and 3 deleted characters kept as tombstones. The counter is 3 everywhere, since each replica only adds to its own slot. "apple" is back, because when an add and a remove happen at the same time, the add wins.
5. Kill C, type something in A, then restart C. It comes back with a new incarnation, empty, and catches up.
6. To see the causal buffer work: set A → C to "Delay: 5 s" and tick "Cut" on C → B. Type "h" in A's box and "i" in B's box. Click Add in A, then straight away Add in B. B's "i" goes after A's "h", but C gets the "i" first (A's "h" is delayed), so C shows "1 waiting in buffer" for a few seconds until the "h" turns up. Then both appear. Click "Heal network" after. (Cutting C → B stops B from filling C in through repair, which would otherwise get there first.)

## How it works

- **Replicas** are separate processes. Each one sends to each peer over its own one-way websocket link, so the network can break one direction without the other.
- **Counter and set** (G-Counter and OR-Set) send their whole state to every peer every second, and merging takes the max (counter) or the union (set). A lost message doesn't matter because the next one has everything.
- **Text** (RGA) works with operations instead. Every character gets a permanent ID (a Lamport timestamp plus the replica's name), and an insert says "put this right after character X". When two people insert at the same spot, the bigger ID goes first, and every replica compares IDs the same way, so they all pick the same order. Deleted characters stay as tombstones so later edits that point at them still work.
- **Getting edits everywhere.** Each edit is sent once. Every replica keeps a log of every edit from everyone. Each second, replicas swap version vectors (how many edits they have from each replica, counting only edits with no gaps) and send each other whatever is missing. An edit that arrives before something it depends on waits in a buffer.
- **The fake network** (`proxy.py`) runs inside `run.py`. Each link gets whole messages and decides whether to deliver, drop, duplicate or delay them.

More detail is in the comments at the top of each file.

## Files

| File | What it is |
|---|---|
| `run.py` | Starts the replicas and the fake network, serves the dashboard, handles kill/restart and network commands |
| `replica.py` | One replica: links to peers, syncing, failure detector, commands |
| `rga.py` | The RGA text CRDT on its own |
| `causal.py` | Edit log, version vectors, causal buffer and repair for the text |
| `gcounter.py`, `orset.py` | The counter and set CRDTs |
| `proxy.py` | The fake network |
| `protocol.py` | Message format and validation |
| `config.py` | Ports, timings and limits |
| `dashboard.html` | The dashboard page |
| `tests/` | The tests |

## Tests

```
python -m pytest
```

There are about 1,000 tests and they take around 2.5 minutes. Most are unit tests of the CRDTs. The rest start real replica processes (and the real network) and break things on purpose: partitions, crashes, lost, duplicated and reordered messages. Those check that every edit ends up on every replica, not just that the texts match.

A few tests drive the dashboard in headless Chrome. They need Google Chrome and Node 22 or newer, and are skipped if either is missing.

One test turns the causal buffer off (`CRDT_CAUSAL_BUFFER=off`) to show what goes wrong without it: a replica that gets edits in the wrong order puts them in the wrong place and never recovers.

## What it doesn't do yet

- Nothing is saved to disk. A restarted replica starts empty and gets everything back from the others, but an edit that never reached another replica before a crash is lost. If a replica crashes after its later edits got out but before an earlier one did, those later edits can never be applied anywhere.
- There are exactly three replicas, set in `config.py`.
- RGA keeps two people's words together when they type forwards, but can interleave them if someone types backwards (inserting each character before the last one).
- A replica's incarnation number comes from the clock, so a clock that jumps backwards could reuse one.
- Everything is kept forever (the edit log and tombstones), and some parts slow down a lot with very long documents.
- It only runs on macOS and Linux.

## What's next

- Saving edits to disk before sending them, so a crash loses nothing, and randomized tests with Hypothesis.
- FugueMax, which doesn't interleave, side by side with RGA, plus a last-writer-wins version that visibly loses edits.
- Replaying a real editing trace from the Eg-walker paper on 5 to 10 replicas and measuring convergence time, bytes sent and how much memory tombstones take.
