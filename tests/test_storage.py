"""Saving to disk (storage.py), and a replica loading it back.

The replica tests here build a Replica object without starting its network,
so they can check exactly what is on disk after each step.
"""

import json
import os

import pytest

import replica as replica_module
from replica import Replica
from rga import MAX_LAMPORT
from storage import CorruptLog, InUse, Storage, in_use


# ---- the files on their own --------------------------------------------------------


def test_ops_come_back_in_the_order_they_were_saved(tmp_path):
    s = Storage(str(tmp_path))
    assert s.load_ops() == ([], 0)
    s.append_ops([{"n": 1}, {"n": 2}])
    s.append_ops([{"n": 3}])
    assert Storage(str(tmp_path)).load_ops() == ([{"n": 1}, {"n": 2}, {"n": 3}], 0)


def test_a_half_written_last_line_is_cut_off(tmp_path):
    s = Storage(str(tmp_path))
    s.load_ops()
    s.append_ops([{"n": 1}, {"n": 2}])
    good_size = os.path.getsize(s.ops_path)
    with open(s.ops_path, "ab") as f:
        f.write(b'{"n": 3, "orig')  # the crash happened mid-write

    s2 = Storage(str(tmp_path))
    ops, cut = s2.load_ops()
    assert ops == [{"n": 1}, {"n": 2}]
    assert cut == len(b'{"n": 3, "orig')
    assert os.path.getsize(s.ops_path) == good_size

    # The next op starts on a clean line instead of being glued to the broken one.
    s2.append_ops([{"n": 4}])
    assert Storage(str(tmp_path)).load_ops() == ([{"n": 1}, {"n": 2}, {"n": 4}], 0)


def test_a_whole_line_without_its_newline_counts_as_unfinished(tmp_path):
    s = Storage(str(tmp_path))
    s.load_ops()
    s.append_ops([{"n": 1}])
    with open(s.ops_path, "ab") as f:
        f.write(b'{"n": 2}')  # valid JSON, but the write never finished
    assert Storage(str(tmp_path)).load_ops()[0] == [{"n": 1}]


def test_a_damaged_line_before_the_end_stops_loading_instead_of_losing_later_ops(tmp_path):
    # A crash can only break the last line. A broken line with confirmed
    # ops after it means the file was damaged: refuse, don't throw them away.
    s = Storage(str(tmp_path))
    with open(s.ops_path, "wb") as f:
        f.write(b'{"n": 1}\nnot json\n{"n": 3}\n')
    with pytest.raises(CorruptLog):
        s.load_ops()
    assert os.path.getsize(s.ops_path) == len(b'{"n": 1}\nnot json\n{"n": 3}\n')  # untouched


def test_only_one_process_can_use_a_folder(tmp_path):
    first = Storage(str(tmp_path))
    first.lock()
    assert in_use(str(tmp_path))
    with pytest.raises(InUse):
        Storage(str(tmp_path)).lock()
    first.close()
    assert not in_use(str(tmp_path))
    Storage(str(tmp_path)).lock()


def test_state_is_saved_whole_and_replaced_whole(tmp_path):
    s = Storage(str(tmp_path))
    assert s.load_state() is None
    s.save_state({"counter": {"A#1": 1}})
    s.save_state({"counter": {"A#1": 2}})
    assert Storage(str(tmp_path)).load_state() == {"counter": {"A#1": 2}}
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]  # no temporary file left over


def test_incarnations_only_go_up_even_if_the_clock_goes_back(tmp_path):
    s = Storage(str(tmp_path))
    assert s.next_incarnation(now_ms=5000) == 5000
    assert Storage(str(tmp_path)).next_incarnation(now_ms=7000) == 7000
    assert Storage(str(tmp_path)).next_incarnation(now_ms=100) == 7001  # clock went back
    assert Storage(str(tmp_path)).next_incarnation(now_ms=100) == 7002


# ---- a replica saving and loading ----------------------------------------------------


@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "unit-data"  # where conftest.py points every Replica


def restart(r):
    """What a crash and restart does: the process (and its lock) is gone,
    the folder stays, and a new process loads it."""
    r.storage.close()
    return Replica(r.id)


def saved_lines(data_dir, rid="A"):
    with open(data_dir / rid / "ops.log") as f:
        return [json.loads(line) for line in f]


def test_a_new_replica_starts_empty(data_dir):
    r = Replica("A")
    assert r.recovered_ops == 0
    assert r.text.text() == "" and r.counter.value() == 0 and r.set.elements() == []


def test_everything_accepted_comes_back_after_a_restart(data_dir):
    r = Replica("A")
    r.on_command({"type": "insert", "index": 0, "text": "cat"})
    r.on_command({"type": "delete", "index": 0, "count": 1})
    r.on_command({"type": "increment", "amount": 2})
    r.on_command({"type": "add", "element": "x"})
    r.on_command({"type": "add", "element": "y"})
    r.on_command({"type": "remove", "element": "y"})

    again = restart(r)
    assert again.incarnation > r.incarnation
    assert again.recovered_ops == 4
    assert again.text.text() == "at"
    assert again.text.doc.state() == r.text.doc.state()  # tombstone included
    assert again.counter.to_dict() == r.counter.to_dict()
    assert again.set.to_dict() == r.set.to_dict()


@pytest.mark.parametrize("command", [
    {"type": "insert", "index": 0, "text": "a"},
    {"type": "increment", "amount": 1},
    {"type": "add", "element": "x"},
])
def test_each_kind_of_edit_is_saved_on_its_own(data_dir, command):
    r = Replica("A")
    r.on_command(command)
    again = restart(r)
    assert again.text.text() == r.text.text()
    assert again.counter.to_dict() == r.counter.to_dict()
    assert again.set.to_dict() == r.set.to_dict()


def test_a_remove_is_saved_on_its_own(data_dir):
    r = Replica("A")
    r.on_command({"type": "add", "element": "x"})
    r.on_command({"type": "remove", "element": "x"})
    assert restart(r).set.elements() == []


def test_a_delete_is_saved_on_its_own(data_dir):
    r = Replica("A")
    r.on_command({"type": "insert", "index": 0, "text": "ab"})
    r.on_command({"type": "delete", "index": 0})
    assert restart(r).text.text() == "b"


def test_a_restarted_replica_types_under_a_new_name_after_its_old_text(data_dir):
    r = Replica("A")
    r.on_command({"type": "insert", "index": 0, "text": "ab"})
    again = restart(r)
    reply = again.on_command({"type": "insert", "index": "end", "text": "c"})
    assert again.text.text() == "abc"
    # New origin, and a Lamport number past the old ops, so no id can clash.
    op = reply["ops"][0]
    assert op["origin"] == again.slot != r.slot
    assert op["op"]["id"][0] > 2


def test_ops_are_on_disk_before_the_reply(data_dir, monkeypatch):
    r = Replica("A")
    sent = []
    monkeypatch.setattr(r, "broadcast_ops", lambda envelopes: sent.append(len(saved_lines(data_dir))))
    reply = r.on_command({"type": "insert", "index": 0, "text": "hi"})
    assert sent == [2]  # both ops were already saved when they were sent
    assert saved_lines(data_dir) == reply["ops"]


def test_ops_from_peers_are_saved_once(data_dir):
    b = Replica("B")
    ops = b.on_command({"type": "insert", "index": 0, "text": "hi"})["ops"]
    a = Replica("A")
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": ops + ops[:1]})
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": ops})  # a repeat
    assert saved_lines(data_dir) == ops

    # A kept B's ops even though B itself is gone.
    assert restart(a).text.text() == "hi"


def test_ops_saved_out_of_order_still_load(data_dir):
    b = Replica("B")
    ops = b.on_command({"type": "insert", "index": 0, "text": "hi"})["ops"]
    a = Replica("A")
    # "i" arrives first and waits for "h". Both are saved as they arrive.
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": [ops[1]]})
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": [ops[0]]})
    again = restart(a)
    assert again.text.text() == "hi"
    assert again.text.waited == 0  # replaying our own log isn't network reordering


def test_bad_ops_from_a_peer_are_not_saved(data_dir):
    a = Replica("A")
    a.on_peer_message({"type": "ops", "from": "B", "inc": 1, "ops": [{"origin": "B#1", "seq": 0}]})
    assert saved_lines(data_dir) == []


def test_counter_and_set_learned_from_a_peer_are_saved(data_dir):
    b = Replica("B")
    b.on_command({"type": "increment", "amount": 3})
    b.on_command({"type": "add", "element": "x"})
    a = Replica("A")
    a.on_peer_message({"type": "sync", "from": "B", "inc": b.incarnation,
                       "counter": b.counter.to_dict(), "set": b.set.to_dict()})
    again = restart(a)
    assert again.counter.value() == 3 and again.set.elements() == ["x"]


def test_replica_stops_if_it_cannot_save(data_dir, monkeypatch):
    r = Replica("A")

    def disk_full(_):
        raise OSError("No space left on device")

    def stop(code):
        raise SystemExit(code)

    monkeypatch.setattr(r.storage, "append_ops", disk_full)
    monkeypatch.setattr(replica_module.os, "_exit", stop)
    with pytest.raises(SystemExit):
        r.on_command({"type": "insert", "index": 0, "text": "x"})


def test_a_second_copy_of_a_running_replica_stops(data_dir):
    running = Replica("A")  # noqa: F841 (kept alive: it holds the lock)
    with pytest.raises(InUse):
        Replica("A")


def test_conflicting_copies_of_an_op_in_one_batch_save_the_one_that_is_kept(data_dir):
    b = Replica("B")
    real = b.on_command({"type": "insert", "index": 0, "text": "x"})["ops"][0]
    fake = {**real, "op": {**real["op"], "char": "y"}}
    a = Replica("A")
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": [real, fake]})
    assert a.text.text() == "x"
    assert saved_lines(data_dir) == [real]
    assert restart(a).text.text() == "x"


def test_running_out_of_character_ids_types_nothing(data_dir):
    a = Replica("A")
    a.text.doc.clock = MAX_LAMPORT - 1  # as if a peer had used a huge number
    reply = a.on_command({"type": "insert", "index": 0, "text": "xy"})
    assert reply["type"] == "error"
    assert a.text.text() == "" and a.text.version_vector() == {}
    assert saved_lines(data_dir) == []


def test_a_set_that_clashes_when_merged_is_not_merged_or_saved(data_dir):
    a = Replica("A")
    tag = a.on_command({"type": "add", "element": "x"})["tag"]
    # A peer claims the same tag belongs to a different element.
    a.on_peer_message({"type": "sync", "from": "B", "inc": 1, "counter": {},
                       "set": {"adds": {"y": [tag]}, "removed": []}})
    assert a.set.elements() == ["x"]
    assert restart(a).set.elements() == ["x"]  # what's on disk still loads


def test_an_op_still_waiting_survives_a_restart(data_dir):
    b = Replica("B")
    h, i = b.on_command({"type": "insert", "index": 0, "text": "hi"})["ops"]
    a = Replica("A")
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": [i]})
    assert a.text.buffer
    a = restart(a)
    assert a.text.buffer and a.text.text() == ""  # still waiting, not lost
    a.on_peer_message({"type": "ops", "from": "B", "inc": b.incarnation, "ops": [h]})
    assert a.text.text() == "hi" and not a.text.buffer


def test_set_tags_made_after_a_restart_are_new(data_dir):
    r = Replica("A")
    old = r.on_command({"type": "add", "element": "x"})["tag"]
    new = restart(r).on_command({"type": "add", "element": "x"})["tag"]
    assert new != old


def test_replica_stops_if_fsync_fails(data_dir, monkeypatch):
    r = Replica("A")

    def broken_fsync(fd):
        raise OSError("I/O error")

    def stop(code):
        raise SystemExit(code)

    monkeypatch.setattr(replica_module.os, "_exit", stop)
    monkeypatch.setattr("storage.os.fsync", broken_fsync)
    with pytest.raises(SystemExit):
        r.on_command({"type": "increment", "amount": 1})
    assert r.counter.value() == 0  # never counted, because it was never saved
    with pytest.raises(SystemExit):
        r.on_command({"type": "insert", "index": 0, "text": "x"})
