"""The dashboard in a real (headless) browser, against the real run.py.

These click the page's actual buttons and read what it shows, so they test
the JavaScript, not just that the page is served. They need Google Chrome
(or Chromium) and Node 22+, and are skipped if either is missing. Set CHROME
to the browser's path if it's somewhere unusual.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import time
import time

import pytest

from cluster import ROOT, TEST_BASE_PORT, send_command, wait_until
from test_supervisor import SUPERVISOR, run_py, supervisor  # noqa: F401 (run_py is a fixture)

DRIVER = os.path.join(ROOT, "tests", "dashboard_driver.mjs")


def find_chrome():
    candidates = [
        os.environ.get("CHROME"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
    ]
    return next((c for c in candidates if c and os.path.exists(c)), None)


def node_is_new_enough():
    node = shutil.which("node")
    if not node:
        return False
    version = subprocess.run([node, "--version"], capture_output=True, text=True).stdout
    return int(version.strip().lstrip("v").split(".")[0]) >= 22  # built-in WebSocket


CHROME = find_chrome()
pytestmark = pytest.mark.skipif(
    not CHROME or not node_is_new_enough(), reason="needs Chrome and Node 22+"
)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Page:
    """The dashboard open in headless Chrome."""

    def __init__(self, url):
        self.proc = subprocess.Popen(
            ["node", DRIVER, CHROME, str(free_port()), url],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        assert self._read() == {"ready": True}

    def _read(self):
        return json.loads(self.proc.stdout.readline())

    def eval(self, expr):
        self.proc.stdin.write(json.dumps({"eval": expr}) + "\n")
        self.proc.stdin.flush()
        reply = self._read()
        if "error" in reply:
            raise RuntimeError(reply["error"])
        return reply["value"]

    def problems(self):
        self.proc.stdin.write(json.dumps({"problems": True}) + "\n")
        self.proc.stdin.flush()
        return self._read()["value"]

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=10)

    # ---- reading and clicking, the way a person would ----

    def panels(self):
        """What each panel shows, keyed by replica id."""
        return {
            p["name"][-1]: p
            for p in self.eval("""[...document.querySelectorAll('.card')].map(c => ({
                name: c.querySelector('.name').textContent,
                status: c.querySelector('.status').textContent,
                counter: c.querySelector('.counter').textContent,
                set: [...c.querySelectorAll('.set .chip')].map(ch => ch.firstChild.textContent),
                text: c.querySelector('.doc').textContent,
                peers: Object.fromEntries([...c.querySelectorAll('.peers tr')].map(
                    tr => [tr.children[0].textContent, tr.children[1].textContent])),
                kill: !c.querySelector('.kill').disabled,
                restart: !c.querySelector('.start').disabled,
            }))""")
        }

    def card(self, rid):
        return f"[...document.querySelectorAll('.card')].find(c => c.querySelector('.name').textContent.endsWith('{rid}'))"

    def click(self, rid, selector):
        self.eval(f"{self.card(rid)}.querySelector('{selector}').click()")

    def add(self, rid, element):
        self.eval(f"""(() => {{
            const c = {self.card(rid)};
            c.querySelector('.element').value = {json.dumps(element)};
            c.querySelector('.add-form').requestSubmit();
        }})()""")

    def type(self, rid, text):
        self.eval(f"""(() => {{
            const c = {self.card(rid)};
            c.querySelector('.typed').value = {json.dumps(text)};
            c.querySelector('.type-form').requestSubmit();
        }})()""")

    def remove(self, rid, element):
        self.eval(f"""[...{self.card(rid)}.querySelectorAll('.chip')]
            .find(ch => ch.firstChild.textContent === {json.dumps(element)})
            .querySelector('button').click()""")


def all_up(panels):
    """Every replica answers, and every replica hears every peer.

    One read of the page, checked as a whole. (Reading it twice once let an
    empty page pass: all() of nothing is True.) Waiting for the peers too
    matters: right after startup a link may not be connected yet, and a
    message sent then only arrives later through repair.
    """
    return len(panels) == 3 and all(
        x["status"] == "up" and len(x["peers"]) == 2 and all(v == "up" for v in x["peers"].values())
        for x in panels.values()
    )


@pytest.fixture
def page(run_py):  # noqa: F811
    p = Page(f"http://localhost:{SUPERVISOR}/")
    try:
        wait_until(lambda: all_up(p.panels()))
        yield p
    finally:
        p.close()


def everywhere(page, check):
    return all(check(p) for p in page.panels().values())


def test_edits_made_in_the_page_reach_every_replica(page):
    page.click("A", ".inc")
    page.click("A", ".inc")
    page.click("C", ".inc")
    page.add("B", "apple")
    page.add("C", "banana")
    wait_until(lambda: everywhere(page, lambda p: p["counter"] == "3" and p["set"] == ["apple", "banana"]))

    page.remove("A", "apple")
    wait_until(lambda: everywhere(page, lambda p: p["set"] == ["banana"]))
    assert page.problems() == []


def test_killed_replica_shows_down_with_an_empty_panel_then_recovers(page):
    page.add("A", "kept")
    wait_until(lambda: everywhere(page, lambda p: p["set"] == ["kept"]))

    page.click("B", ".kill")
    wait_until(lambda: page.panels()["B"]["status"] == "down")
    b = page.panels()["B"]
    assert b["counter"] == "–" and b["set"] == [] and b["peers"] == {}  # no stale state
    assert b["restart"] and not b["kill"]
    wait_until(lambda: page.panels()["A"]["peers"]["B"] == "suspected", timeout=6)

    page.click("A", ".inc")
    page.click("B", ".start")
    wait_until(lambda: page.panels()["B"]["status"] == "up")
    wait_until(lambda: page.panels()["B"]["counter"] == "1" and page.panels()["B"]["set"] == ["kept"])
    assert page.problems() == []


def test_a_frozen_replica_stops_showing_as_up(page):
    # SIGSTOP freezes the process without closing its sockets, so the page's
    # connection stays open but nothing answers. The panel must not keep
    # showing the last state it got as if the replica were fine.
    pid = supervisor({"type": "status"})["replicas"]["C"]["pid"]
    os.kill(pid, signal.SIGSTOP)
    try:
        wait_until(lambda: page.panels()["C"]["status"] != "up", timeout=5)
        c = page.panels()["C"]
        assert c["counter"] == "–" and c["peers"] == {}
        wait_until(lambda: page.panels()["A"]["peers"].get("C") == "suspected", timeout=6)
        assert page.panels()["A"]["status"] == "up"  # A itself is fine
    finally:
        os.kill(pid, signal.SIGCONT)
    wait_until(lambda: page.panels()["C"]["status"] == "up")


def test_element_names_are_shown_as_text_not_run_as_html(page):
    evil = "<img src=x onerror=window.__ran=1>"
    page.add("A", evil)
    wait_until(lambda: everywhere(page, lambda p: p["set"] == [evil]))
    assert page.eval("window.__ran === undefined")


def test_page_says_so_when_run_py_goes_away(page, run_py):  # noqa: F811
    run_py.send_signal(signal.SIGTERM)
    run_py.wait(timeout=10)
    wait_until(lambda: everywhere(page, lambda p: p["status"] in ("unknown", "down")))
    assert everywhere(page, lambda p: not p["kill"] and not p["restart"])
    assert "run.py" in page.eval("document.getElementById('error').textContent")


def test_text_typed_in_one_panel_appears_in_every_panel(page):
    page.type("A", "hello ")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "hello "))
    page.type("C", "world")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "hello world"))
    page.click("B", ".backspace")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "hello worl"))
    assert page.problems() == []


def test_backspace_removes_a_whole_emoji(page):
    # The browser counts an emoji as two characters (UTF-16); the replica
    # counts one. Getting this wrong would delete the wrong character.
    page.type("A", "a😀")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "a😀"))
    page.click("A", ".backspace")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "a"))


def test_two_quick_additions_both_land_in_order(page):
    # Both submitted before the first reply comes back. If the page worked
    # out "the end" itself from its last poll, both would use the same
    # position and could come out reversed.
    page.eval("""(() => {
        const c = [...document.querySelectorAll('.card')].find(c => c.querySelector('.name').textContent.endsWith('A'));
        for (const t of ["first ", "second"]) {
            c.querySelector('.typed').value = t;
            c.querySelector('.type-form').requestSubmit();
        }
    })()""")
    wait_until(lambda: everywhere(page, lambda p: p["text"] == "first second"))


# ---- the network controls --------------------------------------------------------------

def link_row(page, src, dst):
    return f"[...document.querySelectorAll('#links tr')].find(tr => tr.firstChild.textContent === '{src} → {dst}')"


def test_cut_off_a_replica_edit_both_sides_then_heal(page):
    page.eval("document.querySelector('[data-isolate=\"A\"]').click()")
    wait_until(lambda: page.panels()["B"]["peers"]["A"] == "suspected", timeout=8)
    assert "A→B" in page.eval("document.getElementById('network-summary').textContent")
    assert page.eval(f"{link_row(page, 'A', 'B')}.querySelector('input').checked")

    page.type("A", "left ")
    page.type("B", "right ")
    wait_until(lambda: page.panels()["C"]["text"] == "right ")
    time.sleep(1.5)
    assert page.panels()["A"]["text"] == "left "  # the two sides disagree

    page.eval("document.getElementById('heal').click()")
    wait_until(lambda: len({p["text"] for p in page.panels().values()}) == 1
               and len(page.panels()["A"]["text"]) == len("left right "))
    assert page.eval("document.getElementById('network-summary').textContent") == "no links cut"
    assert page.problems() == []


def test_a_link_set_to_drop_everything_shows_its_dropped_count(page):
    page.eval(f"""(() => {{
        const select = {link_row(page, 'A', 'B')}.querySelectorAll('select')[0];
        select.value = "1";
        select.dispatchEvent(new Event('change'));
    }})()""")
    wait_until(lambda: supervisor({"type": "status"})["links"]["A>B"]["drop"] == 1.0)
    # Syncs keep flowing every second, so the dropped count climbs.
    wait_until(lambda: int(page.eval(f"{link_row(page, 'A', 'B')}.lastChild.textContent").split(" · ")[1]) > 0)



def test_cut_checkbox_cuts_one_direction_only(page):
    page.eval(f"""(() => {{
        const box = {link_row(page, 'A', 'B')}.querySelector('input');
        box.checked = true;
        box.dispatchEvent(new Event('change'));
    }})()""")
    wait_until(lambda: supervisor({"type": "status"})["links"]["A>B"]["cut"] is True)
    assert supervisor({"type": "status"})["links"]["B>A"]["cut"] is False
    wait_until(lambda: page.panels()["B"]["peers"]["A"] == "suspected", timeout=8)
    assert page.panels()["A"]["peers"]["B"] == "up"


def test_a_value_set_elsewhere_is_shown_not_left_blank(page):
    supervisor({"type": "link", "from": "C", "to": "A", "settings": {"drop": 0.3, "delay_ms": 4000}})
    wait_until(lambda: page.eval(f"{link_row(page, 'C', 'A')}.querySelectorAll('select')[0].selectedOptions[0]?.textContent") == "30%")
    assert page.eval(f"{link_row(page, 'C', 'A')}.querySelectorAll('select')[2].selectedOptions[0].textContent") == "4000 ms"
    assert "other faults on: C→A" in page.eval("document.getElementById('network-summary').textContent")


def test_network_controls_are_disabled_when_run_py_goes_away(page, run_py):  # noqa: F811
    wait_until(lambda: page.eval("document.querySelectorAll('#links tr').length") == 6)
    run_py.send_signal(signal.SIGTERM)
    run_py.wait(timeout=10)
    wait_until(lambda: page.eval("[...document.querySelectorAll('#links input, #links select')].every(e => e.disabled)"))
    assert "unknown" in page.eval("document.getElementById('network-summary').textContent")


def test_demo_step_where_an_edit_waits_in_the_buffer(page):
    # Step 6 of the demo script, clicked the way a person would.
    def set_select(src, dst, index, value):
        page.eval(f"""(() => {{
            const s = {link_row(page, src, dst)}.querySelectorAll('select')[{index}];
            s.value = "{value}"; s.dispatchEvent(new Event('change'));
        }})()""")
    set_select("A", "C", 2, 5000)  # delay
    page.eval(f"""(() => {{
        const box = {link_row(page, 'C', 'B')}.querySelector('input');
        box.checked = true; box.dispatchEvent(new Event('change'));
    }})()""")
    wait_until(lambda: supervisor({"type": "status"})["links"]["C>B"]["cut"])

    page.type("A", "h")
    time.sleep(0.7)  # moving the mouse over to B
    wait_until(lambda: page.panels()["B"]["text"] == "h")
    page.type("B", "i")

    def c_meta():
        return page.eval("[...document.querySelectorAll('.text-meta')][2].textContent")
    wait_until(lambda: "1 waiting in buffer" in c_meta())
    assert page.panels()["C"]["text"] == ""  # neither shown yet
    wait_until(lambda: page.panels()["C"]["text"] == "hi", timeout=8)
    assert "0 waiting in buffer (1 have waited so far)" in c_meta()
