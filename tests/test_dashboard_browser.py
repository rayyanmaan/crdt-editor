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

    def remove(self, rid, element):
        self.eval(f"""[...{self.card(rid)}.querySelectorAll('.chip')]
            .find(ch => ch.firstChild.textContent === {json.dumps(element)})
            .querySelector('button').click()""")


def all_up(panels):
    # One read of the page, checked as a whole. (Reading it twice once let
    # an empty page pass: all() of nothing is True.)
    return len(panels) == 3 and all(x["status"] == "up" for x in panels.values())


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
