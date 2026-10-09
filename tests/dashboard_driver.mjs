// Opens a page in headless Chrome and runs JavaScript in it on request.
//
// Usage: node dashboard_driver.mjs <chrome-path> <debug-port> <url>
// Reads one JSON request per line on stdin and answers one JSON line on stdout:
//   {"eval": "<js expression>"}  -> {"value": ...} or {"error": "..."}
//   {"problems": true}           -> {"value": [JS errors and console warnings so far]}
// Closing stdin closes the browser. tests/test_dashboard_browser.py drives this.
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";

const [chromePath, debugPort, url] = process.argv.slice(2);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const chrome = spawn(chromePath, [
  "--headless=new", `--remote-debugging-port=${debugPort}`, "--window-size=1280,900",
  `--user-data-dir=${mkdtempSync(join(tmpdir(), "crdt-chrome-"))}`, "about:blank",
], { stdio: "ignore" });

let ws;
let nextId = 1;
const pending = new Map();
const problems = [];

function cdp(method, params = {}) {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

async function pageSocketUrl() {
  for (let i = 0; i < 100; i++) {
    try {
      const tabs = await (await fetch(`http://127.0.0.1:${debugPort}/json/list`)).json();
      const page = tabs.find((t) => t.type === "page");
      if (page) return page.webSocketDebuggerUrl;
    } catch {}
    await sleep(100);
  }
  throw new Error("Chrome did not start");
}

const reply = (obj) => process.stdout.write(JSON.stringify(obj) + "\n");

try {
  ws = new WebSocket(await pageSocketUrl());
  await new Promise((r) => (ws.onopen = r));
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.id && pending.has(m.id)) {
      const p = pending.get(m.id);
      pending.delete(m.id);
      m.error ? p.reject(new Error(m.error.message)) : p.resolve(m.result);
    } else if (m.method === "Runtime.exceptionThrown") {
      const d = m.params.exceptionDetails;
      problems.push("exception: " + (d.exception?.description ?? d.text));
    } else if (m.method === "Runtime.consoleAPICalled" && ["error", "warning"].includes(m.params.type)) {
      problems.push(`console.${m.params.type}: ` + m.params.args.map((a) => a.value ?? a.description).join(" "));
    }
  };
  await cdp("Runtime.enable");
  await cdp("Page.enable");
  await cdp("Page.navigate", { url });
  reply({ ready: true });

  for await (const line of createInterface({ input: process.stdin })) {
    const req = JSON.parse(line);
    if (req.problems) {
      reply({ value: problems });
      continue;
    }
    try {
      const r = await cdp("Runtime.evaluate", { expression: req.eval, returnByValue: true });
      if (r.exceptionDetails) reply({ error: r.exceptionDetails.exception?.description ?? r.exceptionDetails.text });
      else reply({ value: r.result.value ?? null });
    } catch (e) {
      reply({ error: e.message });
    }
  }
} catch (e) {
  reply({ error: e.message });
} finally {
  chrome.kill();
}
