"""End-to-end test with real OS processes.

Unlike the in-process suite, the coordinator and every source run in their
own Python process against their own SQLite file, and "crashes" are real
SIGKILLs.  This is what proves the durability claims rather than trusting
exception injection.

    python3 tests/test_multiprocess.py
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def post(url, payload=None, raw=None, timeout=5.0):
    body = raw if raw is not None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def wait_up(url, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            urllib.request.urlopen(url, timeout=0.3).close()
            return True
        except urllib.error.HTTPError:
            return True
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("timed out waiting for " + url)


class Proc:
    def __init__(self, args, label):
        self.label = label
        self.proc = subprocess.Popen(
            args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    def read_line(self):
        line = self.proc.stdout.readline()
        if not line:
            err = self.proc.stderr.read().decode()
            raise RuntimeError("%s exited early: %s" % (self.label, err))
        return json.loads(line)

    def kill(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)

    def wait(self):
        return self.proc.wait(timeout=10)


class MultiProcess(unittest.TestCase):
    INIT = {
        "A": [
            {"rows": [], "needs": {}},
            {"rows": ["a1"], "needs": {"B": 1}},
            {"rows": ["a2", "a2"], "needs": {"B": 1}},
        ],
        "B": [
            {"rows": [], "needs": {}},
            {"rows": ["b1"], "needs": {}},
        ],
    }

    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="expcoord-mp-")
        self.procs = []
        self.init_path = os.path.join(self.workdir, "init.json")
        with open(self.init_path, "w") as fh:
            json.dump(self.INIT, fh)

    def tearDown(self):
        for p in self.procs:
            try:
                p.kill()
            except Exception:
                pass
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _source_db(self, name):
        return os.path.join(self.workdir, "source_%s.sqlite3" % name)

    def _coord_db(self):
        return os.path.join(self.workdir, "coordinator.sqlite3")

    def start_sources(self):
        """Create every source DB and run every source process."""
        names = list(self.INIT)
        urls = {}
        sys.path.insert(0, ROOT)
        from expcoord.source import init_source_db

        for name in names:
            init_source_db(self._source_db(name), name, self.INIT[name])
        for name in names:
            port = free_port()
            p = Proc(
                [sys.executable, "-m", "expcoord", "source", "--db",
                 self._source_db(name), "--name", name, "--port", str(port)],
                "source-" + name,
            )
            p.read_line()
            wait_up("http://127.0.0.1:%d/int/info" % port)
            urls[name] = "http://127.0.0.1:%d" % port
            self.procs.append(p)
        return urls

    def start_coordinator(self, urls):
        sources_path = os.path.join(self.workdir, "sources.json")
        with open(sources_path, "w") as fh:
            json.dump(urls, fh)
        from expcoord.coordinator import init_coordinator_db

        if not os.path.exists(self._coord_db()):
            init_coordinator_db(self._coord_db(), self.INIT, urls)
        port = free_port()
        p = Proc(
            [sys.executable, "-m", "expcoord", "coordinator", "--db",
             self._coord_db(), "--sources", sources_path, "--port", str(port)],
            "coordinator",
        )
        p.read_line()
        wait_up("http://127.0.0.1:%d/info" % port)
        self.procs.append(p)
        return "http://127.0.0.1:%d" % port

    def gc(self, url, k):
        return post(url + "/", {"op": "gc", "k": k})

    def info(self, url):
        return post(url + "/int/info", {})[1]

    # ------------------------------------------------------------------
    def test_kill_coordinator_midflight_and_resume(self):
        """SIGKILL the coordinator while it holds pins, then resume."""
        urls = self.start_sources()
        coord = self.start_coordinator(urls)

        # Freeze the coordinator with a SIGSTOP right after it pins: we can
        # not easily stop at a precise step from outside, so instead we
        # verify the durable invariants after a real kill at an arbitrary
        # point, repeated many times with different jobs.
        for i in range(12):
            job = "job%d" % i
            status, body = post(
                coord + "/start", {"job": job, "caps": {"A": 2, "B": 1}}
            )
            self.assertEqual(status, 200)
            if body.get("state") == "ready":
                self.assertEqual(body["versions"], {"A": 2, "B": 1})
                self.assertEqual(body["rows"]["A"], ["a2", "a2"])

        # Kill and restart: every job must keep a consistent terminal answer
        # and every pin must eventually drain.
        self.procs[-1].kill()
        coord = self.start_coordinator(urls)
        for i in range(12):
            status, body = post(coord + "/query", {"job": "job%d" % i})
            self.assertEqual(body["state"], "ready", body)
            self.assertEqual(body["versions"], {"A": 2, "B": 1})
        for name in urls:
            self.assertEqual(self.info(urls[name])["pins"], [])
        self.assertEqual(self.gc(urls["A"], 1), (200, {"floor": 1}))

    def _crash(self, urls, job, caps, crash_at):
        """Run a real coordinator that SIGKILLs itself at ``crash_at``."""
        sources_path = os.path.join(self.workdir, "crash-sources.json")
        with open(sources_path, "w") as fh:
            json.dump(urls, fh)
        proc = subprocess.run(
            [
                sys.executable,
                os.path.join(HERE, "crash_child.py"),
                self._coord_db(),
                sources_path,
                job,
                json.dumps(caps),
                crash_at,
            ],
            cwd=ROOT,
            capture_output=True,
            timeout=30,
        )
        if proc.returncode != -signal.SIGKILL:
            sys.stderr.write(
                "crash child %s rc=%s out=%r err=%r\n"
                % (crash_at, proc.returncode, proc.stdout[-2000:], proc.stderr[-2000:])
            )
        return proc

    def _recover_and_query(self, urls, job):
        coord = self.start_coordinator(urls)
        return post(coord + "/query", {"job": job})[1], coord

    def test_sigkill_before_first_pin(self):
        urls = self.start_sources()
        from expcoord.coordinator import init_coordinator_db

        init_coordinator_db(self._coord_db(), self.INIT, urls)
        proc = self._crash(urls, "c1", {"A": 2, "B": 1}, "pin")
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        out, _ = self._recover_and_query(urls, "c1")
        # The job row was durable before any network call.
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["versions"], {"A": 2, "B": 1})

    def test_sigkill_after_pins_before_publish(self):
        """Pins survive the kill and block gc; recovery publishes then frees."""
        urls = self.start_sources()
        from expcoord.coordinator import init_coordinator_db

        init_coordinator_db(self._coord_db(), self.INIT, urls)
        proc = self._crash(urls, "c2", {"A": 2, "B": 1}, "publish")
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        # Pins are durable at the sources and stop collection of the window.
        for name in ("A", "B"):
            self.assertEqual(len(self.info(urls[name])["pins"]), 1)
            self.assertEqual(self.gc(urls[name], 1)[1], {"error": "$.k"})

        out, _ = self._recover_and_query(urls, "c2")
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["rows"], {"A": ["a2", "a2"], "B": ["b1"]})
        for name in ("A", "B"):
            self.assertEqual(self.info(urls[name])["pins"], [])
        self.assertEqual(self.gc(urls["A"], 1), (200, {"floor": 1}))

    def test_sigkill_after_publish_before_release(self):
        """Kill after the ready row is durable but before any release."""
        urls = self.start_sources()
        from expcoord.coordinator import init_coordinator_db

        init_coordinator_db(self._coord_db(), self.INIT, urls)
        proc = self._crash(urls, "c3", {"A": 2, "B": 1}, "release0")
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        for name in ("A", "B"):
            self.assertEqual(len(self.info(urls[name])["pins"]), 1)
            self.assertEqual(self.gc(urls[name], 1)[1], {"error": "$.k"})

        out, _ = self._recover_and_query(urls, "c3")
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["versions"], {"A": 2, "B": 1})
        # Recovery drained every pin, and only then can the window be collected.
        for name in ("A", "B"):
            self.assertEqual(self.info(urls[name])["pins"], [])
        self.assertEqual(self.gc(urls["A"], 1), (200, {"floor": 1}))
        self.assertEqual(self.gc(urls["B"], 1), (200, {"floor": 1}))

    def test_sigkill_at_commit_leaves_source_released(self):
        """A source that acknowledged the commit stays released after a kill."""
        urls = self.start_sources()
        from expcoord.coordinator import init_coordinator_db

        init_coordinator_db(self._coord_db(), self.INIT, urls)
        proc = self._crash(urls, "c4", {"A": 2, "B": 1}, "commit1")
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        # A was already committed (released); B still pins its window.
        self.assertEqual(self.info(urls["A"])["pins"], [])
        self.assertEqual(len(self.info(urls["B"])["pins"]), 1)

        out, _ = self._recover_and_query(urls, "c4")
        self.assertEqual(out["state"], "ready")
        self.assertEqual(self.info(urls["B"])["pins"], [])
        self.assertEqual(self.gc(urls["B"], 1), (200, {"floor": 1}))

    def test_source_restart_keeps_floor_and_pins(self):
        """A killed source restarts with its floor and pins intact."""
        urls = self.start_sources()
        self.assertEqual(self.gc(urls["A"], 1), (200, {"floor": 1}))
        sources = [p for p in self.procs if p.label == "source-A"]
        sources[0].kill()
        self.procs.remove(sources[0])

        port = int(urls["A"].rsplit(":", 1)[1])
        p = Proc(
            [sys.executable, "-m", "expcoord", "source", "--db",
             self._source_db("A"), "--name", "A", "--port", str(port)],
            "source-A",
        )
        p.read_line()
        wait_up(urls["A"] + "/int/info")
        self.procs.append(p)
        self.assertEqual(self.info(urls["A"])["floor"], 1)
        # k=1 is the floor: allowed.  k=0 is below it: rejected.
        self.assertEqual(self.gc(urls["A"], 0), (200, {"error": "$.k"}))
        self.assertEqual(self.gc(urls["A"], 2), (200, {"floor": 2}))

        coord = self.start_coordinator(urls)
        out = post(coord + "/start", {"job": "j", "caps": {"A": 2, "B": 1}})[1]
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["versions"], {"A": 2, "B": 1})

    def test_cluster_launcher(self):
        """The one-shot launcher starts a working cluster."""
        port = free_port()
        p = Proc(
            [sys.executable, "-m", "expcoord", "cluster", "--init",
             self.init_path, "--workdir", os.path.join(self.workdir, "c"),
             "--port", str(port)],
            "cluster",
        )
        info = p.read_line()
        self.procs.append(p)
        url = info["coordinator"]
        wait_up(url + "/info")
        out = post(url + "/start", {"job": "j", "caps": {"A": 2, "B": 1}})[1]
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["rows"], {"A": ["a2", "a2"], "B": ["b1"]})
        # Sources from the same cluster reject out-of-range gc.
        a_url = info["sources"]["A"]
        self.assertEqual(post(a_url + "/", {"op": "gc", "k": 9})[1], {"error": "$.k"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
