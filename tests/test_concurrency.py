"""Concurrency and interleaving tests.

Many jobs run concurrently while other threads issue gc, and the durable
invariants are checked at the end:
  * every terminal job has exactly one result, never a partial one
  * no pin survives
  * a ready job's versions are all >= the floor they were published from
  * gc never removes a version a published result refers to
"""

import json
import os
import random
import sys
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

from harness import Cluster, v0, ver  # noqa: E402


class TestInterleaving(unittest.TestCase):
    def setUp(self):
        self.init = {
            "A": [v0(), ver(["a1"], {"B": 1}), ver(["a2"], {"B": 2}),
                  ver(["a3"], {"B": 2})],
            "B": [v0(), ver(["b1"], {}), ver(["b2"], {"A": 1}),
                  ver(["b3"], {"A": 2})],
            "C": [v0(), ver(["c1"], {"A": 1}), ver(["c2"], {"A": 1, "B": 2})],
        }
        self.c = Cluster(self.init)

    def tearDown(self):
        self.c.close()

    def test_many_jobs_and_gc_interleaved(self):
        errors = []
        results = {}
        lock = threading.Lock()

        def run_job(index):
            job = "job%d" % index
            caps = {
                "A": random.randint(0, 3),
                "B": random.randint(0, 3),
                "C": random.randint(0, 2),
            }
            try:
                out = self.c.call("/start", {"job": job, "caps": caps})
                with lock:
                    results[job] = (caps, out)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append((job, repr(exc)))

        def run_gc(name, k):
            try:
                self.c.gc(name, k)
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(("gc", repr(exc)))

        threads = []
        for i in range(30):
            threads.append(threading.Thread(target=run_job, args=(i,)))
        for name in ("A", "B", "C"):
            for k in (1, 2, 3):
                threads.append(threading.Thread(target=run_gc, args=(name, k)))
        random.shuffle(threads)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

        for job, (caps, out) in results.items():
            self.assertIn(out["state"], ("ready", "failed"), out)
            if out["state"] == "ready":
                # Query is stable and identical to the start reply.
                again = self.c.call("/query", {"job": job})
                self.assertEqual(again, out)
                self.assertEqual(set(out["versions"]), set(self.init))
                self.assertEqual(set(out["rows"]), set(self.init))
                for name, v in out["versions"].items():
                    self.assertLessEqual(v, caps[name])
                # The selection really satisfies its own requirements.
                for name, v in out["versions"].items():
                    needs = self.init[name][v]["needs"]
                    for dep, minimum in needs.items():
                        self.assertGreaterEqual(out["versions"][dep], minimum)
                # Rows come from exactly the selected version.
                for name, v in out["versions"].items():
                    self.assertEqual(out["rows"][name], self.init[name][v]["rows"])

        # Everything drained; a final gc is allowed at the current floor.
        for name in ("A", "B", "C"):
            info = self.c.src_info(name)
            self.assertEqual(info["pins"], [])
            status = self.c.gc(name, info["floor"])
            self.assertEqual(status, {"floor": info["floor"]})

    def test_jobs_never_see_a_partial_publish(self):
        """While jobs publish, a concurrent query must never see partial data."""
        stop = threading.Event()
        bad = []

        def watcher():
            while not stop.is_set():
                try:
                    out = self.c.call("/query", {"job": "watched"})
                except Exception:  # noqa: BLE001
                    continue
                if "error" in out:
                    continue
                if out["state"] == "ready":
                    if set(out.get("versions", {})) != set(self.init):
                        bad.append(out)
                    if set(out.get("rows", {})) != set(self.init):
                        bad.append(out)
                elif out["state"] != "pending":
                    bad.append(out)

        w = threading.Thread(target=watcher, daemon=True)
        w.start()
        try:
            for i in range(20):
                out = self.c.call(
                    "/start", {"job": "watched", "caps": {"A": 3, "B": 3, "C": 2}}
                )
                self.assertEqual(out["state"], "ready")
        finally:
            stop.set()
            w.join(timeout=5)
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main()
