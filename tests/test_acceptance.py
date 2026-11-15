"""Acceptance tests.  Run with: python3 -m unittest discover -s tests -v

Scenarios covered:
  * example A1 needs B1, caps {A:1,B:0} -> ready {A:0,B:0}
  * dependency forcing and the componentwise maximum
  * validation errors and their JSONPaths
  * gc bounds, idempotent replay, permanent release
  * gc/snapshot race (rejected) and gc before a job (collected cap)
  * crash after pinning, after publishing, before release
  * lost responses with a fault-injecting proxy
  * cyclic dependencies (reachable when caps permit, infeasible otherwise)
"""

import json
import os
import sys
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

from harness import Cluster, CrashSignal, CrashyHttp, FaultProxy, v0, ver  # noqa: E402
from expcoord import common  # noqa: E402
from expcoord.coordinator import Coordinator  # noqa: E402
from expcoord.source import init_source_db, run_source  # noqa: E402


class Base(unittest.TestCase):
    init = None

    def setUp(self):
        self.c = Cluster(self.init)

    def tearDown(self):
        self.c.close()

    def start(self, job, caps):
        return self.c.call("/start", {"job": job, "caps": caps})

    def query(self, job):
        return self.c.call("/query", {"job": job})


class TestExample(Base):
    init = {
        "A": [v0(), ver(["a1"], {"B": 1})],
        "B": [v0(), ver(["b1"], {})],
    }

    def test_a1_requires_b1_but_cap_b0(self):
        # A1 needs B1, but B's cap is 0: the maximum feasible A is 0.
        out = self.start("j1", {"A": 1, "B": 0})
        self.assertEqual(out, {"state": "ready", "versions": {"A": 0, "B": 0},
                               "rows": {"A": [], "B": []}})
        self.assertEqual(self.query("j1")["state"], "ready")

    def test_both_caps_allow_a1(self):
        out = self.start("j1", {"A": 1, "B": 1})
        self.assertEqual(out["versions"], {"A": 1, "B": 1})
        self.assertEqual(out["rows"], {"A": ["a1"], "B": ["b1"]})

    def test_componentwise_maximum(self):
        # A2 needs B1, B2 needs nothing: caps (2,2) -> both 2.
        init = {
            "A": [v0(), ver(["a1"], {"B": 1}), ver(["a2"], {"B": 1})],
            "B": [v0(), ver(["b1"], {}), ver(["b2"], {})],
        }
        self.c.close()
        self.c = Cluster(init)
        out = self.start("j", {"A": 2, "B": 2})
        self.assertEqual(out["versions"], {"A": 2, "B": 2})

        # Lower B's cap and A must fall back to the largest A that fits.
        # (needs are non-decreasing, so A2 requires at least A1's B.)
        init2 = {
            "A": [v0(), ver(["a1"], {"B": 1}), ver(["a2"], {"B": 2})],
            "B": [v0(), ver(["b1"], {}), ver(["b2"], {"A": 1})],
        }
        self.c.close()
        self.c = Cluster(init2)
        out = self.start("k", {"A": 2, "B": 1})
        self.assertEqual(out["versions"], {"A": 1, "B": 1})
        self.assertEqual(out["rows"]["A"], ["a1"])


class TestValidation(Base):
    init = {
        "A": [v0(), ver(["a1"], {"B": 1})],
        "B": [v0(), ver(["b1"], {})],
    }

    def test_bad_job_name(self):
        for bad in ["", "a-b", "a b", "é", 12, None, True]:
            out = self.start(bad, {"A": 0, "B": 0})
            self.assertIn("error", out, bad)
            self.assertEqual(out["error"], "$.job")

    def test_caps_exact_cover(self):
        self.assertEqual(self.start("j", {"A": 0})["error"], "$.caps")
        self.assertEqual(self.start("j", {"A": 0, "B": 0, "C": 0})["error"], "$.caps")
        self.assertEqual(self.start("j", [])["error"], "$.caps")

    def test_cap_values(self):
        self.assertEqual(self.start("j", {"A": 2, "B": 0})["error"], "$.caps.A")
        self.assertEqual(self.start("j", {"A": -1, "B": 0})["error"], "$.caps.A")
        self.assertEqual(self.start("j", {"A": True, "B": 0})["error"], "$.caps.A")
        self.assertEqual(self.start("j", {"A": 0.0, "B": 0})["error"], "$.caps.A")
        self.assertEqual(self.start("j", {"A": "0", "B": 0})["error"], "$.caps.A")

    def test_unknown_job_query(self):
        self.assertEqual(self.query("nope")["error"], "$.job")
        self.assertEqual(self.c.call("/query", {})["error"], "$.job")

    def test_syntax_error(self):
        status, body = self.c.raw_post(self.c.coord_url + "/start", b"{oops")
        self.assertEqual((status, body), (200, {"error": "$"}))
        status, body = self.c.raw_post(self.c.coord_url + "/start", b"[]")
        self.assertEqual((status, body), (200, {"error": "$"}))

    def test_same_caps_replay_and_conflict(self):
        first = self.start("j", {"A": 1, "B": 1})
        self.assertEqual(first["state"], "ready")
        again = self.start("j", {"A": 1, "B": 1})
        self.assertEqual(again, first)
        conflict = self.start("j", {"A": 0, "B": 0})
        self.assertEqual(conflict["error"], "$.caps")
        # Rejection leaves the original result untouched.
        self.assertEqual(self.query("j"), first)

    def test_failed_job_is_terminal(self):
        # A1 needs B1; with B capped at 0 the only feasible tuple is (0,0),
        # which is fine.  Force failure by collecting A0 first?  A0 can be
        # collected, but then the cap 0 is out of range, caught earlier.
        # Instead use an infeasible floor produced by gc on a later start.
        self.assertEqual(self.c.gc("A", 1), {"floor": 1})
        out = self.start("j2", {"A": 1, "B": 0})
        # A1 needs B1, B is capped at 0 -> A must drop below floors -> failed
        self.assertEqual(out["state"], "failed")
        self.assertEqual(self.query("j2"), {"state": "failed"})
        self.assertEqual(self.start("j2", {"A": 1, "B": 0}), {"state": "failed"})


class TestGc(Base):
    init = {
        "A": [v0(), ver(["a1"], {}), ver(["a2"], {})],
        "B": [v0(), ver(["b1"], {})],
    }

    def test_gc_bounds_durable(self):
        # k must lie in [current floor, last version]; k == floor is a no-op.
        self.assertEqual(self.c.gc("A", 0), {"floor": 0})
        self.assertEqual(self.c.gc("A", 1), {"floor": 1})
        self.assertEqual(self.c.gc("A", 1), {"floor": 1})
        self.assertEqual(self.c.gc("A", 3)["error"], "$.k")
        self.assertEqual(self.c.gc("A", 2), {"floor": 2})
        self.assertEqual(self.c.gc("A", 1)["error"], "$.k")
        self.assertEqual(self.c.gc("A", -1)["error"], "$.k")
        self.assertEqual(self.c.gc("A", True)["error"], "$.k")
        self.assertEqual(self.c.gc("A", 7)["error"], "$.k")

    def test_gc_then_start(self):
        self.c.gc("A", 1)
        out = self.start("j", {"A": 2, "B": 1})
        self.assertEqual(out["versions"], {"A": 2, "B": 1})
        self.assertEqual(out["rows"]["A"], ["a2"])

    def test_cap_below_floor_fails(self):
        self.c.gc("A", 2)
        out = self.start("j", {"A": 1, "B": 1})
        self.assertEqual(out["state"], "failed")

    def test_gc_pinned_window_rejected(self):
        # Use a crash to stop the coordinator between pinning and committing.
        coord_db = os.path.join(self.c.workdir, "coordinator.sqlite3")
        http = CrashyHttp()
        crashy = Coordinator(coord_db, http)
        http.crash_on(
            lambda url, p: url.endswith("/int/commit") and p.get("job") == "jc"
        )
        with self.assertRaises(CrashSignal):
            crashy.start("jc", {"A": 2, "B": 1})
        # A is pinned at floor 0; gc(1) must be refused while it stands.
        self.assertEqual(self.c.gc("A", 1)["error"], "$.k")
        self.assertEqual(self.c.gc("B", 1)["error"], "$.k")

        # Recovery publishes from the durable plan and releases.
        crashy2 = Coordinator(coord_db, common.Http())
        crashy2.recover()
        out = self.c.call("/query", {"job": "jc"})
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["versions"], {"A": 2, "B": 1})
        self.assertEqual(self.c.gc("A", 1), {"floor": 1})


class TestDependencies(Base):
    init = {
        "A": [v0(), ver(["a1"], {"B": 1}), ver(["a2"], {"B": 2})],
        "B": [v0(), ver(["b1"], {"A": 1}), ver(["b2"], {"A": 2})],
    }

    def test_mutual_dependency_capable(self):
        # A2<->B2 is a realisable cycle: both at their required versions.
        out = self.start("j", {"A": 2, "B": 2})
        self.assertEqual(out["versions"], {"A": 2, "B": 2})
        self.assertEqual(out["rows"], {"A": ["a2"], "B": ["b2"]})

    def test_cycle_where_one_side_is_capped(self):
        out = self.start("j", {"A": 2, "B": 1})
        # B1 requires A1; A1 requires B1.  A2 would need B2, which needs A2,
        # but B is capped at 1, so the maximum feasible tuple is (1,1).
        self.assertEqual(out["versions"], {"A": 1, "B": 1})
        out = self.start("k", {"A": 2, "B": 0})
        self.assertEqual(out["versions"], {"A": 0, "B": 0})

    def test_self_dependency(self):
        init = {"A": [v0(), ver(["a1"], {}), ver(["a2"], {"A": 1})]}
        self.c.close()
        self.c = Cluster(init)
        self.assertEqual(self.start("j", {"A": 2})["versions"], {"A": 2})

    def test_rows_preserve_order_and_duplicates(self):
        init = {"A": [v0(), ver(["x", "x", "y"], {})]}
        self.c.close()
        self.c = Cluster(init)
        out = self.start("j", {"A": 1})
        self.assertEqual(out["rows"]["A"], ["x", "x", "y"])


class TestCrash(Base):
    init = {
        "A": [v0(), ver(["a1"], {"B": 1})],
        "B": [v0(), ver(["b1"], {})],
    }

    def _crashy(self):
        coord_db = os.path.join(self.c.workdir, "coordinator.sqlite3")
        http = CrashyHttp()
        return coord_db, http, Coordinator(coord_db, http)

    def test_crash_after_some_pins(self):
        coord_db, http, crashy = self._crashy()
        http.crash_on(lambda url, p: url.endswith("/int/pin") and p.get("job") == "j")
        with self.assertRaises(CrashSignal):
            crashy.start("j", {"A": 1, "B": 1})
        self.assertEqual(self.query("j")["state"], "pending")
        # Pending jobs can be collected past only where not pinned.
        fresh = Coordinator(coord_db, common.Http())
        fresh.recover()
        out = self.c.call("/query", {"job": "j"})
        self.assertEqual(out["state"], "ready")
        self.assertEqual(out["versions"], {"A": 1, "B": 1})
        self.assertEqual(self.c.src_info("A")["pins"], [])
        self.assertEqual(self.c.src_info("B")["pins"], [])

    def test_crash_while_releasing_after_publish(self):
        """Publish is durable before any release; a crash there is finished later.

        The commit (release) at the sources has not been sent yet, so pins
        still block gc.  The ready result is already durable, which is the
        point: the result can never be lost once nothing protects it.
        """
        coord_db, http, crashy = self._crashy()
        http.crash_on(
            lambda url, p: url.endswith("/int/release") and p.get("job") == "j"
        )
        with self.assertRaises(CrashSignal):
            crashy.start("j", {"A": 1, "B": 1})
        # Publication happened before release, so the result is already
        # durable even though the process died mid-release.
        result = self.query("j")
        self.assertEqual(result["state"], "ready")
        self.assertEqual(result["versions"], {"A": 1, "B": 1})
        # The first source was already released when the crash hit; the
        # second still protects its window.
        self.assertEqual(self.c.src_info("A")["pins"], [])
        self.assertEqual(len(self.c.src_info("B")["pins"]), 1)
        self.assertEqual(self.c.gc("B", 1)["error"], "$.k")

        fresh = Coordinator(coord_db, common.Http())
        fresh.recover()
        self.assertEqual(self.c.src_info("A")["pins"], [])
        self.assertEqual(self.c.src_info("B")["pins"], [])
        self.assertEqual(self.c.gc("A", 1), {"floor": 1})
        self.assertEqual(self.c.gc("B", 1), {"floor": 1})
        # The published result is unchanged by the late cleanup.
        self.assertEqual(self.query("j"), result)

    def test_crash_between_commit_calls(self):
        """Crash after publishing, before the first commit: recovery releases."""
        coord_db, http, crashy = self._crashy()
        http.crash_on(
            lambda url, p: url.endswith("/int/commit") and p.get("job") == "j"
        )
        with self.assertRaises(CrashSignal):
            crashy.start("j", {"A": 1, "B": 1})
        # Publish is durable first, so the job already reads ready while the
        # sources still protect its window.
        self.assertEqual(self.query("j")["state"], "ready")
        self.assertEqual(len(self.c.src_info("A")["pins"]), 1)
        self.assertEqual(len(self.c.src_info("B")["pins"]), 1)
        self.assertEqual(self.c.gc("B", 1)["error"], "$.k")

        crashy2 = Coordinator(coord_db, common.Http())
        crashy2.recover()
        out = self.c.call("/query", {"job": "j"})
        self.assertEqual(out, {"state": "ready", "versions": {"A": 1, "B": 1},
                               "rows": {"A": ["a1"], "B": ["b1"]}})
        self.assertEqual(self.c.src_info("B")["pins"], [])
        self.assertEqual(self.c.gc("B", 1), {"floor": 1})

    def test_lost_response_on_commit(self):
        """A dropped commit reply must not corrupt or duplicate the job."""
        upstream = self.c.source_urls["B"]
        with FaultProxy(upstream, lambda path, body: path.endswith("/int/commit"),
                        mode="drop_reply") as proxy:
            coord_db = os.path.join(self.c.workdir, "coordinator.sqlite3")
            http = common.Http(timeout=2.0)
            coord = Coordinator(coord_db, http)
            coord.sources["B"] = proxy.url
            out = coord.start("j", {"A": 1, "B": 1})
            # The publish is durable before commit, so the job is ready; the
            # commit at B did land (its pin is gone) but its reply was lost.
            self.assertEqual(out["state"], "ready")
            self.assertEqual(self.c.src_info("B")["pins"], [])
            coord2 = Coordinator(coord_db, common.Http())
            coord2.recover()
            out2 = self.c.call("/query", {"job": "j"})
            self.assertEqual(out2, {"state": "ready", "versions": {"A": 1, "B": 1},
                                    "rows": {"A": ["a1"], "B": ["b1"]}})
            self.assertEqual(self.c.src_info("A")["pins"], [])
            self.assertEqual(self.c.gc("A", 1), {"floor": 1})

    def test_lost_response_on_pin(self):
        upstream = self.c.source_urls["A"]
        with FaultProxy(upstream, lambda path, body: path.endswith("/int/pin"),
                        mode="drop_reply") as proxy:
            coord_db = os.path.join(self.c.workdir, "coordinator.sqlite3")
            http = common.Http(timeout=2.0)
            coord = Coordinator(coord_db, http)
            coord.sources["A"] = proxy.url
            out = coord.start("j", {"A": 1, "B": 1})
            self.assertEqual(out["state"], "pending")
            # A is pinned even though the coordinator never saw the reply.
            self.assertEqual(len(self.c.src_info("A")["pins"]), 1)
            # gc above the pin floor is refused.
            self.assertEqual(self.c.gc("A", 1)["error"], "$.k")
            coord2 = Coordinator(coord_db, common.Http())
            coord2.recover()
            out2 = self.c.call("/query", {"job": "j"})
            self.assertEqual(out2["state"], "ready")
            self.assertEqual(self.c.src_info("A")["pins"], [])

    def test_worker_retries_after_source_outage(self):
        """A job started while a source is down completes once it returns."""
        coord_db = os.path.join(self.c.workdir, "coordinator.sqlite3")
        http = common.Http(timeout=0.5)
        coord = Coordinator(coord_db, http)
        server = self.c.source_servers["B"]
        port = self.c.source_urls["B"].rsplit(":", 1)[1]
        coord.sources["A"] = self.c.source_urls["A"]
        coord.sources["B"] = "http://127.0.0.1:%d" % (int(port) + 1)  # dead
        out = coord.start("j", {"A": 1, "B": 1})
        self.assertEqual(out["state"], "pending")
        # Bring the real sources back and recover.
        coord.sources["B"] = self.c.source_urls["B"]
        coord.recover()
        out2 = self.c.call("/query", {"job": "j"})
        self.assertEqual(out2["state"], "ready")


if __name__ == "__main__":
    unittest.main()
