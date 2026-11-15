"""Source-level contract, exercised through the internal HTTP endpoints."""

import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

from harness import Cluster, v0, ver  # noqa: E402


class TestSourceContract(unittest.TestCase):
    def setUp(self):
        self.c = Cluster({
            "A": [v0(), ver(["a1"], {}), ver(["a2"], {})],
        })

    def tearDown(self):
        self.c.close()

    def call(self, path, payload):
        status, body = self.c.raw_post(
            self.c.source_urls["A"] + path, json.dumps(payload).encode()
        )
        self.assertEqual(status, 200, body)
        return body

    def test_pin_is_idempotent_and_replays_floor(self):
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 2}),
                         {"floor": 0})
        # A repeated pin with the same caps returns the same floor.
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 2}),
                         {"floor": 0})
        # A different caps for the same job is rejected.
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 1})["error"],
                         "$.job")

    def test_pin_refused_for_collected_cap(self):
        self.c.gc("A", 2)
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 1})["error"],
                         "$.caps")

    def test_release_is_permanent(self):
        self.call("/int/pin", {"job": "j", "caps": 2})
        self.assertEqual(self.call("/int/commit", {"job": "j", "v": 2}),
                         {"ok": True, "v": 2, "floor": 0})
        # Re-pinning after release is barred for good.
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 2})["error"],
                         "$.job")
        # Replaying the commit is harmless and returns the same receipt.
        self.assertEqual(self.call("/int/commit", {"job": "j", "v": 2}),
                         {"ok": True, "v": 2, "floor": 0})
        # A different version for the same receipt is refused.
        self.assertEqual(self.call("/int/commit", {"job": "j", "v": 1})["error"],
                         "$.job")

    def test_commit_requires_a_pin(self):
        self.assertEqual(self.call("/int/commit", {"job": "nope", "v": 1})["error"],
                         "$.job")

    def test_commit_outside_pinned_window(self):
        self.call("/int/pin", {"job": "j", "caps": 1})
        self.assertEqual(self.call("/int/commit", {"job": "j", "v": 2})["error"],
                         "$.v")
        # gc cannot pass a live pin, so the window is still there.
        self.assertEqual(self.call("/int/commit", {"job": "j", "v": 1})["ok"], True)

    def test_plain_release_records_receipt(self):
        self.call("/int/pin", {"job": "j", "caps": 2})
        self.assertEqual(self.call("/int/release", {"job": "j"}),
                         {"ok": True, "v": None})
        self.assertEqual(self.call("/int/pin", {"job": "j", "caps": 2})["error"],
                         "$.job")

    def test_snapshot_contents_and_floor(self):
        self.c.gc("A", 1)
        snap = self.call("/int/snapshot", {"job": "j", "caps": 2})
        self.assertEqual(snap["floor"], 1)
        self.assertEqual(sorted(snap["versions"]), ["1", "2"])
        self.assertEqual(snap["versions"]["1"]["rows"], ["a1"])
        self.assertEqual(snap["versions"]["2"]["rows"], ["a2"])
        self.assertEqual(snap["versions"]["2"]["needs"], {})

    def test_gc_rejected_above_any_pin_floor(self):
        self.call("/int/pin", {"job": "j", "caps": 2})
        self.assertEqual(self.c.gc("A", 1)["error"], "$.k")
        self.assertEqual(self.c.gc("A", 2)["error"], "$.k")
        self.call("/int/release", {"job": "j"})
        self.assertEqual(self.c.gc("A", 2), {"floor": 2})

    def test_gc_validation(self):
        for bad in [True, False, 1.0, "1", None, [], {}]:
            self.assertEqual(self.c.gc("A", bad)["error"], "$.k", bad)


if __name__ == "__main__":
    unittest.main()
