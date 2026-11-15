"""Initialisation-document validation and init-time edge cases."""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from expcoord.common import InputError  # noqa: E402
from expcoord.validate import validate_init  # noqa: E402


def v0():
    return {"rows": [], "needs": {}}


def ver(rows, needs=None):
    return {"rows": list(rows), "needs": dict(needs or {})}


class TestInitValidation(unittest.TestCase):
    def assertPath(self, doc, path):
        with self.assertRaises(InputError) as ctx:
            validate_init(doc)
        self.assertEqual(ctx.exception.path, path)

    def test_valid_document(self):
        validate_init({"A": [v0(), ver(["a"], {"B": 1})], "B": [v0(), ver(["b"])]})

    def test_top_level(self):
        self.assertPath([], "$")
        self.assertPath({}, "$")
        self.assertPath("x", "$")

    def test_source_name(self):
        self.assertPath({"A-B": [v0()]}, "$")
        self.assertPath({"": [v0()]}, "$")
        self.assertPath({"é": [v0()]}, "$")

    def test_version_array_non_empty(self):
        self.assertPath({"A": []}, "$.A")
        self.assertPath({"A": {}}, "$.A")

    def test_version_shape(self):
        self.assertPath({"A": []}, "$.A")
        self.assertPath({"A": [{"rows": []}]}, "$.A[0]")
        self.assertPath({"A": [{"rows": [], "needs": {}, "x": 1}]}, "$.A[0]")
        self.assertPath({"A": [{"rows": [1], "needs": {}}]}, "$.A[0].rows")
        self.assertPath({"A": [{"rows": [], "needs": []}]}, "$.A[0].needs")

    def test_version_zero_is_empty(self):
        self.assertPath({"A": [{"rows": ["x"], "needs": {}}]}, "$.A[0]")
        self.assertPath({"A": [{"rows": [], "needs": {"B": 0}}], "B": [v0()]}, "$.A[0]")

    def test_dependency_must_exist(self):
        self.assertPath({"A": [v0(), ver(["a"], {"Z": 1})]}, "$.A[1].needs.Z")

    def test_dependency_in_range(self):
        doc = {"A": [v0(), ver(["a"], {"B": 5})], "B": [v0()]}
        self.assertPath(doc, "$.A[1].needs.B")

    def test_self_requirement_not_above_own_version(self):
        self.assertPath({"A": [v0(), ver(["a"], {"A": 2})]}, "$.A[1].needs.A")
        # Equal to the version is fine.
        validate_init({"A": [v0(), ver(["a"], {"A": 1})]})

    def test_needs_are_integers_not_bools(self):
        doc = {"A": [v0(), ver(["a"], {"B": True})], "B": [v0()]}
        self.assertPath(doc, "$.A[1].needs.B")
        doc = {"A": [v0(), ver(["a"], {"B": 1.0})], "B": [v0(), v0()]}
        self.assertPath(doc, "$.A[1].needs.B")

    def test_needs_non_decreasing(self):
        doc = {
            "A": [v0(), ver(["a"], {"B": 1}), ver(["b"], {"B": 0})],
            "B": [v0(), v0()],
        }
        self.assertPath(doc, "$.A[2].needs.B")

    def test_dropping_a_dependency_is_decreasing(self):
        doc = {
            "A": [v0(), ver(["a"], {"B": 1}), ver(["b"], {})],
            "B": [v0(), v0()],
        }
        self.assertPath(doc, "$.A[2].needs.B")

    def test_omitted_means_zero(self):
        # A1 needs nothing; A2 needs B1: still non-decreasing.
        validate_init({"A": [v0(), ver(["a"]), ver(["b"], {"B": 1})],
                       "B": [v0(), v0()]})


if __name__ == "__main__":
    unittest.main()
