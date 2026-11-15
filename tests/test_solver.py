"""The solver's unit contract: componentwise maximum, checked by brute force.

For small random instances (needs non-decreasing, as the init validator
guarantees) enumerate every feasible tuple and compare with ``_solve``.
"""

import itertools
import os
import random
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from expcoord.coordinator import Coordinator  # noqa: E402


class Solver:
    """Minimal stand-in so _solve can be called without a database."""

    def __init__(self, names):
        self.source_names = names

    _solve = Coordinator._solve


def random_instance(rng):
    n = rng.randint(1, 4)
    names = ["S%d" % i for i in range(n)]
    counts = {name: rng.randint(1, 4) for name in names}  # versions 0..count-1
    needs = {name: [] for name in names}
    for name in names:
        prev = {}
        for v in range(counts[name]):
            if v == 0:
                needs[name].append({})
                continue
            current = {}
            for dep in names:
                # Non-decreasing requirement, never above the version itself
                # for self references.
                limit = min(v, counts[dep] - 1)
                if limit < 0:
                    continue
                base = prev.get(dep, 0)
                if rng.random() < 0.5 and limit >= base:
                    current[dep] = rng.randint(base, limit)
                elif base > 0:
                    current[dep] = base
            needs[name].append(current)
            prev = current
    return names, counts, needs


class TestSolver(unittest.TestCase):
    def test_against_bruteforce(self):
        rng = random.Random(20240101)
        for _ in range(400):
            names, counts, needs = random_instance(rng)
            caps = {n: rng.randrange(counts[n]) for n in names}
            floors = {n: rng.randrange(caps[n] + 1) for n in names}
            snapshots = {
                n: {
                    str(v): {"rows": [], "needs": needs[n][v]}
                    for v in range(floors[n], counts[n])
                }
                for n in names
            }
            got = Solver(names)._solve(caps, floors, snapshots)

            feasible = []
            ranges = [range(floors[n], caps[n] + 1) for n in names]
            for combo in itertools.product(*ranges):
                candidate = dict(zip(names, combo))
                if all(
                    needs[n][candidate[n]].get(d, 0) <= candidate[d]
                    for n in names
                    for d in needs[n][candidate[n]]
                ):
                    feasible.append(candidate)
            if not feasible:
                self.assertIsNone(got, (caps, floors, needs))
                continue
            best = {
                n: max(candidate[n] for candidate in feasible) for n in names
            }
            # The componentwise maximum must itself be feasible, which holds
            # because feasibility is downward closed in every component.
            self.assertIn(best, feasible, (caps, floors, needs))
            self.assertEqual(got, best, (caps, floors, needs))

    def test_trivial_cases(self):
        s = Solver(["A", "B"])
        snaps = {"A": {"0": {"needs": {}}}, "B": {"0": {"needs": {}}}}
        self.assertEqual(s._solve({"A": 0, "B": 0}, {"A": 0, "B": 0}, snaps),
                         {"A": 0, "B": 0})

    def test_infeasible_when_floor_requires_too_much(self):
        s = Solver(["A", "B"])
        # A1 needs B1 but B's floor is 1 and cap 0 is impossible, so use
        # A floor 1, B cap 0 -> infeasible.
        snaps = {
            "A": {"1": {"needs": {"B": 1}}},
            "B": {"0": {"needs": {}}},
        }
        self.assertIsNone(s._solve({"A": 1, "B": 0}, {"A": 1, "B": 0}, snaps))


if __name__ == "__main__":
    unittest.main()
