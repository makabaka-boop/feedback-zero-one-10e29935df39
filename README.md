# expcoord — cross-source expansion coordinator

A from-scratch implementation, using only the Python standard library and
SQLite, of a two-tier export system:

* **one HTTP process per source**, each with its own SQLite database;
* **one export-coordinator process** with its own SQLite database;
* the coordinator talks to the sources over HTTP, and all durability claims
  survive process death at any point (verified with real `SIGKILL`s).

```text
            start(job, caps) / query(job)
 client ──────────────────────────────────► coordinator (SQLite: jobs,
                                              plans)
                                                │  HTTP, per source
                            ┌───────────────────┼───────────────────┐
                            ▼                   ▼                   ▼
                      source A (SQLite)   source B (SQLite)   source C (SQLite)
                        gc / pin /          ...                 ...
                        snapshot / commit
```

## Running

```bash
python3 -m expcoord cluster --init init.json --workdir ./run
# {"coordinator": "http://127.0.0.1:41237", "sources": {"A": "...", "B": "..."}}

curl -s -XPOST http://127.0.0.1:41237/start \
     -d '{"job":"j1","caps":{"A":1,"B":0}}'
# {"state":"ready","versions":{"A":0,"B":0},"rows":{"A":[],"B":[]}}
```

Components can also be run separately (one OS process each):

```bash
python3 -m expcoord source      --db run/source_A.sqlite3 --name A --port 7001
python3 -m expcoord coordinator --db run/coordinator.sqlite3 \
        --sources sources.json --port 7000     # {"A": "http://127.0.0.1:7001", ...}
```

The coordinator and sources are also usable as a library —
`expcoord.source.run_source`, `expcoord.coordinator.run_coordinator` — which
is what the tests and `tests/crash_child.py` do.

HTTP surface:

| service     | endpoint          | body                                  | reply |
|-------------|-------------------|---------------------------------------|-------|
| coordinator | `/start`          | `{"job":j,"caps":{src:v,...}}`        | `{"state":...}` |
| coordinator | `/query`          | `{"job":j}`                           | `{"state":...}` |
| source      | `/`               | `{"op":"gc","k":k}`                   | `{"floor":k}` |

`start` also accepts `{"op":"start",...}`, `query` accepts
`{"op":"query",...}` on `/`.  Every failure is `{"error": <jsonpath>}` with
the path of the offending value (`$` for malformed JSON), and a rejected
request changes no state.

Internal coordinator↔source endpoints (`/int/pin`, `/int/snapshot`,
`/int/commit`, `/int/release`, `/int/info`) are not part of the public API.

## Initialisation document

```json
{"A": [{"rows": [], "needs": {}},
       {"rows": ["a1"], "needs": {"B": 1}}],
 "B": [{"rows": [], "needs": {}},
       {"rows": ["b1"], "needs": {}}]}
```

The array index **is** the version.  Rules enforced by
`expcoord.validate.validate_init` (each with its JSONPath):

* keys and job names are non-empty ASCII alphanumerics (`$.A-B`);
* each version is an object with exactly `rows` and `needs` (`$.A[1]`);
* `rows` is a list of strings, order and duplicates preserved (`$.A[1].rows`);
* version 0 is `rows=[]`, `needs={}` (`$.A[0]`);
* `needs` maps an existing source to an integer ≥ 0 (`$.A[1].needs.Z`,
  `$.A[1].needs.B`); booleans and floats are rejected;
* a self-requirement may not exceed its own version (`$.A[1].needs.A`);
* requirements are non-decreasing with the version, and dropping one counts
  as returning to 0 (`$.A[2].needs.B`).

Versions never grow after initialisation.  `gc` is initialised at floor 0.

## Semantics

### `gc(k)` — source-local collection

Deletes versions below `k`, returns `{"floor": k}`.  `k` must be an integer
(not a boolean) in `[current floor, last version]`, else `{"error":"$.k"}`.
While any job has fixed the source, `gc` above that job's floor is refused —
this is what keeps a job's window alive.  `k == floor` is the idempotent
no-op that makes a lost gc response harmless.

### `start(job, caps)` — fix, solve, publish

1. **Validate.**  `caps` must map *exactly* every source to a legal version.
2. **Register.**  The job is durably stored as `pending` before any network
   call.  Repeating a job with the same caps replays its state; different
   caps is rejected (`$.caps`) without touching the job.
3. **Fix.**  Every source is asked to pin, then snapshotted.  A pin freezes
   the source's window `[floor, caps]`, so the read is atomic with respect
   to `gc`: a concurrent `gc` is either ordered before the pin (and the job
   starts at the new floor) or waits for the release (the job is already
   published).  A cap that was already collected makes the job `failed`.
4. **Solve.**  Seeds `v = caps` and repeatedly lowers each component to the
   highest version in `[floor, caps]` whose `needs` fit under the current
   tuple.  Requirements are non-decreasing, so this monotone iteration
   reaches the greatest fixed point: **the componentwise maximum of all
   feasible tuples** (`tests/test_solver.py` checks it against brute force on
   400 random instances).  No feasible tuple ⇒ `failed`, even with cycles.
5. **Publish.**  The single `ready` row is written atomically; only then is
   protection released.  A result can therefore never be lost after it is
   visible.

`start` and `query` return `{"state": "pending"|"ready"|"failed"}`; `ready`
adds `versions` and `rows`, each keyed by source name.  `rows` keeps the
source's order and duplicates.  Terminal states are durable and never
reversed.

### Crash and retry behaviour

* **Pins and receipts live in the source DBs.**  A job that has released at
  a source can never pin there again; a retried commit/release replays its
  receipt, so a lost response is harmless.
* **The planned selection is durable before any commit** (`plans` table), so
  a coordinator that dies mid-publish restarts and commits the same tuple
  instead of re-solving against a changed world.
* **A background worker** resumes `pending` jobs and finishes the release of
  terminal jobs whose pins outlived publication (two-phase: durable result,
  then release).  Untrusted/duplicate requests, dropped replies and source
  restarts are all covered by the suite.
* Readers never see a partial result because `query` reads exactly one row.

## Acceptance

```bash
python3 -m unittest discover -s tests -v      # in-process, incl. SQLite restarts
python3 tests/test_multiprocess.py -v         # real processes, real SIGKILL
```

65 tests (58 in-process + 7 with real processes).  The scenarios the brief calls out:

| requirement | where |
|---|---|
| example `A1` needs `B1`, `caps={A:1,B:0}` ⇒ ready `{A:0,B:0}` | `TestExample.test_a1_requires_b1_but_cap_b0` |
| componentwise maximum, non-decreasing needs | `TestExample.test_componentwise_maximum`, `test_solver.py` |
| cyclic dependencies (realisable and capped) | `TestDependencies.test_mutual_dependency_capable`, `test_cycle_where_one_side_is_capped` |
| `gc` bounds, idempotence, durability across restart | `TestGc.test_gc_bounds_durable`, `MultiProcess.test_source_restart_keeps_floor_and_pins` |
| gc/snapshot race rejected | `TestGc.test_gc_pinned_window_rejected`, `MultiProcess.test_sigkill_after_pins_before_publish` |
| cap already collected ⇒ failed | `TestGc.test_cap_below_floor_fails`, `TestValidation.test_failed_job_is_terminal` |
| crash after pinning / after publish / before release | `TestCrash.*`, `MultiProcess.test_sigkill_*` |
| hard kill after publish, before release | `MultiProcess.test_sigkill_after_publish_before_release` |
| dropped responses, source outage, retry | `TestCrash.test_lost_response_on_*`, `test_worker_retries_after_source_outage` |
| same job same caps replay / different caps rejected | `TestValidation.test_same_caps_replay_and_conflict` |
| permanent ban on re-pinning after release | asserted in `TestCrash.*` (a released source refuses) |
| invalid input, unknown job, JSON syntax (`$`), no state change | `TestValidation` |
| interleaved jobs + gc, no partial publish | `test_concurrency.py` |

## Files

```
expcoord/common.py        validation primitives, SQLite Tx, HTTP client/server
expcoord/validate.py      initialisation-document validation
expcoord/source.py        source service (gc, pin, snapshot, commit, release)
expcoord/coordinator.py   coordinator service (start, query, solve, publish, recovery)
expcoord/__main__.py      CLI: cluster / source / coordinator
tests/                    acceptance, validation, solver, concurrency, multi-process
```

Only `sqlite3`, `http.server`, `urllib`, `json`, `threading`, `argparse` and
friends from the standard library are used; there are no third-party
dependencies.
