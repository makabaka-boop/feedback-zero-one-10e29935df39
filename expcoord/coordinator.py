"""Coordinator service: validates jobs, fixes per-source version windows,
resolves the componentwise-maximal selection and publishes it durably.

Wire API (JSON object POST):
    /            {"op":"start","job":j,"caps":{s:v}}  or {"op":"query","job":j}
    /start       {"job":j,"caps":{s:v}}   -> {"state": "pending"|"ready"|"failed", ...}
    /query       {"job":j}                -> {"state": ...}
    /info        {}                       -> debug view

A ready reply is ``{"state":"ready","versions":{s:v},"rows":{s:[...]}}``.

Durability protocol
-------------------
1. ``start`` validates the payload, then durably records the job as
   ``pending``.  Same job + same caps replays; different caps is rejected.
2. For every source the coordinator obtains a *pin* (its current floor) and
   a snapshot.  The pin blocks ``gc`` above the floor, so the window
   ``[floor, caps]`` is frozen for the duration.
3. The selected tuple is written to a durable ``plans`` row *before* any
   source is committed.  ``commit`` at a source is the release and is
   idempotent per job, so a coordinator crash anywhere after the plan is
   recoverable: the same selection is replayed and finished.
4. Only when every source has acknowledged the commit is the job's
   ``ready`` result written.  Until then the job reads ``pending``.
5. Recovery (on start and from the background worker) replays pending jobs
   from their plan or re-resolves them, and finishes release for terminal
   jobs whose pins outlived their publication.
"""

import json
import os
import threading
import time

from .common import (
    HttpError,
    InputError,
    SourceProtocolError,
    SourceUnreachable,
    Tx,
    is_name,
    is_version,
    make_handler,
    open_db,
    serve_in_thread,
)
from .validate import validate_init

SCHEMA = """
CREATE TABLE IF NOT EXISTS init_sources (
    name      TEXT PRIMARY KEY,
    idx       INTEGER NOT NULL,
    nversions INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
    name TEXT PRIMARY KEY,
    url  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    job         TEXT PRIMARY KEY,
    caps_json   TEXT NOT NULL,
    state       TEXT NOT NULL,        -- pending | ready | failed
    result_json TEXT,
    cleaned     INTEGER NOT NULL DEFAULT 0,  -- release+cleanup finished
    created     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS plans (
    job       TEXT PRIMARY KEY,
    plan_json TEXT NOT NULL
);
"""


def init_coordinator_db(path, init_doc, source_urls):
    """Create a fresh coordinator database.

    ``source_urls`` must cover exactly the names of ``init_doc``.
    """
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.unlink(path + suffix)
    validate_init(init_doc)
    if set(source_urls) != set(init_doc):
        raise ValueError("source URLs must cover exactly the initialised sources")
    conn = open_db(path)
    conn.executescript(SCHEMA)
    with Tx(conn, threading.RLock()) as c:
        for idx, (name, versions) in enumerate(init_doc.items()):
            c.execute(
                "INSERT INTO init_sources(name,idx,nversions) VALUES(?,?,?)",
                (name, idx, len(versions)),
            )
            c.execute(
                "INSERT INTO sources(name,url) VALUES(?,?)", (name, source_urls[name])
            )
    conn.close()


class _JobFailed(Exception):
    """The job has no feasible selection, or a cap was already collected."""


class Coordinator:
    def __init__(self, db_path, http_client, host="127.0.0.1", port=0):
        self.db_path = db_path
        self.conn = open_db(db_path)
        self.lock = threading.RLock()
        self.tx = Tx(self.conn, self.lock)
        self.http = http_client
        self.host = host
        self.port = port
        self._job_locks = {}
        self._job_locks_guard = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker = None
        self.sources = self._load_sources()
        self.source_names = list(self.sources)

    # -- metadata --------------------------------------------------------
    def _load_sources(self):
        rows = self.conn.execute(
            "SELECT s.name AS name, s.url AS url FROM sources s "
            "JOIN init_sources i ON i.name = s.name ORDER BY i.idx"
        ).fetchall()
        return {r["name"]: r["url"] for r in rows}

    def _nsources(self):
        return {
            r["name"]: r["nversions"]
            for r in self.conn.execute("SELECT name, nversions FROM init_sources")
        }

    def _job_lock(self, job):
        with self._job_locks_guard:
            lock = self._job_locks.get(job)
            if lock is None:
                lock = threading.RLock()
                self._job_locks[job] = lock
            return lock

    def _row(self, job):
        return self.conn.execute("SELECT * FROM jobs WHERE job=?", (job,)).fetchone()

    # -- public API ------------------------------------------------------
    def start(self, job, caps):
        caps = self._validate_start(job, caps)
        canonical = json.dumps(caps, sort_keys=True, separators=(",", ":"))
        with self.tx as c:
            row = c.execute("SELECT * FROM jobs WHERE job=?", (job,)).fetchone()
            if row is None:
                c.execute(
                    "INSERT INTO jobs(job,caps_json,state,result_json,created) "
                    "VALUES(?,?, 'pending', NULL, ?)",
                    (job, canonical, time.time()),
                )
            elif row["caps_json"] != canonical:
                raise InputError("$.caps")
        try:
            self.drive(job)
        except _JobFailed:
            self._fail(job)
        except SourceUnreachable:
            # Still durably pending; the worker retries shortly.
            self._wake.set()
        except (HttpError, SourceProtocolError):
            self._fail(job)
        return self.query(job)

    def query(self, job):
        if not is_name(job):
            raise InputError("$.job")
        row = self._row(job)
        if row is None:
            raise InputError("$.job")
        return self._public(row)

    @staticmethod
    def _public(row):
        if row["state"] == "ready":
            return json.loads(row["result_json"])
        return {"state": row["state"]}

    def _validate_start(self, job, caps):
        if not is_name(job):
            raise InputError("$.job")
        if not isinstance(caps, dict):
            raise InputError("$.caps")
        sizes = self._nsources()
        for name in caps:
            if not is_name(name) or name not in sizes:
                raise InputError("$.caps")
        for name in sizes:
            if name not in caps:
                raise InputError("$.caps")
        for name, v in caps.items():
            if not is_version(v) or v < 0 or v >= sizes[name]:
                raise InputError("$.caps." + name)
        # Keep source order for stable output.
        return {name: caps[name] for name in self.source_names}

    # -- driving a job ---------------------------------------------------
    def drive(self, job):
        with self._job_lock(job):
            row = self._row(job)
            if row is None:
                return
            if row["state"] != "pending":
                self._cleanup_terminal(row)
                return
            plan = self._load_plan(job)
            if plan is None:
                caps = json.loads(row["caps_json"])
                plan = self._resolve(job, caps)
                self._save_plan(job, plan)
            self._publish_and_release(job, plan)

    def _load_plan(self, job):
        row = self.conn.execute("SELECT plan_json FROM plans WHERE job=?", (job,)).fetchone()
        return None if row is None else json.loads(row["plan_json"])

    def _save_plan(self, job, plan):
        with self.tx as c:
            c.execute(
                "INSERT OR REPLACE INTO plans(job,plan_json) VALUES(?,?)",
                (job, json.dumps(plan, separators=(",", ":"))),
            )

    def _post(self, name, path, payload):
        url = self.sources[name] + path
        try:
            return self.http.post(url, payload)
        except HttpError as exc:
            body = exc.payload if isinstance(exc.payload, dict) else {}
            reason = body.get("reason")
            if reason in ("collected", "released", "unpinned", "mismatch"):
                raise _JobFailed(reason) from exc
            raise

    def _resolve(self, job, caps):
        """Pin, snapshot and solve; returns the plan (selection + rows).

        Pins are taken for every source before any snapshot is read.  A pin
        froze the source's window at its current floor, and since no other
        job's ``gc`` can pass that floor, the snapshot below always contains
        every version in ``[floor, caps]``.
        """
        floors = {}
        snapshots = {}
        for name in self.source_names:
            resp = self._post(name, "/int/pin", {"job": job, "caps": caps[name]})
            floor = resp.get("floor")
            if not is_version(floor) or floor < 0 or floor > caps[name]:
                raise _JobFailed("floor")
            floors[name] = floor
        for name in self.source_names:
            snap = self._post(name, "/int/snapshot", {"job": job, "caps": caps[name]})
            if snap.get("floor") != floors[name]:
                raise _JobFailed("floor")
            snapshots[name] = snap["versions"]

        chosen = self._solve(caps, floors, snapshots)
        if chosen is None:
            raise _JobFailed("infeasible")
        rows = {}
        for name in self.source_names:
            entry = snapshots[name].get(str(chosen[name]))
            if entry is None:
                raise _JobFailed("collected")
            rows[name] = entry["rows"]
        checkpoint = getattr(self, "checkpoint", None)
        if checkpoint is not None:
            # Test seam: lets an external harness kill the process at an
            # exact protocol point.  Never set in production.
            checkpoint("resolved")
        return {"versions": chosen, "rows": rows}

    def _solve(self, caps, floors, snapshots):
        """Componentwise-maximal feasible selection, or None.

        ``needs`` never decreases as versions rise, so the set of versions of
        one source whose requirements fit under a given tuple is a prefix
        ``[floor, x]``.  Lowering each component to its largest admissible
        value and repeating is monotone; it converges to the greatest fixed
        point, which is the componentwise maximum over all feasible tuples.
        """
        values = dict(caps)
        while True:
            changed = False
            for name in self.source_names:
                needs_by_v = snapshots[name]
                best = values[name]
                while best >= floors[name]:
                    entry = needs_by_v.get(str(best))
                    if entry is None:
                        # Missing from the snapshot: cannot be chosen.
                        best -= 1
                        continue
                    needs = entry["needs"]
                    if all(needs.get(dep, 0) <= values[dep] for dep in needs):
                        break
                    best -= 1
                if best < floors[name]:
                    return None
                if best != values[name]:
                    values[name] = best
                    changed = True
            if not changed:
                return values

    def _publish_and_release(self, job, plan):
        """Make the terminal result durable first, then release protection.

        The selection cannot be invalidated while the pins exist (``gc``
        cannot pass a pin's floor), so it is safe to persist ``ready`` before
        asking the sources to commit.  Committing at a source re-checks the
        choice against the pinned window and drops the pin; every call is
        idempotent per job, so recovery can replay it after a crash.  The
        result is one atomic row, so readers never observe a partial publish.
        """
        result = {
            "state": "ready",
            "versions": plan["versions"],
            "rows": plan["rows"],
        }
        with self.tx as c:
            changed = c.execute(
                "UPDATE jobs SET state='ready', result_json=? "
                "WHERE job=? AND state='pending'",
                (json.dumps(result, separators=(",", ":")), job),
            ).rowcount
            if changed != 1:
                return
            c.execute("DELETE FROM plans WHERE job=?", (job,))
        # Durable on disk now; only afterwards may protection be dropped.
        if self._release_all(job, plan["versions"]):
            self._mark_clean(job)

    def _fail(self, job):
        with self.tx as c:
            c.execute(
                "UPDATE jobs SET state='failed' WHERE job=? AND state='pending'", (job,)
            )
            c.execute("DELETE FROM plans WHERE job=?", (job,))
        if self._release_all(job):
            self._mark_clean(job)

    def _mark_clean(self, job):
        with self.tx as c:
            c.execute("UPDATE jobs SET cleaned=1 WHERE job=?", (job,))

    def _release_all(self, job, versions=None):
        """Release ``job``'s pins everywhere; commit the selection if known.

        ``commit`` is the release for a job that selected a version, and the
        plain ``release`` covers pins left by an attempt that selected
        nothing.  Both are idempotent, and the job is permanently barred from
        pinning again at that source afterwards.  Returns True when every
        source acknowledged; otherwise the worker retries.
        """
        ok = True
        for name in self.source_names:
            checkpoint = getattr(self, "checkpoint", None)
            if checkpoint is not None:
                # Test seam: fires before this source is touched.
                checkpoint("release", name)
            if versions is not None:
                try:
                    self.http.post(
                        self.sources[name] + "/int/commit",
                        {"job": job, "v": versions[name]},
                    )
                except (SourceUnreachable, SourceProtocolError):
                    ok = False
                except HttpError:
                    # Explicit rejection: the source is reachable and has
                    # decided.  Retrying would not change the answer.
                    pass
            try:
                self.http.post(self.sources[name] + "/int/release", {"job": job})
            except (SourceUnreachable, SourceProtocolError):
                ok = False
            except HttpError:
                pass
        return ok

    def _cleanup_terminal(self, row):
        """Finish a terminal job: release any pin that outlived publishing."""
        if row["cleaned"]:
            return
        job = row["job"]
        versions = None
        if row["state"] == "ready":
            try:
                versions = json.loads(row["result_json"])["versions"]
            except (TypeError, KeyError, ValueError):
                versions = None
        if self._release_all(job, versions):
            self._mark_clean(job)

    # -- recovery --------------------------------------------------------
    def recover(self):
        """Resume every pending job and finish cleanup of terminal ones."""
        rows = self.conn.execute("SELECT * FROM jobs ORDER BY created").fetchall()
        for row in rows:
            try:
                self.drive(row["job"])
            except _JobFailed:
                self._fail(row["job"])
            except SourceUnreachable:
                continue
            except (HttpError, SourceProtocolError):
                self._fail(row["job"])

    def start_worker(self, interval=0.5):
        if self._worker is not None:
            return

        def loop():
            while not self._stop.is_set():
                self._wake.wait(interval)
                self._wake.clear()
                if self._stop.is_set():
                    break
                try:
                    self.recover()
                except Exception:
                    pass

        self._worker = threading.Thread(target=loop, name="coord-worker", daemon=True)
        self._worker.start()

    def stop_worker(self):
        self._stop.set()
        self._wake.set()

    def info(self):
        with self.lock:
            jobs = {}
            for row in self.conn.execute("SELECT * FROM jobs ORDER BY created"):
                jobs[row["job"]] = {
                    "state": row["state"],
                    "caps": json.loads(row["caps_json"]),
                    "cleaned": bool(row["cleaned"]),
                }
            plans = [
                r["job"] for r in self.conn.execute("SELECT job FROM plans ORDER BY job")
            ]
            return {"jobs": jobs, "plans": plans, "sources": self.source_names}


def build_routes(coordinator):
    def start(payload):
        if "job" not in payload:
            raise InputError("$.job")
        if "caps" not in payload:
            raise InputError("$.caps")
        return coordinator.start(payload["job"], payload["caps"])

    def query(payload):
        if "job" not in payload:
            raise InputError("$.job")
        return coordinator.query(payload["job"])

    def root(payload):
        op = payload.get("op")
        if op == "start":
            return start(payload)
        if op == "query":
            return query(payload)
        raise InputError("$.op")

    return {
        "/": root,
        "/start": start,
        "/query": query,
        "/info": lambda p: coordinator.info(),
    }


def run_coordinator(db_path, http_client, host="127.0.0.1", port=0):
    coordinator = Coordinator(db_path, http_client, host, port)
    server, thread = serve_in_thread(
        make_handler(build_routes(coordinator)), host, port
    )
    return coordinator, server, server.server_address[1]
