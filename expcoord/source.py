"""Source service: one process, one SQLite database, one source's versions.

Wire API (JSON object POST):
    POST /                {"op":"gc","k":<int>}   -> {"floor": k}
    POST /int/pin         {"job":j,"caps":c}      -> {"floor": f}
    POST /int/snapshot    {"job":j,"caps":c}      -> {"floor":f,"versions":{...}}
    POST /int/commit      {"job":j,"v":v}         -> {"ok":true,"v":v,"floor":f}
    POST /int/release     {"job":j}               -> {"ok":true,"v":null}
    POST /int/info        {}                      -> debug view

Locking protocol (the heart of the cross-source guarantee)
---------------------------------------------------------
* ``pin(job, caps)`` durably records this job's floor at this source.  The
  floor is the lowest version the job may still choose.
* While a pin exists at floor ``f``, ``gc(k)`` with ``k > f`` is refused.
  Versions ``>= f`` therefore cannot be collected while the job is fixing its
  window, which makes "read floor + read versions" atomic against ``gc``:
  any ``gc`` either happens before the pin (the job starts at the new floor)
  or waits for the release (the job is already published).
* ``commit`` is the release.  It re-checks the choice against the pin and the
  current floor, drops the pin, and writes a permanent release receipt for
  the job.  A job that has released here can never pin here again; a
  retried commit replays the receipt, so lost responses are harmless.
* ``release`` abandons a job's pin without a selection (failed jobs) and
  leaves the same permanent receipt.

Everything is transactionally durable before it is acknowledged, so a crash
after any step is recoverable: pins and receipts live in the source DB, the
job's terminal result lives in the coordinator DB.
"""

import json
import os
import threading
import time

from .common import (
    InputError,
    Tx,
    is_name,
    is_version,
    make_handler,
    open_db,
    serve_in_thread,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    v          INTEGER PRIMARY KEY,
    rows_json  TEXT NOT NULL,
    needs_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pins (
    job    TEXT PRIMARY KEY,
    floor  INTEGER NOT NULL,
    caps   INTEGER NOT NULL,
    at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS released (
    job    TEXT PRIMARY KEY,
    v      INTEGER,          -- NULL when the job failed before selecting here
    floor  INTEGER,
    at     REAL NOT NULL
);
"""


def init_source_db(path, name, versions, floor=0):
    """Create a fresh source database."""
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.unlink(path + suffix)
    conn = open_db(path)
    conn.executescript(SCHEMA)
    tx = Tx(conn, threading.RLock())
    with tx as c:
        c.execute("INSERT INTO meta(key,value) VALUES('name',?)", (name,))
        c.execute("INSERT INTO meta(key,value) VALUES('floor',?)", (str(floor),))
        for idx, ver in enumerate(versions):
            c.execute(
                "INSERT INTO versions(v,rows_json,needs_json) VALUES(?,?,?)",
                (
                    idx,
                    json.dumps(ver["rows"], separators=(",", ":")),
                    json.dumps(ver["needs"], separators=(",", ":")),
                ),
            )
    conn.close()


class Source:
    def __init__(self, name, db_path):
        self.name = name
        self.db_path = db_path
        self.conn = open_db(db_path)
        self.lock = threading.RLock()
        self.tx = Tx(self.conn, self.lock)

    # -- state -----------------------------------------------------------
    def _meta(self, c, key):
        return c.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()["value"]

    def floor(self, c=None):
        return int(self._meta(c or self.conn, "floor"))

    def last(self, c=None):
        row = (c or self.conn).execute("SELECT MAX(v) AS m FROM versions").fetchone()
        return 0 if row["m"] is None else int(row["m"])

    def _pin(self, c, job):
        return c.execute("SELECT * FROM pins WHERE job=?", (job,)).fetchone()

    def _receipt(self, c, job):
        return c.execute("SELECT * FROM released WHERE job=?", (job,)).fetchone()

    # -- collection ------------------------------------------------------
    def gc(self, k):
        if not is_version(k):
            raise InputError("$.k")
        with self.tx as c:
            floor, last = self.floor(c), self.last(c)
            if k < floor or k > last:
                raise InputError("$.k")
            pinned = c.execute("SELECT MIN(floor) AS f FROM pins").fetchone()["f"]
            if pinned is not None and k > pinned:
                # A job fixed this floor; its window must survive.  The
                # public error is exactly the JSONPath, as the spec requires.
                raise InputError("$.k")
            c.execute("DELETE FROM versions WHERE v < ?", (k,))
            c.execute("UPDATE meta SET value=? WHERE key='floor'", (str(k),))
            return {"floor": k}

    # -- job fixing ------------------------------------------------------
    def pin(self, job, caps):
        if not is_name(job):
            raise InputError("$.job")
        if not is_version(caps):
            raise InputError("$.caps")
        with self.tx as c:
            if self._receipt(c, job) is not None:
                # Released jobs are permanently barred from pinning again.
                raise InputError("$.job", "released")
            existing = self._pin(c, job)
            if existing is not None:
                if existing["caps"] != caps:
                    raise InputError("$.job", "mismatch")
                return {"floor": existing["floor"]}
            floor = self.floor(c)
            if caps > self.last(c) or caps < floor:
                # The requested cap was already collected, or never existed.
                raise InputError("$.caps", "collected")
            c.execute(
                "INSERT INTO pins(job,floor,caps,at) VALUES(?,?,?,?)",
                (job, floor, caps, time.time()),
            )
            return {"floor": floor}

    def snapshot(self, job, caps):
        self.pin(job, caps)
        with self.lock:
            floor = self.floor()
            rows = self.conn.execute(
                "SELECT v, rows_json, needs_json FROM versions WHERE v >= ? "
                "ORDER BY v ASC",
                (floor,),
            ).fetchall()
        versions = {}
        for offset, row in enumerate(rows):
            if row["v"] != floor + offset:
                raise RuntimeError("version gap in source %s" % self.name)
            versions[str(row["v"])] = {
                "rows": json.loads(row["rows_json"]),
                "needs": json.loads(row["needs_json"]),
            }
        return {"floor": floor, "versions": versions}

    def commit(self, job, v):
        if not is_name(job):
            raise InputError("$.job")
        if not is_version(v):
            raise InputError("$.v")
        with self.tx as c:
            receipt = self._receipt(c, job)
            if receipt is not None:
                if receipt["v"] == v:
                    return {"ok": True, "v": v, "floor": receipt["floor"]}
                raise InputError("$.job", "released")
            pin = self._pin(c, job)
            if pin is None:
                raise InputError("$.job", "unpinned")
            if not (self.floor(c) <= v <= pin["caps"]) or v > self.last(c):
                # The selected version left the pinned window (should be
                # impossible under the gc rule, so treat as a conflict).
                raise InputError("$.v", "collected")
            floor = self.floor(c)
            c.execute("DELETE FROM pins WHERE job=?", (job,))
            c.execute(
                "INSERT INTO released(job,v,floor,at) VALUES(?,?,?,?)",
                (job, v, floor, time.time()),
            )
            return {"ok": True, "v": v, "floor": floor}

    def release(self, job):
        if not is_name(job):
            raise InputError("$.job")
        with self.tx as c:
            receipt = self._receipt(c, job)
            if receipt is not None:
                return {"ok": True, "v": receipt["v"]}
            if self._pin(c, job) is None:
                return {"ok": True, "v": None}
            c.execute("DELETE FROM pins WHERE job=?", (job,))
            c.execute(
                "INSERT INTO released(job,v,floor,at) VALUES(?,NULL,NULL,?)",
                (job, time.time()),
            )
            return {"ok": True, "v": None}

    def info(self):
        with self.lock:
            return {
                "name": self.name,
                "floor": self.floor(),
                "last": self.last(),
                "pins": [
                    dict(r)
                    for r in self.conn.execute("SELECT job, floor, caps FROM pins")
                ],
                "released": [
                    dict(r)
                    for r in self.conn.execute("SELECT job, v FROM released")
                ],
            }


def build_routes(source):
    def need(payload, key):
        if key not in payload:
            raise InputError("$." + key)
        return payload[key]

    return {
        "/": lambda p: source.gc(need(p, "k")),
        "/int/pin": lambda p: source.pin(need(p, "job"), need(p, "caps")),
        "/int/snapshot": lambda p: source.snapshot(need(p, "job"), need(p, "caps")),
        "/int/commit": lambda p: source.commit(need(p, "job"), need(p, "v")),
        "/int/release": lambda p: source.release(need(p, "job")),
        "/int/info": lambda p: source.info(),
    }


def run_source(name, db_path, host="127.0.0.1", port=0):
    source = Source(name, db_path)
    server, thread = serve_in_thread(make_handler(build_routes(source)), host, port)
    return source, server, server.server_address[1]
