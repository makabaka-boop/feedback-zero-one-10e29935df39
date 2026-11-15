"""Test fixtures: build a live cluster with one HTTP process per source."""

import json
import os
import shutil
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from expcoord.common import Http  # noqa: E402
from expcoord.coordinator import Coordinator, init_coordinator_db  # noqa: E402
from expcoord.source import init_source_db, run_source  # noqa: E402
from expcoord.common import make_handler  # noqa: E402
from expcoord.coordinator import build_routes  # noqa: E402

import urllib.error  # noqa: E402
import urllib.request  # noqa: E402


def v0():
    return {"rows": [], "needs": {}}


def ver(rows, needs=None):
    return {"rows": list(rows), "needs": dict(needs or {})}


class Cluster:
    """A live cluster: N source HTTP servers + coordinator HTTP server."""

    def __init__(self, init_doc, workdir=None):
        self.init_doc = init_doc
        self.workdir = workdir or tempfile.mkdtemp(prefix="expcoord-test-")
        self._own_workdir = workdir is None
        self.source_servers = {}
        self.source_objs = {}
        self.source_urls = {}
        self.coord_server = None
        self.coordinator = None
        self.http = Http(timeout=5.0)
        self._start()

    def _start(self):
        for name, versions in self.init_doc.items():
            db = os.path.join(self.workdir, "source_%s.sqlite3" % name)
            init_source_db(db, name, versions)
            obj, server, port = run_source(name, db)
            self.source_objs[name] = obj
            self.source_servers[name] = server
            self.source_urls[name] = "http://127.0.0.1:%d" % port
        coord_db = os.path.join(self.workdir, "coordinator.sqlite3")
        init_coordinator_db(coord_db, self.init_doc, self.source_urls)
        self.coordinator = Coordinator(coord_db, self.http)
        self.coord_server, _ = self._serve_coord()

    def _serve_coord(self):
        from expcoord.common import serve_in_thread

        server, thread = serve_in_thread(
            make_handler(build_routes(self.coordinator)), "127.0.0.1", 0
        )
        return server, thread

    @property
    def coord_url(self):
        host, port = self.coord_server.server_address[:2]
        return "http://%s:%d" % (host, port)

    def call(self, path, payload):
        """POST to the coordinator and return the JSON body (errors included)."""
        status, body = self.raw_post(self.coord_url + path, json.dumps(payload).encode())
        assert status == 200, (status, body)
        return body

    def raw_post(self, url, body):
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def gc(self, name, k):
        """POST gc to a source and return the JSON body (errors included)."""
        status, body = self.raw_post(
            self.source_urls[name] + "/", json.dumps({"op": "gc", "k": k}).encode()
        )
        assert status == 200, (status, body)
        return body

    def src_info(self, name):
        status, body = self.raw_post(self.source_urls[name] + "/int/info", b"{}")
        assert status == 200, (status, body)
        return body

    def restart_coordinator(self):
        """Simulate a coordinator crash: new process, same database."""
        self.coord_server.shutdown()
        self.coord_server.server_close()
        coord_db = os.path.join(self.workdir, "coordinator.sqlite3")
        self.coordinator = Coordinator(coord_db, Http(timeout=5.0))
        self.coord_server, _ = self._serve_coord()
        return self.coordinator

    def close(self):
        for server in list(self.source_servers.values()):
            server.shutdown()
            server.server_close()
        if self.coord_server is not None:
            self.coord_server.shutdown()
            self.coord_server.server_close()
        if self._own_workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)


def make_cluster(init_doc):
    return Cluster(init_doc)


class FaultProxy:
    """A forwarding HTTP proxy used to inject network faults.

    ``mode`` controls what happens to a request that matches ``match``:
      * ``"drop_reply"``: forward it (the source acts) but answer 502, as if
        the response were lost on the way back.
      * ``"drop_request"``: never forward it; answer 502.
    """

    def __init__(self, upstream_url, match=None, mode="drop_reply"):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        self.upstream = upstream_url.rstrip("/")
        self.match = match or (lambda path, body: True)
        self.mode = mode
        self.hits = []
        self.lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                path = self.path
                hit = proxy.match(path, raw)
                if hit:
                    with proxy.lock:
                        proxy.hits.append((path, raw.decode("utf-8", "replace")))
                body = b"{}"
                status = 502
                if not (hit and proxy.mode == "drop_request"):
                    req = urllib.request.Request(
                        proxy.upstream + path,
                        data=raw,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    try:
                        with urllib.request.urlopen(req, timeout=5.0) as resp:
                            body = resp.read()
                            status = 200
                    except urllib.error.HTTPError as exc:
                        body = exc.read()
                        status = 200
                    except Exception:
                        body, status = b"{}", 502
                if hit and proxy.mode == "drop_reply":
                    body, status = b"{}", 502
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.server.server_address[1]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *a):
        self.server.shutdown()
        self.server.server_close()


class CrashSignal(Exception):
    pass


class CrashyHttp(Http):
    """An Http client that "kills the process" at a chosen request.

    ``crash_on(method=..., count=...)`` raises :class:`CrashSignal` instead
    of performing the request, after ``count`` matching calls have gone
    through.  The point is to stop the coordinator exactly between durable
    steps; the DB is left as a real crash would leave it.
    """

    def __init__(self, timeout=5.0):
        super().__init__(timeout)
        self.marker = None
        self.matched = 0

    def crash_on(self, marker):
        self.marker = marker
        self.matched = 0

    def post(self, url, payload):
        if self.marker is not None and self.marker(url, payload):
            self.matched += 1
            raise CrashSignal((url, payload))
        return super().post(url, payload)
