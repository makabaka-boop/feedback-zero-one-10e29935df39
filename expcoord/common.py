"""Shared pieces: input validation, SQLite helpers, HTTP client/server plumbing.

Everything here is standard library only (httpserver, sqlite3, json, urllib).
"""

import json
import os
import re
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Source names and job names: non-empty ASCII alphanumeric strings.
NAME_RE = re.compile(r"^[A-Za-z0-9]+$")


class InputError(Exception):
    """A request payload violated the specification.

    ``path`` is a zero-based JSONPath (``$`` itself for a syntax error);
    it is reported verbatim inside ``{"error": <path>}``.  Internal source
    rejections carry an extra machine-readable ``reason``.
    """

    def __init__(self, path, reason=None):
        super().__init__(path)
        self.path = path
        self.reason = reason


def is_name(value):
    return isinstance(value, str) and NAME_RE.match(value) is not None


def is_version(value):
    """Integers only; booleans are rejected even though bool subclasses int."""
    return isinstance(value, int) and not isinstance(value, bool)


def require(condition, path):
    if not condition:
        raise InputError(path)


def is_syntax_error(exc):
    """True when a JSON text is malformed (as opposed to semantically invalid)."""
    return isinstance(exc, ValueError) and not isinstance(exc, InputError)


def reply(state):
    """Normalise a handler result into the response body."""
    if "error" in state:
        out = {"error": state["error"]}
        if "reason" in state:
            out["reason"] = state["reason"]
        return out
    return state


# --------------------------------------------------------------------------
# SQLite


def open_db(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class Tx:
    """Write transactions, serialised inside the process as well as in SQLite.

    With ``isolation_level=None`` every statement autocommits; explicit
    BEGIN IMMEDIATE/COMMIT makes multi-row state changes atomic, and the
    in-process lock keeps threads from fighting over the same write lock.
    """

    def __init__(self, conn, lock):
        self._conn = conn
        self._lock = lock

    def __enter__(self):
        self._lock.acquire()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            return self._conn
        except BaseException:
            self._lock.release()
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()
        return False


class HttpError(Exception):
    """A source answered with an explicit error body."""

    def __init__(self, status, payload):
        super().__init__(payload)
        self.status = status
        self.payload = payload


class SourceUnreachable(Exception):
    pass


class SourceProtocolError(Exception):
    pass


# --------------------------------------------------------------------------
# HTTP client


class Http:
    """Small JSON-over-HTTP client with timeouts and connection reuse."""

    # Transient connect resets happen when a burst of clients hits the
    # accept queue.  Every endpoint here is idempotent per (job, request):
    # pin/snapshot are keyed by job, commit/release replay their receipt,
    # and gc(k) with the same k gives the same floor.  Retrying a request
    # the peer may already have executed is therefore harmless.
    def __init__(self, timeout=3.0, retries=2):
        self.timeout = timeout
        self.retries = retries
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
        )

    def post(self, url, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        attempt = 0
        while True:
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    raw = resp.read()
                    status = resp.status
                break
            except urllib.error.HTTPError as exc:
                raw = exc.read()
                status = exc.code
                break
            except (urllib.error.URLError, OSError, socket.timeout) as exc:
                attempt += 1
                if attempt > self.retries:
                    raise SourceUnreachable(f"{url}: {exc}") from exc
                time.sleep(0.01 * attempt)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SourceProtocolError(f"{url}: non-JSON response ({status})") from exc
        if not isinstance(data, dict):
            raise SourceProtocolError(f"{url}: response is not an object")
        if status >= 500:
            # Gateways and crashed servers alike: the request may or may not
            # have landed, which is exactly the retryable case.
            raise SourceUnreachable(f"{url}: HTTP {status}")
        if status != 200 or "error" in data:
            raise HttpError(status, data)
        return data


# --------------------------------------------------------------------------
# HTTP server


class JsonHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    # The stdlib default backlog (5) drops connections under modest bursts,
    # which surfaces as ECONNRESET on the client.  Coordinators and sources
    # both accept many concurrent jobs.
    request_queue_size = 256


def make_handler(routes, log_path=None):
    """Build a request handler for ``{path: fn(payload)->dict}`` routes.

    Any request other than a JSON-object POST to a known route is answered
    with ``{"error": "$"}``; validation paths are produced by the routes.
    """

    log_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # keep stdout quiet
            if log_path:
                with log_lock:
                    try:
                        with open(log_path, "a", encoding="utf-8") as fh:
                            fh.write("%s %s\n" % (self.address_string(), fmt % args))
                    except OSError:
                        pass

        def _send(self, status, obj):
            raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            try:
                self.wfile.write(raw)
            except OSError:
                pass

        def do_GET(self):
            self._send(404, {"error": "$"})

        def do_POST(self):
            fn = routes.get(self.path)
            if fn is None:
                self._send(404, {"error": "$"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                self._send(400, {"error": "$"})
                return
            try:
                raw = self.rfile.read(length) if length else b""
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    raise InputError("$")
                if not isinstance(payload, dict):
                    raise InputError("$")
                result = fn(payload)
                self._send(200, reply(result))
            except InputError as exc:
                body = {"error": exc.path}
                if exc.reason:
                    body["reason"] = exc.reason
                self._send(200, body)
            except Exception:  # never let a bug become a broken socket
                self._send(500, {"error": "$"})

    return Handler


def serve_in_thread(handler, host, port):
    """Start a server on its own thread.  Returns ``(server, thread)``."""
    server = JsonHttpServer((host, port), handler)
    host, port = server.server_address[:2]
    thread = threading.Thread(target=server.serve_forever, name="http", daemon=True)
    thread.start()
    return server, thread


def wait_for_http(url, timeout=10.0):
    """Block until ``url`` answers a GET, or raise SourceUnreachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=0.5).close()
            return True
        except urllib.error.HTTPError:
            return True  # server is up; it just does not serve GET
        except (urllib.error.URLError, OSError, socket.timeout):
            time.sleep(0.05)
    raise SourceUnreachable(url)
