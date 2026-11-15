"""Command line entry points.

    python -m expcoord cluster --init init.json --workdir DIR [--host H] [--port P]
    python -m expcoord source  --db PATH --port P
    python -m expcoord coordinator --db PATH --sources sources.json --port P

``cluster`` is the convenient launcher: it validates the initial document,
creates one SQLite database per source plus the coordinator's, starts every
source on an ephemeral port, then the coordinator, prints the coordinator URL
as JSON and serves until Ctrl-C.
"""

import argparse
import json
import os
import sys
import threading

from .common import Http, InputError
from .coordinator import init_coordinator_db, run_coordinator
from .source import init_source_db, run_source
from .validate import validate_init


def cmd_cluster(args):
    with open(args.init, "r", encoding="utf-8") as fh:
        init_doc = json.load(fh)
    try:
        validate_init(init_doc)
    except InputError as exc:
        print(json.dumps({"error": exc.path}))
        return 2

    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    urls = {}
    servers = []
    for name, versions in init_doc.items():
        db = os.path.join(workdir, "source_%s.sqlite3" % name)
        init_source_db(db, name, versions)
        _source, server, port = run_source(name, db, args.host)
        servers.append(server)
        urls[name] = "http://%s:%d" % (args.host, port)
    coord_db = os.path.join(workdir, "coordinator.sqlite3")
    init_coordinator_db(coord_db, init_doc, urls)
    coordinator, server, port = run_coordinator(coord_db, Http(), args.host, args.port)
    servers.append(server)
    coordinator.recover()
    coordinator.start_worker()
    print(
        json.dumps(
            {"coordinator": "http://%s:%d" % (args.host, port), "sources": urls}
        ),
        flush=True,
    )
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    coordinator.stop_worker()
    for s in servers:
        s.shutdown()
    return 0


def cmd_source(args):
    _source, server, port = run_source(
        args.name, args.db, args.host, args.port
    )
    print(json.dumps({"url": "http://%s:%d" % (args.host, port), "name": args.name}), flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    server.shutdown()
    return 0


def cmd_coordinator(args):
    with open(args.sources, "r", encoding="utf-8") as fh:
        urls = json.load(fh)
    coordinator, server, port = run_coordinator(
        args.db, Http(), args.host, args.port
    )
    coordinator.recover()
    coordinator.start_worker()
    print(json.dumps({"url": "http://%s:%d" % (args.host, port)}), flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    coordinator.stop_worker()
    server.shutdown()
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="expcoord")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("cluster", help="start sources and coordinator together")
    p.add_argument("--init", required=True)
    p.add_argument("--workdir", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0)
    p.set_defaults(fn=cmd_cluster)

    p = sub.add_parser("source", help="run one source service")
    p.add_argument("--db", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0)
    p.set_defaults(fn=cmd_source)

    p = sub.add_parser("coordinator", help="run the coordinator service")
    p.add_argument("--db", required=True)
    p.add_argument("--sources", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0)
    p.set_defaults(fn=cmd_coordinator)

    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
