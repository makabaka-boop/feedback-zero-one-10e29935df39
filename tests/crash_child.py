"""Child process used by tests/test_multiprocess.py.

Runs a real coordinator (own OS process, own SQLite database) and SIGKILLs
itself at a chosen step of the start protocol, so the crash lands at an
exact point rather than at an arbitrary moment.

    python3 tests/crash_child.py DB SOURCES_JSON JOB CAPS_JSON CRASH_AT

CRASH_AT:
    pin       just before the first /int/pin request
    snapshot  just before the first /int/snapshot (all pins already taken)
    resolved  after solving, before the plan is stored
    publish   after the plan is stored, before the ready row is durable
    commit0   just before the first /int/commit
    commit1   after the first source committed, before the second
    release0  after publishing, just before the first /int/release

The hook is the coordinator's ``checkpoint`` attribute, which exists only for
this purpose and is never set by the service itself.
"""

import json
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from expcoord.common import Http  # noqa: E402
from expcoord.coordinator import Coordinator  # noqa: E402


class KillSwitch(Http):
    def __init__(self, crash_at):
        super().__init__(timeout=3.0)
        self.crash_at = crash_at
        self.fired = False

    def post(self, url, payload):
        path = "/" + url.split("/", 3)[3] if url.count("/") >= 3 else "/"
        if not self.fired:
            hit = (
                (self.crash_at == "pin" and path == "/int/pin")
                or (self.crash_at == "snapshot" and path == "/int/snapshot")
            )
            if hit:
                self.fired = True
                os.kill(os.getpid(), signal.SIGKILL)
        return super().post(url, payload)


def main():
    db, sources_path, job, caps_json, crash_at = sys.argv[1:6]
    with open(sources_path) as fh:
        urls = json.load(fh)
    caps = json.loads(caps_json)

    coord = Coordinator(db, KillSwitch(crash_at))
    coord.sources = dict(urls)
    source_order = list(coord.source_names)

    def checkpoint(stage, name=None):
        if crash_at == "resolved" and stage == "resolved":
            os.kill(os.getpid(), signal.SIGKILL)
        if stage == "release":
            index = source_order.index(name)
            # "commit0"/"commit1" fire just before that source is released,
            # i.e. after each earlier source has committed.
            if crash_at == "commit%d" % index:
                os.kill(os.getpid(), signal.SIGKILL)
            if crash_at == "release0" and index == 0:
                os.kill(os.getpid(), signal.SIGKILL)

    coord.checkpoint = checkpoint

    if crash_at == "publish":
        # Kill just after the plan row is durable, before publishing.
        saved = coord._save_plan

        def save_then_die(job_name, plan):
            saved(job_name, plan)
            os.kill(os.getpid(), signal.SIGKILL)

        coord._save_plan = save_then_die

    coord.start(job, caps)
    json.dump({"result": coord.query(job)}, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
