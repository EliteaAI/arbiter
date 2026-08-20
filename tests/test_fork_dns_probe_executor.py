#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0114,C0115,C0116,C0411,C0103

#   Copyright 2026 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# These cases drive the real executor, so they cannot run in the pytest process: the fork
# branch of _executor__multiprocessing ends in os._exit(). Each case therefore runs in its
# own interpreter that plays the part of the forked child - it installs a resolver that
# never returns, calls node.executor(..., "fork", ...) and is allowed to exit. The pytest
# process then inspects the exit code, the stderr notes and the result artifact.

import gzip
import os
import pickle
import subprocess
import sys
from pathlib import Path

import pytest  # pylint: disable=E0401,W0611

from arbiter.tasknode.tasknode import TaskNode


REPO_ROOT = str(Path(__file__).resolve().parents[1])
TASK_ID = "fork-dns-probe-task"

CHILD_SCRIPT = """
import os
import socket
import sys
import threading
import types

sys.path.insert(0, os.environ["PROBE_REPO_ROOT"])

if os.environ["PROBE_MODE"] == "poisoned":
    # Same observable shape as the inherited glibc resolver mutex: the lookup blocks and
    # never comes back, so the probe thread cannot finish and only join(timeout) returns.
    _never = threading.Event()

    def _wedged_getaddrinfo(*args, **kwargs):
        _never.wait()
        raise socket.gaierror("wedged")

    socket.getaddrinfo = _wedged_getaddrinfo

from arbiter.tasknode import tasknode as tasknode_module
from arbiter.tasknode.tasknode import TaskNode


def _forbidden_make_event_node(*args, **kwargs):
    sys.stderr.write("MAKE_EVENT_NODE_WAS_CALLED\\n")
    sys.stderr.flush()
    raise AssertionError("make_event_node must not be called on the abort path")


tasknode_module.make_event_node = _forbidden_make_event_node

# The threading executor assigns onto this module instead of creating it.
sys.modules["tasknode_task"] = types.ModuleType("tasknode_task")


def probe_target(value):
    return {"ran": True, "value": value}


node = TaskNode(
    types.SimpleNamespace(can_emit=True, event_callbacks={}, catch_all_callbacks=[]),
    fork_dns_probe_enabled=os.environ["PROBE_ENABLED"] == "1",
    fork_dns_probe_timeout=0.3,
)


class MemorySink:
    # Stands in for the caller-owned queue: memory transport only needs a put(), and a real
    # Queue would be unreadable after the fork branch os._exit()s.
    def __init__(self, path):
        self.path = path

    def put(self, value):
        with open(self.path, "wb") as file:
            file.write(value)

    def close(self):
        pass

    def join_thread(self):
        pass


if os.environ["PROBE_TRANSPORT"] == "memory":
    result_config = MemorySink(
        os.path.join(os.environ["PROBE_RESULT_CONFIG"], "memory.bin")
    )
else:
    result_config = os.environ["PROBE_RESULT_CONFIG"]

node.executor(
    "probe_task", probe_target, os.environ["PROBE_TASK_ID"], {}, [7], {},
    os.environ["PROBE_TRANSPORT"], result_config,
    os.environ["PROBE_CONTEXT"], None,
)

# Only reachable under the threading executor; the fork branch os._exit()s above.
print("EXECUTOR_RETURNED")
"""


def run_child(tmp_path, mode, transport="files", context="fork", enabled="1"):
    """ Run one executor case in its own interpreter and return the completed process """
    env = dict(os.environ)
    env.update({
        "PROBE_REPO_ROOT": REPO_ROOT,
        "PROBE_MODE": mode,
        "PROBE_TRANSPORT": transport,
        "PROBE_CONTEXT": context,
        "PROBE_ENABLED": enabled,
        "PROBE_TASK_ID": TASK_ID,
        "PROBE_RESULT_CONFIG": str(tmp_path),
    })
    return subprocess.run(  # pylint: disable=W1510
        [sys.executable, "-c", CHILD_SCRIPT],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120,
    )


def read_result(tmp_path):
    """ Unpickle the file-transport result artifact the way the watcher does """
    return pickle.loads(gzip.decompress((tmp_path / f"{TASK_ID}.bin").read_bytes()))


def read_memory_result(tmp_path):
    """ Same for what the memory transport handed to the caller's queue """
    return pickle.loads(gzip.decompress((tmp_path / "memory.bin").read_bytes()))


class TestHealthyChild:
    @staticmethod
    def test_probe_lets_a_healthy_fork_child_run_its_target(tmp_path):
        # The false-positive guard: a working resolver must be completely transparent.
        completed = run_child(tmp_path, "healthy")
        assert completed.returncode == 0, completed.stderr
        assert read_result(tmp_path) == {"return": {"ran": True, "value": 7}}
        assert "[task_startup]" not in completed.stderr


class TestPoisonedChild:
    @staticmethod
    def test_failure_is_serialized_as_a_raise_and_re_raised_by_get_task_result(tmp_path):
        completed = run_child(tmp_path, "poisoned")
        # Exit 0 is correct here: the child did its job, which was to report and stop.
        assert completed.returncode == 0, completed.stderr
        assert "[task_startup]" in completed.stderr
        #
        result = read_result(tmp_path)
        assert "return" not in result
        assert "arbiter.tasknode.tools.ForkDnsUnusableError" in result["raise"]
        #
        # Parent side: DNS works there, so the traceback must surface as a real exception
        # rather than the Ellipsis that stop_task and a missing result both produce.
        node = TaskNode(None)
        node.global_task_state[TASK_ID] = {
            "result": (tmp_path / f"{TASK_ID}.bin").read_bytes(),
        }
        with pytest.raises(Exception) as raised:
            node.get_task_result(TASK_ID)
        assert "ForkDnsUnusableError" in str(raised.value)

    @staticmethod
    def test_memory_transport_reports_the_failure_like_files_does(tmp_path):
        # Memory transport is a put() on a queue the caller owns - no name resolution - so it
        # must report normally. Only the events transport has to bail out.
        completed = run_child(tmp_path, "poisoned", transport="memory")
        assert completed.returncode == 0, completed.stderr
        assert "MAKE_EVENT_NODE_WAS_CALLED" not in completed.stderr
        assert "[task_startup]" in completed.stderr
        #
        result = read_memory_result(tmp_path)
        assert "return" not in result
        assert "arbiter.tasknode.tools.ForkDnsUnusableError" in result["raise"]

    @staticmethod
    def test_events_transport_exits_without_touching_the_event_node(tmp_path):
        # The events transport ships the result over Redis, which needs the very lookup
        # this process can no longer make - so it must exit instead of wedging in the reply.
        completed = run_child(tmp_path, "poisoned", transport="events")
        assert completed.returncode == 1, completed.stderr
        assert "MAKE_EVENT_NODE_WAS_CALLED" not in completed.stderr
        assert "[task_startup]" in completed.stderr
        assert "result transport is 'events'" in completed.stderr
        assert not (tmp_path / f"{TASK_ID}.bin").exists()


class TestProbeGating:
    @staticmethod
    def test_disabled_by_kwarg_skips_the_probe(tmp_path):
        completed = run_child(tmp_path, "poisoned", enabled="0")
        assert completed.returncode == 0, completed.stderr
        assert read_result(tmp_path) == {"return": {"ran": True, "value": 7}}
        assert "[task_startup]" not in completed.stderr

    @staticmethod
    def test_threading_context_skips_the_probe(tmp_path):
        # One process, so no lock can be inherited across a fork and there is nothing to
        # detect. Paying for a probe on every threaded task start would be pure overhead.
        completed = run_child(tmp_path, "poisoned", context="threading")
        assert completed.returncode == 0, completed.stderr
        assert "EXECUTOR_RETURNED" in completed.stdout
        assert read_result(tmp_path) == {"return": {"ran": True, "value": 7}}
        assert "[task_startup]" not in completed.stderr
