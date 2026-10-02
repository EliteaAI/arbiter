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

import os
import time
import signal
import logging
import multiprocessing

import pytest  # pylint: disable=E0401

from arbiter.tasknode.watcher import TaskNodeWatcher

from tests.test_orphan_reverify import make_node


def _sleep_forever():
    time.sleep(60)


def _exit_with(code):
    os._exit(code)  # pylint: disable=W0212


def _start(node, task_id, target, args=()):
    """ Register a real fork child the way execute_local_task would """
    process = multiprocessing.get_context("fork").Process(target=target, args=args)
    process.start()
    #
    node.local_tasks[task_id] = {"name": "t", "meta": {}}
    node.running_tasks[task_id] = {"process": process, "result": None}
    node.have_running_tasks.set()
    #
    return process


def _drain(node, task_id):
    """ Run watcher passes until the task is announced stopped """
    watcher = TaskNodeWatcher(node)
    deadline = time.time() + 10
    #
    while task_id in node.running_tasks and time.time() < deadline:
        watcher._watch_stopped_tasks__multiprocessing()  # pylint: disable=W0212


def _messages(caplog):
    return [(r.levelno, r.getMessage()) for r in caplog.records if r.name.startswith("arbiter")]


@pytest.fixture
def node(tmp_path):
    # files is the default transport: a killed child leaves no .bin behind
    return make_node(
        multiprocessing_context="fork", result_transport="files",
        tmp_path=str(tmp_path), watcher_max_wait=0.2,
    )


class TestMissingResultLogging:
    """ A child that dies without a result must be visible in logs, unlike a Stop """

    @staticmethod
    def test_sigkill_without_stop_logs_a_death_warning(node, caplog):
        # Stand-in for the OOM killer: SIGKILL that nobody asked for
        caplog.set_level(logging.INFO)
        process = _start(node, "oom", _sleep_forever)
        os.kill(process.pid, signal.SIGKILL)
        #
        _drain(node, "oom")
        #
        assert (logging.WARNING, "Task oom died (SIGKILL) without producing a result") \
            in _messages(caplog)

    @staticmethod
    def test_nonzero_exit_logs_the_exit_code(node, caplog):
        caplog.set_level(logging.INFO)
        _start(node, "crash", _exit_with, (3,))
        #
        _drain(node, "crash")
        #
        assert (logging.WARNING, "Task crash died (exit 3) without producing a result") \
            in _messages(caplog)

    @staticmethod
    @pytest.mark.parametrize("kill_on_stop,signame", [(False, "SIGTERM"), (True, "SIGKILL")])
    def test_requested_stop_is_not_reported_as_a_death(node, caplog, kill_on_stop, signame):
        # kill_on_stop makes a Stop look exactly like an OOM by exit code alone
        caplog.set_level(logging.INFO)
        node.kill_on_stop = kill_on_stop
        _start(node, "stopped", _sleep_forever)
        node._stop_task__multiprocessing("stopped")  # pylint: disable=W0212
        #
        _drain(node, "stopped")
        #
        messages = _messages(caplog)
        assert (logging.INFO, f"Task stopped stopped on request ({signame}), no result") \
            in messages
        assert not [m for level, m in messages if level >= logging.WARNING and "died" in m]

    @staticmethod
    def test_clean_exit_logs_nothing(node, caplog):
        caplog.set_level(logging.INFO)
        _start(node, "clean", _exit_with, (0,))
        #
        _drain(node, "clean")
        #
        assert not [m for _, m in _messages(caplog) if "died" in m or "on request" in m]
