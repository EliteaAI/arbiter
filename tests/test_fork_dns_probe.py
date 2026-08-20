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

import logging
import socket
import threading

import pytest  # pylint: disable=E0401,W0611

from arbiter.tasknode.tasknode import TaskNode
from arbiter.tasknode.tools import (
    FORK_DNS_PROBE_TARGETS,
    ForkDnsUnusableError,
    InterruptTaskThread,
    TaskStartupError,
    detach_resolving_log_handlers,
    probe_dns_usable,
    stderr_note,
)


class _RecordingHandler(logging.Handler):
    """ Stand-in for an eventnode log handler: not a StreamHandler, and notes close() """

    def __init__(self):
        super().__init__()
        self.close_calls = 0

    def emit(self, record):
        pass

    def close(self):
        self.close_calls += 1
        super().close()


class TestGuardExceptionHierarchy:
    @staticmethod
    def test_fork_dns_error_is_a_task_startup_error():
        # Consumers (and future guards) key off the base class, so a new pre-execution
        # check only needs a new subclass here - no change on the reporting side.
        assert issubclass(ForkDnsUnusableError, TaskStartupError)

    @staticmethod
    def test_task_startup_error_is_not_the_thread_interrupt_sentinel():
        # A user pressing Stop must never look like a startup failure.
        assert not issubclass(TaskStartupError, InterruptTaskThread)
        assert not issubclass(InterruptTaskThread, TaskStartupError)


class TestProbeTargets:
    @staticmethod
    def test_three_distinct_nss_paths_are_probed():
        # Probing a single shape lets roughly 15% of poisoned children through, because
        # a resolvable name, an NXDOMAIN name and a bare IP take different NSS paths.
        assert len(FORK_DNS_PROBE_TARGETS) == 3
        hosts = [host for host, _ in FORK_DNS_PROBE_TARGETS]
        assert "localhost" in hosts
        assert "127.0.0.1" in hosts

    @staticmethod
    def test_nxdomain_target_is_fully_qualified():
        # Without the trailing dot the resolver retries the name against every
        # resolv.conf search domain. On a 9-domain search list that walk measured 8.7s
        # against a 2s probe timeout, which aborts perfectly healthy children.
        nxdomain = [host for host in (h for h, _ in FORK_DNS_PROBE_TARGETS) if ".invalid" in host]
        assert len(nxdomain) == 1
        assert nxdomain[0].endswith("."), "the NXDOMAIN probe target must be fully qualified"


class TestProbeDnsUsable:
    @staticmethod
    def test_healthy_resolver_is_reported_usable():
        # Generous timeout on purpose: this asserts "the probe completes", not "how fast".
        assert probe_dns_usable(10.0) is True

    @staticmethod
    def test_blocked_resolver_is_reported_unusable_within_the_timeout(monkeypatch):
        # Emulates the inherited-locked-mutex shape: the lookup never returns, so the
        # probe thread never finishes and join(timeout) is the only thing that comes back.
        release = threading.Event()

        def blocked_getaddrinfo(*args, **kwargs):  # pylint: disable=W0613
            release.wait()
            raise socket.gaierror("blocked")

        monkeypatch.setattr(socket, "getaddrinfo", blocked_getaddrinfo)
        try:
            assert probe_dns_usable(0.3) is False
        finally:
            # Let the daemon probe thread unwind instead of leaking it into later tests.
            release.set()


class TestResolutionFreeAbortPath:
    @staticmethod
    def test_stderr_note_resolves_nothing(monkeypatch, capfd):
        def exploding_getaddrinfo(*args, **kwargs):  # pylint: disable=W0613
            raise AssertionError("stderr_note must not resolve any name")

        monkeypatch.setattr(socket, "getaddrinfo", exploding_getaddrinfo)
        stderr_note("[task_startup] hello")
        assert "[task_startup] hello" in capfd.readouterr().err

    @staticmethod
    def test_detach_keeps_stream_handlers_and_never_closes_the_others():
        original = list(logging.root.handlers)
        stream_handler = logging.StreamHandler()
        resolving_handler = _RecordingHandler()
        logging.root.handlers = [stream_handler, resolving_handler]
        try:
            detach_resolving_log_handlers()
            assert logging.root.handlers == [stream_handler]
            # close() on an eventnode handler stops its node, which can itself talk Redis.
            assert resolving_handler.close_calls == 0
        finally:
            logging.root.handlers = original


class TestTaskNodeOptions:
    @staticmethod
    def test_probe_options_default_to_on_with_a_two_second_timeout():
        node = TaskNode(None)
        assert node.fork_dns_probe_enabled is True
        assert node.fork_dns_probe_timeout == 2.0

    @staticmethod
    def test_probe_options_are_configurable():
        node = TaskNode(None, fork_dns_probe_enabled=False, fork_dns_probe_timeout=0.5)
        assert node.fork_dns_probe_enabled is False
        assert node.fork_dns_probe_timeout == 0.5

    @staticmethod
    def test_unknown_option_is_ignored_with_a_warning(caplog):
        # Plugins are updated independently of the arbiter shipped inside the pylon image,
        # so "newer plugin, older arbiter" is normal. A TypeError here would stop the whole
        # pylon from starting - but a silent no-op on a safety flag is just as bad, hence
        # both halves are pinned: it must construct, and it must say what it dropped.
        with caplog.at_level(logging.WARNING, logger="arbiter.tasknode.tasknode"):
            node = TaskNode(None, some_future_option=123)
        assert not hasattr(node, "some_future_option")
        assert any(
            "some_future_option" in record.getMessage() and record.levelno == logging.WARNING
            for record in caplog.records
        )
