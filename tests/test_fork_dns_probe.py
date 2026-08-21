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

from arbiter.eventnode.mock import MockEventNode
from arbiter.tasknode.tasknode import TaskNode
from arbiter.tasknode.tools import (
    FORK_DNS_PROBE_ENV_PREFIX,
    FORK_DNS_PROBE_TARGETS,
    FORK_DNS_RESOLVER_PROBE_TARGET,
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
    def test_the_local_leg_needs_no_network():
        # localhost still opens nsswitch.conf, resolv.conf, /etc/hosts and the nscd socket
        # (measured), so a hang here is an inherited lock with no latency explanation.
        hosts = [host for host, _ in FORK_DNS_PROBE_TARGETS]
        assert hosts == ["localhost"]
        assert FORK_DNS_RESOLVER_PROBE_TARGET[0] not in hosts

    @staticmethod
    def test_no_numeric_target_is_probed():
        # A numeric literal short-circuits inside getaddrinfo before any NSS machinery:
        # measured 0 file opens, 0 sockets, 0 dlopens. It can only ever prove nothing.
        assert not [
            host for host, _ in FORK_DNS_PROBE_TARGETS
            if host.replace(".", "").isdigit() or ":" in host
        ]

    @staticmethod
    def test_the_resolver_leg_is_separate_from_the_local_one():
        # The only leg that reaches the resolver, and the only one that dlopens an NSS
        # module - i.e. the only one exercising _dl_load_lock, a distinct deadlock.
        assert ".invalid" in FORK_DNS_RESOLVER_PROBE_TARGET[0]

    @staticmethod
    def test_nxdomain_target_is_fully_qualified():
        # Without the trailing dot the resolver retries the name against every
        # resolv.conf search domain. On a 9-domain search list that walk measured 8.7s
        # against a 2s probe timeout, which aborts perfectly healthy children.
        assert FORK_DNS_RESOLVER_PROBE_TARGET[0].endswith("."), \
            "the NXDOMAIN probe target must be fully qualified"


class TestProbeDnsUsable:
    @staticmethod
    def test_healthy_resolver_is_reported_usable(monkeypatch):
        # Faked rather than real: a CI runner without egress DNS sits right on glibc's
        # default timeout:5 attempts:2, which would make this flaky. Real-network
        # behaviour is exercised by the subprocess tests instead.
        monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [("fake",)])
        assert probe_dns_usable(10.0) is True

    @staticmethod
    def test_a_resolver_slower_than_the_budget_is_treated_as_unusable(monkeypatch):
        # Deliberate, and the reason the budget is one number instead of two: no timeout can
        # prove a mutex, so the guard measures against what healthy costs here instead. A
        # healthy resolver answers this target in well under a millisecond (0.33ms median
        # measured on a live cluster), so anything near the budget leaves the child unusable
        # either way. A deployment that genuinely needs longer raises the env timeout.
        release = threading.Event()
        real_getaddrinfo = socket.getaddrinfo

        def wedged_network_target(host, *args, **kwargs):
            if host == FORK_DNS_RESOLVER_PROBE_TARGET[0]:
                release.wait()
            return real_getaddrinfo(host, *args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", wedged_network_target)
        try:
            assert probe_dns_usable(0.3) is False
        finally:
            release.set()

    @staticmethod
    def test_which_leg_stalled_is_reported_on_stderr(monkeypatch, capfd):
        # Diagnostic only - it never changes the verdict - but it is the single line that
        # tells an operator whether to look at the resolver or at the fork itself.
        release = threading.Event()
        real_getaddrinfo = socket.getaddrinfo

        def wedged_network_target(host, *args, **kwargs):
            if host == FORK_DNS_RESOLVER_PROBE_TARGET[0]:
                release.wait()
            return real_getaddrinfo(host, *args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", wedged_network_target)
        try:
            assert probe_dns_usable(0.3) is False
            assert "stalled on the resolver lookups" in capfd.readouterr().err
        finally:
            release.set()

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


class TestAbortHelperIsForkOnly:
    @staticmethod
    def test_a_non_fork_context_raises_without_touching_process_state():
        # The helper is the advertised extension point for future startup guards, and the
        # obvious next call site is the threading executor - which runs in the pylon
        # process. There the handler surgery would permanently strip the pylon's eventnode
        # logging, and the events branch would os._exit the whole pylon. In-process the
        # plain raise is already correct, so that is all it is allowed to do.
        handler = _RecordingHandler()
        logging.root.addHandler(handler)
        error = ForkDnsUnusableError("nope")
        try:
            with pytest.raises(ForkDnsUnusableError) as raised:
                TaskNode(None)._abort_task_startup(  # pylint: disable=W0212
                    "task-1", "events", error, "threading",
                )
            assert raised.value is error
            assert handler in logging.root.handlers
        finally:
            logging.root.removeHandler(handler)


class TestForkingParentResolvesNoNames:
    @staticmethod
    def test_start_performs_no_name_resolution(monkeypatch):
        # The regression this replaces: an earlier revision calibrated the probe here, in the
        # parent. On timeout that lookup could not be cancelled, so a thread stayed parked
        # inside getaddrinfo for the process lifetime - and then the process forked. That is
        # the precondition of the very bug being guarded against, manufactured by the guard.
        # Counted rather than raised: every probe helper swallows exceptions by design, so an
        # exploding stub would be caught and the call would go unnoticed.
        calls = []
        monkeypatch.setattr(
            socket, "getaddrinfo", lambda *a, **k: calls.append(a) or [("fake",)],
        )
        node = TaskNode(
            MockEventNode(), multiprocessing_context="fork", result_transport="memory",
        )
        try:
            node.start()
            assert node.started
            assert not calls, calls
        finally:
            node.stop()

    @staticmethod
    def test_no_probe_thread_survives_into_the_fork_window(monkeypatch):
        # The same failure from the side that actually bit: a lookup that never returns. The
        # parent cannot cancel it, so it must never have been started here in the first place.
        release = threading.Event()

        def blocked_getaddrinfo(*args, **kwargs):  # pylint: disable=W0613
            release.wait()
            raise socket.gaierror("blocked")

        monkeypatch.setattr(socket, "getaddrinfo", blocked_getaddrinfo)
        node = TaskNode(
            MockEventNode(), multiprocessing_context="fork", result_transport="memory",
        )
        try:
            node.start()
            lingering = [
                thread.name for thread in threading.enumerate()
                if "fork_dns" in thread.name
            ]
            assert not lingering, lingering
        finally:
            release.set()
            node.stop()


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
    def test_env_overrides_win_over_the_constructor(monkeypatch):
        # The ops surface. No plugin wires these to descriptor config, so without an env
        # override the only remedy for a misfiring default-on guard is a plugin change
        # plus an image roll - too slow for something that can abort a whole fork pool.
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}ENABLED", "false")
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}TIMEOUT", "4.5")
        node = TaskNode(None, fork_dns_probe_enabled=True, fork_dns_probe_timeout=1.0)
        assert node.fork_dns_probe_enabled is False
        assert node.fork_dns_probe_timeout == 4.5

    @staticmethod
    def test_an_unparseable_env_override_falls_back_instead_of_raising(monkeypatch):
        # A typo in an env var must not stop the pylon from starting.
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}TIMEOUT", "two seconds")
        assert TaskNode(None).fork_dns_probe_timeout == 2.0

    @staticmethod
    @pytest.mark.parametrize("raw", ["treu", "", "maybe", "1.0", "none"])
    def test_a_misspelled_enable_flag_keeps_the_guard_on(monkeypatch, raw):
        # The dangerous direction: this guard is default-ON, so anything not recognised as
        # an explicit false must leave it on. A truthiness check would silently disable it.
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}ENABLED", raw)
        assert TaskNode(None).fork_dns_probe_enabled is True

    @staticmethod
    @pytest.mark.parametrize("raw", ["off", "0", "no", "FALSE", " false "])
    def test_an_explicit_false_token_disables_the_guard(monkeypatch, raw):
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}ENABLED", raw)
        assert TaskNode(None).fork_dns_probe_enabled is False

    @staticmethod
    @pytest.mark.parametrize("raw", ["0", "-1", "nan", "inf", "-0.0"])
    def test_a_nonsensical_timeout_keeps_the_default(monkeypatch, raw):
        # These all reach Thread.join() as something that is not a wait: zero and negatives
        # return immediately (every child aborted), nan compares false against everything.
        monkeypatch.setenv(f"{FORK_DNS_PROBE_ENV_PREFIX}TIMEOUT", raw)
        assert TaskNode(None).fork_dns_probe_timeout == 2.0

    @staticmethod
    @pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), "soon", None])
    def test_a_nonsensical_constructor_timeout_keeps_the_default(value):
        # Same validation for a caller as for an operator: a plugin passing a computed
        # timeout that came out zero must not turn the guard into an unconditional abort.
        assert TaskNode(None, fork_dns_probe_timeout=value).fork_dns_probe_timeout == 2.0

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
