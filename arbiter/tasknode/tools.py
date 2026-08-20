#!/usr/bin/python3
# coding=utf-8

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

"""
    Task node
"""

import os
import socket
import logging
import threading

from arbiter import log


def reap_zombies():
    """ Reap zombie processes """
    while True:
        try:
            child_siginfo = os.waitid(os.P_ALL, os.getpid(), os.WEXITED | os.WNOHANG)  # pylint: disable=E1101
            #
            if child_siginfo is None:
                break
            #
            log.info(
                "Reaped child process: %s -> %s -> %s",
                child_siginfo.si_pid,
                child_siginfo.si_code,
                child_siginfo.si_status,
            )
        except:  # pylint: disable=W0702
            break


class InterruptTaskThread(Exception):
    """ Special exception sent to thread in stop_task """
    pass  # pylint: disable=W0107


class TaskStartupError(Exception):
    """ Base for pre-execution guard failures: the task was refused, never started """
    pass  # pylint: disable=W0107


class ForkDnsUnusableError(TaskStartupError):
    """ Raised in a forked child that inherited a locked glibc resolver mutex """
    pass  # pylint: disable=W0107


# Decisive: these need no network, so a hang here can only be the inherited lock.
FORK_DNS_PROBE_TARGETS = (
    ("localhost", 80),
    ("127.0.0.1", 0),
)

# Advisory: catches a resolver lock the local shapes miss, but a slow network is
# indistinguishable from a wedge - so this one gets a long budget, not a fast verdict.
# Fully qualified on purpose: without the dot it is retried against every resolv.conf
# search domain (measured 8.7s vs 0.01s on 9 domains), which is itself a false abort.
FORK_DNS_RESOLVER_PROBE_TARGET = ("elitea-fork-probe.invalid.", 80)

FORK_DNS_PROBE_ENV_PREFIX = "ARBITER_FORK_DNS_PROBE_"


def fork_dns_probe_env_override(name, value):
    """ Env wins: a default-on guard must be tunable without a plugin change and image roll """
    raw = os.environ.get(f"{FORK_DNS_PROBE_ENV_PREFIX}{name.upper()}")
    #
    if raw is None:
        return value
    #
    if isinstance(value, bool):
        return raw.strip().lower() in ("true", "1", "yes", "on")
    #
    try:
        return float(raw)
    except ValueError:
        return value


def stderr_note(message):
    """ Diagnostic that cannot wedge: no logging, no name resolution """
    try:
        os.write(2, message.encode("utf-8", "replace") + b"\n")
    except:  # pylint: disable=W0702
        pass


def detach_resolving_log_handlers():
    """ Keep only stream handlers: eventnode ones publish to Redis, which needs DNS """
    try:
        for handler in list(logging.root.handlers):
            if isinstance(handler, logging.StreamHandler):
                continue
            # No close(): closing an eventnode handler stops its node, which can talk Redis
            logging.root.removeHandler(handler)
    except:  # pylint: disable=W0702
        pass


def probe_dns_usable(timeout=2.0, resolver_timeout=15.0):
    """ True if getaddrinfo still works in this process """
    # On a throwaway thread, not under SIGALRM: a Python signal handler never fires on a
    # C-level futex deadlock, but join() on a wedged thread does return.
    local_done = []
    finished = []
    #
    def _probe():
        for host, port in FORK_DNS_PROBE_TARGETS:
            try:
                socket.getaddrinfo(host, port)
            except:  # pylint: disable=W0702
                pass  # resolution failing is fine; hanging is not
        #
        local_done.append(True)
        #
        try:
            socket.getaddrinfo(*FORK_DNS_RESOLVER_PROBE_TARGET)
        except:  # pylint: disable=W0702
            pass
        #
        finished.append(True)
    #
    thread = threading.Thread(target=_probe, name="fork_dns_probe", daemon=True)
    thread.start()
    #
    thread.join(timeout)
    if not local_done:
        return False  # a lookup that needs no network hung: the lock is held
    #
    # Slow is not wedged, and only an endless wait is worth aborting a task over: a
    # struggling resolver used to abort healthy children fleet-wide (#6288 review).
    thread.join(resolver_timeout)
    return bool(finished)
