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
import math
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


# The leg that needs no network at all: a stall here has no latency explanation, so it is
# the unambiguous inherited-lock signal and it names which layer stalled in the note.
FORK_DNS_PROBE_TARGETS = (
    ("localhost", 80),
)

# The only leg reaching the resolver and dlopening the NSS module: a real name can be
# shortcut by /etc/hosts, and the local leg sends no packet and loads no module.
# Fully qualified on purpose: without the dot it is retried against every resolv.conf
# search domain (measured 8.7s vs 0.01s on 9 domains), which is itself a false abort.
FORK_DNS_RESOLVER_PROBE_TARGET = ("elitea-fork-probe.invalid.", 80)

FORK_DNS_PROBE_ENV_PREFIX = "ARBITER_FORK_DNS_PROBE_"

FORK_DNS_PROBE_TRUE_TOKENS = frozenset({"true", "1", "yes", "on"})
FORK_DNS_PROBE_FALSE_TOKENS = frozenset({"false", "0", "no", "off"})

FORK_DNS_PROBE_DEFAULTS = {
    "enabled": True,
    "timeout": 2.0,
}


def validate_probe_setting(name, raw, default):
    """ Keep the default on junk: a typo must not disable a guard or zero its budget """
    if isinstance(default, bool):
        token = str(raw).strip().lower()
        #
        if token in FORK_DNS_PROBE_TRUE_TOKENS:
            return True
        #
        if token in FORK_DNS_PROBE_FALSE_TOKENS:
            return False
        #
        log.warning("Ignoring invalid %s=%r, keeping %r", name, raw, default)
        return default
    #
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        log.warning("Ignoring invalid %s=%r, keeping %r", name, raw, default)
        return default
    #
    # nan, inf, zero and negatives all make the join() budget meaningless
    if not math.isfinite(parsed) or parsed <= 0:
        log.warning("Ignoring out-of-range %s=%r, keeping %r", name, raw, default)
        return default
    #
    return parsed


def resolve_fork_dns_probe_setting(name, value):
    """ Env wins over the caller, and either may be junk: fall back to the shipped default """
    # Env must stay usable without a plugin change and an image roll
    resolved = validate_probe_setting(
        f"fork_dns_probe_{name}", value, FORK_DNS_PROBE_DEFAULTS[name],
    )
    #
    env_name = f"{FORK_DNS_PROBE_ENV_PREFIX}{name.upper()}"
    raw = os.environ.get(env_name)
    #
    if raw is None:
        return resolved
    #
    return validate_probe_setting(env_name, raw, resolved)


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


def probe_dns_usable(timeout=2.0):
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
    #
    if finished:
        return True
    #
    # Which leg stalled is diagnostic only: a healthy resolver answers in well under a
    # millisecond (measured), so either stall leaves this child unusable.
    stderr_note(
        "[fork_dns_probe] no answer in %ss, stalled on the %s lookups" % (
            timeout, "resolver" if local_done else "local",
        )
    )
    return False
