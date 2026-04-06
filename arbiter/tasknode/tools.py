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
