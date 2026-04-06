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
    Tools
"""


def is_runtime_gevent():
    """ Detect gevent """
    try:
        import pylon  # pylint: disable=C0415,E0401,W0611
        from tools import context  # pylint: disable=C0415,E0401
        #
        is_gevent = context.web_runtime == "gevent"
    except:  # pylint: disable=W0702
        is_gevent = False
    #
    return is_gevent
