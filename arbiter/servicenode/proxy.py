#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0116,C0302

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
    Service node

"""

import functools


class ServiceNodeProxy:  # pylint: disable=R0902,R0903,R0904
    """ Service node proxy - call services """

    def __init__(  # pylint: disable=R0913,R0914
            self, service_node,
            timeout=...,
    ):
        self.__service_node = service_node
        self.__timeout = timeout if timeout is not ... else self.__service_node.default_timeout
        self.__partials = {}

    def __call__(self, *args, **kwargs):
        return ServiceNodeProxy(self.__service_node, *args, **kwargs)

    def __request(self, service, *args, **kwargs):
        return self.__service_node.request(
            service=service,
            args=args,
            kwargs=kwargs,
            timeout=self.__timeout,
        )

    def __getattr__(self, name):
        if name not in self.__partials:
            self.__partials[name] = functools.partial(self.__request, name)
        return self.__partials[name]
