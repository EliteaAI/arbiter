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
    RPC node

    Allows to call and register RPC functions

    Uses existing EventNode as a transport
"""

import time
import functools

from arbiter import log
from ..servicenode import ServiceNode


class RpcNode:  # pylint: disable=R0902
    """ RPC node - register and call remote functions """

    def __init__(self, event_node, id_prefix=None, trace=False, proxy_timeout=None):
        self.event_node = event_node
        self.event_node_was_started = False
        #
        self.service_node = ServiceNode(
            event_node=self.event_node,
            id_prefix=id_prefix if id_prefix is not None else "",
            default_timeout=proxy_timeout,
        )
        #
        if proxy_timeout is None:
            self.proxy = RpcProxy(self)
        else:
            self.proxy = RpcTimeoutProxy(self, proxy_timeout)
        #
        self.started = False
        self.trace = trace

    def start(self):
        """ Start RPC node """
        if self.started:
            return
        #
        if not self.event_node.started:
            self.event_node.start()
            self.event_node_was_started = True
        #
        self.service_node.start()
        self.started = True

    def stop(self):
        """ Stop RPC node """
        self.service_node.stop()
        #
        if self.event_node_was_started:
            self.event_node.stop()

    def register(self, func, name=None):
        """ Register RPC function """
        self.service_node.register(func, name)

    def unregister(self, func, name=None):
        """ Unregister RPC function """
        self.service_node.unregister(func, name)

    def call(self, func, *args, **kvargs):
        """ Invoke RPC function """
        if not self.started:
            raise RuntimeError("RpcNode is not started")
        #
        if func in self.service_node.services:
            return self.service_node.services[func](*args, **kvargs)
        #
        if self.trace:
            trace_start = time.time()
        #
        try:
            return self.service_node.request(
                service=func,
                args=args,
                kwargs=kvargs,
            )
        finally:
            if self.trace:
                trace_end = time.time()
                log.info(
                    "[RPC TRACE] %s() = %s seconds",
                    func, trace_end - trace_start,
                )

    def call_with_timeout(self, func, timeout, *args, **kvargs):
        """ Invoke RPC function with timeout """
        if not self.started:
            raise RuntimeError("RpcNode is not started")
        #
        if func in self.service_node.services:
            return self.service_node.services[func](*args, **kvargs)
        #
        if self.trace:
            trace_start = time.time()
        #
        try:
            return self.service_node.request(
                service=func,
                args=args,
                kwargs=kvargs,
                timeout=timeout,
            )
        finally:
            if self.trace:
                trace_end = time.time()
                log.info(
                    "[RPC TRACE] %s() = %s seconds",
                    func, trace_end - trace_start,
                )

    def timeout(self, timeout):
        """ Get RpcTimeoutProxy instance """
        return RpcTimeoutProxy(self, timeout)


class RpcProxy:  # pylint: disable=R0903
    """ RPC proxy - syntax sugar for RPC calls """
    def __init__(self, rpc_node):
        self.__rpc_node = rpc_node
        self.__partials = {}

    def __invoke(self, func, *args, **kvargs):
        return self.__rpc_node.call(func, *args, **kvargs)

    def __getattr__(self, name):
        if name not in self.__partials:
            self.__partials[name] = functools.partial(self.__invoke, name)
        return self.__partials[name]


class RpcTimeoutProxy:  # pylint: disable=R0903
    """ RPC proxy - syntax sugar for RPC calls - with timeout support """
    def __init__(self, rpc_node, timeout):
        self.__rpc_node = rpc_node
        self.__timeout = timeout
        self.__partials = {}

    def __invoke(self, func, *args, **kvargs):
        return self.__rpc_node.call_with_timeout(func, self.__timeout, *args, **kvargs)

    def __getattr__(self, name):
        if name not in self.__partials:
            self.__partials[name] = functools.partial(self.__invoke, name)
        return self.__partials[name]
