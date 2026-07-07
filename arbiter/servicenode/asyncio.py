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
    Service node (asyncio variant)

    A ServiceNode whose request() is a coroutine awaited on the event loop.

    The wire protocol is identical to the threaded ServiceNode: it emits
    service_discovery / service_request and waits for service_provider /
    service_response replies delivered by the event bus on its callback
    threads.  The only difference is that the per-request reply queues are
    _LoopQueue instances (thread-safe put(), awaitable get()) instead of
    queue.SimpleQueue, so waiting for a reply suspends the coroutine on the
    loop instead of blocking a thread.

    This lets the gate issue service calls (e.g. wsgi_request_start) for many
    concurrent connections without consuming a thread per in-flight call.
"""

import queue
import asyncio
import traceback

from .servicenode import ServiceNode
from ..streamnode.asyncio import _LoopQueue


class AsyncServiceNode(ServiceNode):  # pylint: disable=R0902,R0904
    """ ServiceNode variant with an awaitable request() """

    def __init__(  # pylint: disable=R0913,R0914
            self, event_node,
            id_prefix="",
            default_timeout=None,
            default_discovery_attempts=1,
            default_request_exception=queue.Empty,
            loop=None,
    ):
        super().__init__(
            event_node,
            id_prefix=id_prefix,
            default_timeout=default_timeout,
            default_discovery_attempts=default_discovery_attempts,
            default_request_exception=default_request_exception,
        )
        #
        # The gate loop.  Captured at construction (the node is created on the
        # loop thread) unless provided explicitly.
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        #
        # Async proxy: `await node.call.some_service(*args)`.
        self.call = AsyncServiceNodeProxy(self)

    async def request(  # pylint: disable=R0913
            self, service,
            args=None, kwargs=None,
            timeout=...,
            discovery_attempts=...,
            request_exception=...,
    ):
        """ Service request (awaitable) """
        if not self.started:
            raise RuntimeError("ServiceNode is not started")
        #
        if discovery_attempts is ...:
            discovery_attempts = self.default_discovery_attempts
        #
        if request_exception is ...:
            request_exception = self.default_request_exception
        #
        for _ in range(discovery_attempts):
            try:
                return await self._request(service, args, kwargs, timeout, request_exception)
            except request_exception:  # pylint: disable=E0712
                continue
        #
        raise request_exception()

    async def _request(  # pylint: disable=R0913
            self, service, args=None, kwargs=None, timeout=..., request_exception=...,
    ):
        if timeout is ...:
            timeout = self.default_timeout
        #
        if request_exception is ...:
            request_exception = self.default_request_exception
        #
        request_id = self.generate_request_id()
        #
        discovery_queue = f'{request_id}:discovery'
        request_queue = f'{request_id}:request'
        #
        # _LoopQueue: on_service_provider / on_service_response run on bus
        # callback threads and call self.queues[target].put(payload); put()
        # schedules onto the loop, and we await get() below.
        with self.lock:
            self.known_ids.add(request_id)
            self.queues[discovery_queue] = _LoopQueue(self._loop)
            self.queues[request_queue] = _LoopQueue(self._loop)
        #
        try:
            self.event_node.emit(
                "service_discovery",
                {
                    "service": service,
                    "reply_to": discovery_queue,
                }
            )
            #
            while True:
                try:
                    provider = await self.queues[discovery_queue].get(timeout=timeout)
                except asyncio.TimeoutError:
                    # No providers available, raise for possible discovery retry
                    raise request_exception(traceback.format_exc())  # pylint: disable=W0707
                #
                self.event_node.emit(
                    "service_request",
                    {
                        "target": provider["ident"],
                        "service": service,
                        "args": args,
                        "kwargs": kwargs,
                        "reply_to": request_queue,
                    }
                )
                #
                try:
                    response = await self.queues[request_queue].get(timeout=timeout)
                except asyncio.TimeoutError:
                    # Response timeout, try next provider if present
                    continue
                #
                if "raise" in response:
                    raise response.get("raise", RuntimeError())
                return response.get("return", None)
        finally:
            with self.lock:
                self.queues.pop(request_queue, None)
                self.queues.pop(discovery_queue, None)
                self.known_ids.discard(request_id)


class AsyncServiceNodeProxy:  # pylint: disable=R0903
    """ Async service node proxy — `await proxy.service(*args, **kwargs)` """

    def __init__(self, service_node, timeout=...):
        self.__service_node = service_node
        self.__timeout = timeout if timeout is not ... else service_node.default_timeout
        self.__partials = {}

    def __call__(self, *args, **kwargs):
        return AsyncServiceNodeProxy(self.__service_node, *args, **kwargs)

    async def __request(self, service, *args, **kwargs):
        return await self.__service_node.request(
            service=service,
            args=args,
            kwargs=kwargs,
            timeout=self.__timeout,
        )

    def __getattr__(self, name):
        if name not in self.__partials:
            # functools.partial won't bind the coroutine method nicely for the
            # name arg; a small closure keeps `await proxy.name(...)` working.
            async def _call(*args, __service=name, **kwargs):
                return await self.__request(__service, *args, **kwargs)
            #
            self.__partials[name] = _call
        #
        return self.__partials[name]
