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

    Allows to use services

    Uses existing EventNode as a transport
"""

import uuid
import queue
import functools
import threading
import traceback

from .proxy import ServiceNodeProxy


class ServiceNode:  # pylint: disable=R0902,R0904
    """ Service node - use services """

    def __init__(  # pylint: disable=R0913,R0914
            self, event_node,
            id_prefix="",
            default_timeout=None,
            default_discovery_attempts=1,
            default_request_exception=queue.Empty,
    ):
        self.event_node = event_node
        self.event_node_was_started = False
        #
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.started = False
        #
        self.id_prefix = id_prefix
        self.known_ids = set()
        self.ident = None
        #
        self.default_timeout = default_timeout
        self.default_discovery_attempts = default_discovery_attempts
        self.default_request_exception = default_request_exception
        #
        self.services = {}
        self.queues = {}
        #
        self.call = ServiceNodeProxy(self)

    #
    # Node start and stop
    #

    def start(self, block=False):
        """ Start node """
        if self.started:
            return
        #
        self.stop_event.clear()
        #
        if not self.event_node.started:
            self.event_node.start()
            self.event_node_was_started = True
        #
        self.event_node.subscribe("service_discovery", self.on_service_discovery)
        self.event_node.subscribe("service_provider", self.on_service_provider)
        self.event_node.subscribe("service_request", self.on_service_request)
        self.event_node.subscribe("service_response", self.on_service_response)
        #
        self.ident = f"{self.id_prefix}{str(uuid.uuid4())}"
        self.started = True
        #
        if block:
            self.stop_event.wait()

    def stop(self):
        """ Stop task node """
        self.event_node.unsubscribe("service_response", self.on_service_response)
        self.event_node.unsubscribe("service_request", self.on_service_request)
        self.event_node.unsubscribe("service_provider", self.on_service_provider)
        self.event_node.unsubscribe("service_discovery", self.on_service_discovery)
        #
        if self.event_node_was_started:
            self.event_node.stop()
        #
        self.started = False
        self.stop_event.set()

    #
    # Service registration
    #

    def register(self, callback, name=None):
        """ Register service """
        if name is None:
            name = self.get_callable_name(callback)
        #
        with self.lock:
            self.services[name] = callback

    def unregister(self, callback, name=None):
        """ Unregister service """
        if name is None:
            name = self.get_callable_name(callback)
        #
        with self.lock:
            if name in self.services:
                self.services.pop(name)

    #
    # Service request
    #

    def request(  # pylint: disable=R0913
            self, service,
            args=None, kwargs=None,
            timeout=...,
            discovery_attempts=...,
            request_exception=...,
    ):
        """ Service request """
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
                return self._request(service, args, kwargs, timeout, request_exception)
            except request_exception:  # pylint: disable=E0712
                continue
        #
        raise request_exception()

    def _request(self, service, args=None, kwargs=None, timeout=..., request_exception=...):  # pylint: disable=R0913
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
        with self.lock:
            self.known_ids.add(request_id)
            self.queues[discovery_queue] = queue.SimpleQueue()
            self.queues[request_queue] = queue.SimpleQueue()
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
                    provider = self.queues[discovery_queue].get(timeout=timeout)
                except BaseException as exception_data:
                    # No providers available, raise for possible discovery retry
                    if isinstance(exception_data, request_exception):
                        raise exception_data
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
                    response = self.queues[request_queue].get(timeout=timeout)
                except:  # pylint: disable=W0702
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

    #
    # Event handlers
    #

    def on_service_discovery(self, event_name, payload):
        """ Process service event """
        _ = event_name
        #
        if payload.get("service") not in self.services:
            return
        #
        self.event_node.emit(
            "service_provider",
            {
                "target": payload.get("reply_to"),
                "ident": self.ident,
            }
        )

    def on_service_provider(self, event_name, payload):
        """ Process service event """
        _ = event_name
        #
        target = payload.get("target")
        #
        if target not in self.queues:
            return
        #
        self.queues[target].put(payload)

    def on_service_request(self, event_name, payload):
        """ Process service event """
        _ = event_name
        #
        if payload.get("target") != self.ident:
            return
        #
        service = payload.get("service")
        if service not in self.services:
            return
        #
        args = payload.get("args")
        if args is None:
            args = []
        #
        kwargs = payload.get("kwargs")
        if kwargs is None:
            kwargs = {}
        #
        try:
            return_data = self.services[service](*args, **kwargs)
            #
            self.event_node.emit(
                "service_response",
                {
                    "target": payload.get("reply_to"),
                    "return": return_data,
                }
            )
        except BaseException as exception_data:  # pylint: disable=W0703
            self.event_node.emit(
                "service_response",
                {
                    "target": payload.get("reply_to"),
                    "raise": exception_data,
                }
            )

    def on_service_response(self, event_name, payload):
        """ Process service event """
        _ = event_name
        #
        target = payload.get("target")
        #
        if target not in self.queues:
            return
        #
        self.queues[target].put(payload)

    #
    # Tools
    #

    def get_callable_name(self, func):
        """ Get function name """
        if hasattr(func, "__name__"):
            return func.__name__
        #
        if isinstance(func, functools.partial):
            return self.get_callable_name(func.func)
        #
        raise ValueError("Cannot guess callable name")

    def generate_request_id(self):
        """ Get 'mostly' safe new request_id """
        with self.lock:
            while True:
                request_id = f"{self.id_prefix}{str(uuid.uuid4())}"
                #
                if request_id in self.known_ids:
                    continue
                #
                break
        #
        return request_id
