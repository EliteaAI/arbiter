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
    Stream tools (asyncio variant)
"""

import asyncio
import threading

from arbiter import log


class AsyncStreamConsumer:  # pylint: disable=R0902,R0904
    """ Async stream consumer — iterate with `async for` on the event loop """

    def __init__(  # pylint: disable=R0913,R0914
            self, stream_node, stream_id, timeout=None,
    ):
        self.stream_node = stream_node
        self.stream_id = stream_id
        #
        self.timeout = timeout
        #
        # OOB handlers may be either plain callables or coroutine functions.
        # A coroutine handler is awaited inline in the iteration loop; a plain
        # callable is invoked directly (must not block the loop).
        self.oob_handlers = {}
        self.oob_handlers_lock = threading.Lock()

    def register_oob_handler(self, tag=None, handler=None):
        """ OOB """
        if handler is None:
            return
        #
        with self.oob_handlers_lock:
            if tag not in self.oob_handlers:
                self.oob_handlers[tag] = []
            #
            if handler not in self.oob_handlers[tag]:
                self.oob_handlers[tag].append(handler)

    def unregister_oob_handler(self, tag=None, handler=None):
        """ OOB """
        if handler is None:
            return
        #
        with self.oob_handlers_lock:
            if tag not in self.oob_handlers:
                return
            #
            if handler in self.oob_handlers[tag]:
                self.oob_handlers[tag].remove(handler)

    async def _dispatch_oob(self, event_data):
        if not isinstance(event_data, dict):
            return
        #
        oob_tag = event_data.get("tag", None)
        oob_payload = event_data.get("payload", None)
        #
        with self.oob_handlers_lock:
            handlers = list(self.oob_handlers.get(oob_tag, []))
        #
        for handler in handlers:
            try:
                result = handler(oob_tag, oob_payload)
                if asyncio.iscoroutine(result):
                    await result
            except:  # pylint: disable=W0702
                log.exception("OOB handler '%s' failed, skipping", handler)

    async def __aiter__(self):
        """ Consume """
        try:
            # Grab the queue reference under the lock so we never race with a
            # concurrent remove_stream() that pops the entry first.
            with self.stream_node.lock:
                stream = self.stream_node.streams.get(self.stream_id)
            #
            if stream is None:
                raise RuntimeError(
                    f"Stream {self.stream_id!r} was removed before iteration started"
                )
            #
            while True:
                event = await stream.get(timeout=self.timeout)
                #
                event_type = event.get("type", None)
                event_data = event.get("data", None)
                #
                if event_type == "stream_end":
                    break
                #
                if event_type == "stream_oob":
                    await self._dispatch_oob(event_data)
                #
                if event_type == "stream_chunk":
                    yield event_data
                #
                if event_type == "stream_exception":
                    raise RuntimeError(event_data)
        finally:
            self.stream_node.remove_stream(self.stream_id)
