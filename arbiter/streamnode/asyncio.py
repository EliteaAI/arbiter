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
    Stream node (asyncio variant)

    A StreamNode that can be consumed from an asyncio event loop without
    blocking a thread per stream.

    The event bus still delivers events on its own (native) threads via
    on_stream_event().  The only change from the threaded StreamNode is the
    per-stream storage: instead of a queue.SimpleQueue (drained with a blocking
    get()), each stream is backed by a _LoopQueue whose put() is thread-safe
    (schedules the actual enqueue on the loop via call_soon_threadsafe) and
    whose get() is a coroutine awaited on the loop.

    All ordering/reassembly logic in StreamNode.on_stream_event is inherited
    unchanged: it calls stream.put(event), and _LoopQueue.put() is a drop-in
    for queue.SimpleQueue.put().  This keeps the per-stream sequence-number
    reassembly (chunk... end order) identical to the threaded node.
"""

import asyncio

from .streamnode import StreamNode
from .consumer import StreamConsumer
from .asyncio_consumer import AsyncStreamConsumer


class _LoopQueue:
    """ Minimal queue: thread-safe put(), awaitable get() bound to a loop

    put() may be called from any thread (typically an arbiter callback thread).
    get() must be awaited on the owning loop.  Implemented over an
    asyncio.Queue whose mutations are always scheduled on the loop via
    call_soon_threadsafe, so no cross-thread access to loop internals occurs.
    """

    def __init__(self, loop):
        self._loop = loop
        self._queue = asyncio.Queue()

    def put(self, item):
        """ Thread-safe put — schedules the enqueue on the owning loop """
        # asyncio.Queue is unbounded here (no maxsize), so put_nowait never
        # raises QueueFull; scheduling it on the loop keeps all queue state
        # touched from a single thread.
        self._loop.call_soon_threadsafe(self._queue.put_nowait, item)

    async def get(self, timeout=None):
        """ Await the next item, optionally with a timeout (seconds) """
        if timeout is None:
            return await self._queue.get()
        #
        return await asyncio.wait_for(self._queue.get(), timeout)


class AsyncStreamNode(StreamNode):  # pylint: disable=R0902,R0904
    """ StreamNode variant whose streams are consumed from an asyncio loop """

    def __init__(self, event_node, id_prefix="", loop=None):
        super().__init__(event_node, id_prefix=id_prefix)
        #
        # The loop the gate runs on.  Captured at construction time (the node
        # is created from async_main, i.e. on the loop thread) unless provided
        # explicitly.  All _LoopQueue puts are scheduled onto this loop.
        self._loop = loop if loop is not None else asyncio.get_event_loop()

    def add_stream(self, stream_id=None):
        """ Create stream backed by a loop-bound async queue """
        if stream_id is None:
            stream_id = self.generate_stream_id()
        #
        stream_queue = _LoopQueue(self._loop)
        #
        with self.lock:
            self.streams[stream_id] = stream_queue
        #
        return stream_id

    def get_consumer(self, stream_id, timeout=None):
        """ Get an async consumer wrapper (use `async for`) """
        return AsyncStreamConsumer(self, stream_id, timeout)

    def get_sync_consumer(self, stream_id, timeout=None):
        """ Get a blocking consumer wrapper

        Not usable against _LoopQueue-backed streams (get() is a coroutine);
        provided only for API symmetry / accidental-call diagnostics.
        """
        return StreamConsumer(self, stream_id, timeout)
