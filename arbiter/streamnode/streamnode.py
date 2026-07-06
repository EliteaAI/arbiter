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
    Stream node

    Allows to use streams

    Uses existing EventNode as a transport
"""

import uuid
import queue
import threading
import traceback

from .emitter import StreamEmitter
from .consumer import StreamConsumer


class StreamNode:  # pylint: disable=R0902,R0904
    """ Stream node - use streams """

    def __init__(  # pylint: disable=R0913,R0914
            self, event_node,
            id_prefix="",
    ):
        self.event_node = event_node
        self.event_node_was_started = False
        #
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.started = False
        #
        self.id_prefix = id_prefix
        self.streams = {}
        #
        # Ordering support.
        #
        # The event bus may dispatch each event on its own thread
        # (callback_workers=None), which gives no delivery-order guarantee
        # between consecutive events of the same stream.  For a stream the
        # correct order is "chunk... chunk end", and losing it truncates the
        # stream (a stream_end overtaking a chunk makes the consumer stop
        # early -> StopIteration / hang).
        #
        # Fix, contained entirely in StreamNode: the emitting side stamps a
        # per-stream monotonic sequence number on every event; the receiving
        # side (on_stream_event) reassembles events into per-stream order
        # before handing them to the consumer queue.  Consumers/pumps keep
        # reading an already-ordered queue and need no changes.
        #
        # Emit side: stream_id -> next sequence number to assign.
        self.emit_seq = {}
        self.emit_seq_lock = threading.Lock()
        #
        # Receive side: stream_id -> {"expected": int, "pending": {seq: event}}
        # Guarded by self.lock (same lock that guards self.streams) so lookup,
        # buffering and in-order release are atomic w.r.t. remove_stream().
        self.recv_state = {}

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
        self.event_node.subscribe("stream_event", self.on_stream_event)
        #
        self.started = True
        #
        if block:
            self.stop_event.wait()

    def stop(self):
        """ Stop task node """
        self.event_node.unsubscribe("stream_event", self.on_stream_event)
        #
        for stream_id in list(self.streams):
            self.remove_stream(stream_id)
        #
        if self.event_node_was_started:
            self.event_node.stop()
        #
        self.started = False
        self.stop_event.set()

    #
    # Stream registration
    #

    def add_stream(self, stream_id=None):
        """ Create stream """
        if stream_id is None:
            stream_id = self.generate_stream_id()
        #
        stream_queue = queue.SimpleQueue()
        #
        with self.lock:
            self.streams[stream_id] = stream_queue
        #
        return stream_id

    def remove_stream(self, stream_id):
        """ Destroy stream """
        with self.lock:
            stream = self.streams.pop(stream_id, None)
            # Drop any reassembly buffer for this stream (may hold events that
            # arrived out of order and were never released).
            self.recv_state.pop(stream_id, None)
        #
        if stream is None:
            return
        #
        # Put a stream_end sentinel so any consumer that is currently blocked
        # on stream.get() wakes up and exits cleanly.  The queue object stays
        # alive until the consumer drains it; the dict entry is already gone
        # so on_stream_event will silently drop any further events for this
        # stream_id (which is the desired behaviour).
        stream.put({
            "type": "stream_end",
            "data": None,
        })

    #
    # Stream changes
    #

    def _emit_stream_event(self, stream_id, event_type, data):
        """ Emit a stream event stamped with a per-stream sequence number

        A single node emits all events for a given stream_id (one emitter per
        stream), so a simple monotonic counter yields a total order the
        receiving side can reassemble regardless of bus dispatch order.
        """
        with self.emit_seq_lock:
            seq = self.emit_seq.get(stream_id, 0)
            self.emit_seq[stream_id] = seq + 1
            #
            # Terminal events end the sequence: drop the counter so a reused
            # stream_id (unlikely, but possible) restarts cleanly.
            if event_type in ("stream_end", "stream_exception"):
                self.emit_seq.pop(stream_id, None)
        #
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "seq": seq,
                "type": event_type,
                "data": data,
            },
        )

    def stream_chunk(self, stream_id, chunk):
        """ Stream change """
        self._emit_stream_event(stream_id, "stream_chunk", chunk)

    def stream_oob(self, stream_id, oob_tag, oob_payload):
        """ Stream change """
        self._emit_stream_event(
            stream_id,
            "stream_oob",
            {
                "tag": oob_tag,
                "payload": oob_payload,
            },
        )

    def stream_end(self, stream_id):
        """ Stream change """
        self._emit_stream_event(stream_id, "stream_end", None)

    def stream_exception(self, stream_id, exception_info=None):
        """ Stream change """
        if exception_info is None:
            exception_info = traceback.format_exc()
        #
        self._emit_stream_event(stream_id, "stream_exception", exception_info)

    #
    # Event handlers
    #

    def on_stream_event(self, event_name, payload):
        """ Process stream event

        Reassembles events into per-stream sequence order before delivering
        them to the consumer queue.  The bus may deliver events of one stream
        out of order (per-event dispatch threads); the "seq" stamped by the
        emitting side lets us restore "chunk... end" order here so consumers
        never see a stream_end ahead of its chunks.
        """
        _ = event_name
        #
        event = payload.copy()
        #
        stream_id = event.pop("stream_id", None)
        seq = event.pop("seq", None)
        #
        # Do lookup, buffering and in-order release atomically w.r.t.
        # remove_stream().  We only release into the (thread-safe) queue while
        # holding the lock; the queue put itself does not require it, but
        # keeping it simple avoids interleaving with teardown.
        with self.lock:
            stream = self.streams.get(stream_id)
            #
            if stream is None:
                return
            #
            # Backwards/loose compatibility: unsequenced events (seq is None,
            # e.g. from an older peer or the remove_stream sentinel) are
            # delivered immediately without reassembly.
            if seq is None:
                stream.put(event)
                return
            #
            state = self.recv_state.get(stream_id)
            if state is None:
                state = {"expected": 0, "pending": {}}
                self.recv_state[stream_id] = state
            #
            # Drop stale/duplicate events already passed.
            if seq < state["expected"]:
                return
            #
            if seq > state["expected"]:
                # Out of order: buffer until the gap is filled.
                state["pending"][seq] = event
                return
            #
            # seq == expected: release this event and any now-contiguous ones.
            stream.put(event)
            state["expected"] += 1
            #
            while state["expected"] in state["pending"]:
                stream.put(state["pending"].pop(state["expected"]))
                state["expected"] += 1

    #
    # Tools
    #

    def generate_stream_id(self):
        """ Get 'mostly' safe new stream_id """
        # Fix: hold the lock for the full check-then-break so we never return
        # an ID that another thread is about to register.
        while True:
            stream_id = f"{self.id_prefix}{str(uuid.uuid4())}"
            #
            with self.lock:
                if stream_id not in self.streams:
                    break
        #
        return stream_id

    #
    # Wrappers
    #

    def get_emitter(self, stream_id):
        """ Get wrapper """
        return StreamEmitter(self, stream_id)

    def get_consumer(self, stream_id, timeout=None):
        """ Get wrapper """
        return StreamConsumer(self, stream_id, timeout)
