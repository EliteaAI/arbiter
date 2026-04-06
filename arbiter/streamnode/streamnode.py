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
        with self.lock:
            self.streams[stream_id] = queue.SimpleQueue()
        #
        return stream_id

    def remove_stream(self, stream_id):
        """ Destroy stream """
        with self.lock:
            stream = self.streams.pop(stream_id, None)
            #
            if stream is None:
                return
            #
            stream.put({
                "type": "stream_end",
                "data": None,
            })

    #
    # Stream changes
    #

    def stream_chunk(self, stream_id, chunk):
        """ Stream change """
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_chunk",
                "data": chunk,
            },
        )

    def stream_oob(self, stream_id, oob_tag, oob_payload):
        """ Stream change """
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_oob",
                "data": {
                    "tag": oob_tag,
                    "payload": oob_payload,
                },
            },
        )

    def stream_end(self, stream_id):
        """ Stream change """
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_end",
                "data": None,
            },
        )

    def stream_exception(self, stream_id, exception_info=None):
        """ Stream change """
        if exception_info is None:
            exception_info = traceback.format_exc()
        #
        self.event_node.emit(
            "stream_event",
            {
                "stream_id": stream_id,
                "type": "stream_exception",
                "data": exception_info,
            },
        )

    #
    # Event handlers
    #

    def on_stream_event(self, event_name, payload):
        """ Process stream event """
        _ = event_name
        #
        event = payload.copy()
        #
        stream_id = event.pop("stream_id", None)
        #
        if stream_id not in self.streams:
            return
        #
        self.streams[stream_id].put(event)

    #
    # Tools
    #

    def generate_stream_id(self):
        """ Get 'mostly' safe new stream_id """
        with self.lock:
            while True:
                stream_id = f"{self.id_prefix}{str(uuid.uuid4())}"
                #
                if stream_id in self.streams:
                    continue
                #
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
