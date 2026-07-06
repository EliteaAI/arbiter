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
    Event node
"""

import hmac
import gzip
import queue
import pickle
import threading

from arbiter import log

from .tools import make_event_node
from . import hooks


class EventNodeBase:  # pylint: disable=R0902
    """ Event node (base) - allows to subscribe to events and to emit new events """

    def __init__(
            self,
            hmac_key=None, hmac_digest="sha512",
            callback_workers=1,
            log_errors=True,
            use_emit_queue=False,
            emitting_workers=1,
    ):  # pylint: disable=R0913
        self.clone_config = None
        #
        self.log_errors = log_errors
        self.event_callbacks = {}  # event_name -> [callbacks]
        self.catch_all_callbacks = []
        #
        self.before_callback_hooks = []
        self.after_callback_hooks = []
        #
        self.hmac_key = hmac_key
        self.hmac_digest = hmac_digest
        if self.hmac_key is not None and isinstance(self.hmac_key, str):
            self.hmac_key = self.hmac_key.encode("utf-8")
        #
        self.stop_event = threading.Event()
        self.event_lock = threading.Lock()
        #
        # Events listed here are dispatched inline (in the single drain
        # thread) instead of on a per-event thread, which preserves strict
        # FIFO delivery order for their handlers.  Used for ordering-sensitive
        # events (e.g. "stream_event": chunk... end) whose handlers are
        # contractually non-blocking.  Maps event_name -> refcount so several
        # nodes can share one bus.  Only consulted by callback_spawner.
        self.inline_dispatch_events = {}
        #
        self.queue_get_timeout = 1
        #
        self.sync_queue = queue.SimpleQueue()
        self.use_emit_queue = use_emit_queue
        #
        self.emit_queue = None
        self.emitting_threads = None
        #
        if self.use_emit_queue:
            self.emit_queue = queue.SimpleQueue()
            self.emitting_threads = []
            #
            for _ in range(emitting_workers):
                self.emitting_threads.append(
                    threading.Thread(target=self.emitting_worker, daemon=True)
                )
        #
        self.listening_thread = threading.Thread(target=self.listening_worker, daemon=True)
        self.callback_threads = []
        #
        if callback_workers is None:
            self.callback_threads.append(
                threading.Thread(target=self.callback_spawner, daemon=True)
            )
        else:
            for _ in range(callback_workers):
                self.callback_threads.append(
                    threading.Thread(target=self.callback_worker, daemon=True)
                )
        #
        self.emitting_ready_event = threading.Event()
        self.listening_ready_event = threading.Event()
        #
        self.can_emit = True
        self.started = False

    def clone(self):
        """ Make new event node with same config """
        if self.clone_config is None:
            raise NotImplementedError
        #
        return make_event_node(config=self.clone_config)

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        if self.use_emit_queue:
            for emitting_thread in self.emitting_threads:
                emitting_thread.start()
        else:
            self.emitting_ready_event.set()
        #
        if emit_only:
            self.listening_ready_event.set()
        else:
            self.listening_thread.start()
            #
            for callback_thread in self.callback_threads:
                callback_thread.start()
        #
        self.emitting_ready_event.wait()
        self.listening_ready_event.wait()
        #
        self.started = True

    def stop(self):
        """ Stop event node """
        self.stop_event.set()
        #
        # FIXME: wait for threads?

    @property
    def running(self):
        """ Check if it is time to stop """
        return not self.stop_event.is_set()

    def register_inline_dispatch_event(self, event_name):
        """ Mark an event to be dispatched in-order (inline in drain thread)

        Handlers for such events MUST be non-blocking; they run in the single
        callback drain thread, so a blocking handler would stall the whole
        bus.  Idempotent per-caller via refcount.
        """
        with self.event_lock:
            self.inline_dispatch_events[event_name] = \
                self.inline_dispatch_events.get(event_name, 0) + 1

    def unregister_inline_dispatch_event(self, event_name):
        """ Drop one registration of an inline-dispatch event """
        with self.event_lock:
            if event_name not in self.inline_dispatch_events:
                return
            #
            self.inline_dispatch_events[event_name] -= 1
            #
            if self.inline_dispatch_events[event_name] <= 0:
                del self.inline_dispatch_events[event_name]

    def decode_data(self, data):
        """ Verify and decode raw event data into an event dict

        Returns None on any digest/decode failure (logging as configured), so
        callers can simply skip the event.
        """
        try:
            if self.hmac_key is not None:
                hmac_obj = hmac.new(self.hmac_key, digestmod=self.hmac_digest)
                hmac_size = hmac_obj.digest_size
                #
                body_digest = data[-hmac_size:]
                data = data[:-hmac_size]
                #
                digest = hmac.digest(self.hmac_key, data, self.hmac_digest)
                #
                if not hmac.compare_digest(body_digest, digest):
                    if self.log_errors:
                        log.error("Invalid event digest, skipping")
                    #
                    return None
            #
            return pickle.loads(gzip.decompress(data))
        except:  # pylint: disable=W0702
            if self.log_errors:
                log.exception("Error during event processing, skipping")
            #
            return None

    def subscribe(self, event_name, callback):
        """ Subscribe to event """
        with self.event_lock:
            if event_name is ...:
                if callback not in self.catch_all_callbacks:
                    self.catch_all_callbacks.append(callback)
                return
            #
            if event_name not in self.event_callbacks:
                self.event_callbacks[event_name] = []
            if callback not in self.event_callbacks[event_name]:
                self.event_callbacks[event_name].append(callback)

    def unsubscribe(self, event_name, callback):
        """ Unsubscribe from event """
        with self.event_lock:
            if event_name is ...:
                if callback in self.catch_all_callbacks:
                    self.catch_all_callbacks.remove(callback)
                return
            #
            if event_name not in self.event_callbacks:
                return
            if callback not in self.event_callbacks[event_name]:
                return
            self.event_callbacks[event_name].remove(callback)

    def emit(self, event_name, payload=None):
        """ Emit event with payload data """
        if not self.can_emit:
            return
        #
        data = self.make_event_data(event_name, payload)
        #
        if self.use_emit_queue:
            self.emit_queue.put(data)
        else:
            self.emit_data(data)

    def make_event_data(self, event_name, payload=None):
        """ Make event data """
        event = {
            "name": event_name,
            "payload": payload,
        }
        #
        data = gzip.compress(pickle.dumps(
            event, protocol=pickle.HIGHEST_PROTOCOL
        ))
        #
        if self.hmac_key is not None:
            digest = hmac.digest(self.hmac_key, data, self.hmac_digest)
            data = data + digest
        #
        return data

    def emit_data(self, data):
        """ Emit event data """

    def emitting_worker(self):
        """ Emitting thread: emit event data from emit_queue """

    def listening_worker(self):
        """ Listening thread: push event data to sync_queue """

    def add_before_callback_hook(self, hook):
        """ Register pre-callback hook """
        with self.event_lock:
            if hook not in self.before_callback_hooks:
                self.before_callback_hooks.append(hook)

    def remove_before_callback_hook(self, hook):
        """ De-register pre-callback hook """
        with self.event_lock:
            while hook in self.before_callback_hooks:
                self.before_callback_hooks.remove(hook)

    def add_after_callback_hook(self, hook):
        """ Register post-callback hook """
        with self.event_lock:
            if hook not in self.after_callback_hooks:
                self.after_callback_hooks.append(hook)

    def remove_after_callback_hook(self, hook):
        """ De-register post-callback hook """
        with self.event_lock:
            while hook in self.after_callback_hooks:
                self.after_callback_hooks.remove(hook)

    def callback_worker(self):
        """ Callback thread: call subscribers """
        while self.running:
            try:
                data = self.sync_queue.get(timeout=self.queue_get_timeout)
                #
                self.process_data(data)
            except queue.Empty:
                pass
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception("Error during event processing, skipping")
        #
        log.debug("Callback worker thread exiting")

    def callback_spawner(self):
        """ Callback thread: call subscribers in separate thread

        Most events are dispatched on their own thread so a slow/blocking
        handler cannot stall the whole bus.  Events registered via
        register_inline_dispatch_event() are instead dispatched inline, in
        this single drain thread, which preserves strict FIFO delivery order
        for their (non-blocking) handlers.  This matters for stream events:
        under native OS threads the per-event spawn model gives no ordering
        guarantee, so a "stream_end" could overtake a preceding
        "stream_chunk" and truncate the stream.
        """
        while self.running:
            try:
                data = self.sync_queue.get(timeout=self.queue_get_timeout)
                #
                # When nothing is registered for inline dispatch, keep the
                # original cheap fast path: spawn without decoding here.
                if not self.inline_dispatch_events:
                    spawned_thread = threading.Thread(
                        target=self.process_data,
                        args=[data],
                        daemon=True,
                    )
                    spawned_thread.start()
                    continue
                #
                # Otherwise decode once to route: inline-dispatch events run
                # synchronously (preserving FIFO order), the rest are spawned.
                event = self.decode_data(data)
                #
                if event is None:
                    continue
                #
                with self.event_lock:
                    inline = event.get("name") in self.inline_dispatch_events
                #
                if inline:
                    self.process_event(event)
                else:
                    spawned_thread = threading.Thread(
                        target=self.process_event,
                        args=[event],
                        daemon=True,
                    )
                    spawned_thread.start()
            except queue.Empty:
                pass
            except:  # pylint: disable=W0702
                if self.log_errors:
                    log.exception("Error during event processing, skipping")
        #
        log.debug("Callback worker thread exiting")

    def process_data(self, data):
        """ Process: decode raw event data and call subscribers """
        event = self.decode_data(data)
        #
        if event is None:
            return
        #
        self.process_event(event)

    def process_event(self, event):  # pylint: disable=R0912
        """ Process: call subscribers for an already-decoded event """
        try:
            event_name = event.get("name")
            event_payload = event.get("payload")
            #
            with self.event_lock:
                callbacks = self.catch_all_callbacks.copy()
                if event_name in self.event_callbacks:
                    callbacks.extend(self.event_callbacks[event_name])
            #
            for callback in callbacks:
                for hook in hooks.before_callback_hooks + self.before_callback_hooks:
                    try:
                        hook(callback, event_name, event_payload)
                    except:  # pylint: disable=W0702
                        if self.log_errors:
                            log.exception("Before callback hook failed, skipping")
                #
                try:
                    callback_result = callback(event_name, event_payload)
                except:  # pylint: disable=W0702
                    if self.log_errors:
                        log.exception("Event callback failed, skipping")
                    #
                    callback_result = None  # FIXME: pass exceptions to after_callback_hooks?
                #
                for hook in hooks.after_callback_hooks + self.after_callback_hooks:
                    try:
                        hook(callback, callback_result, event_name, event_payload)
                    except:  # pylint: disable=W0702
                        if self.log_errors:
                            log.exception("After callback hook failed, skipping")
        except:  # pylint: disable=W0702
            if self.log_errors:
                log.exception("Error during event processing, skipping")
