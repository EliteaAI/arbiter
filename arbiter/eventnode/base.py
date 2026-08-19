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

import sys
import time
import hmac
import gzip
import queue
import pickle
import threading

from arbiter import log

from .tools import make_event_node
from . import hooks


class EventNodeStartTimeout(Exception):
    """ Raised when event node fails to become ready in time """
    #
    # Node must be discarded after this: its worker threads are spent and cannot be restarted


class EventNodeBase:  # pylint: disable=R0902
    """ Event node (base) - allows to subscribe to events and to emit new events """

    def __init__(
            self,
            hmac_key=None, hmac_digest="sha512",
            callback_workers=1,
            log_errors=True,
            use_emit_queue=False,
            emitting_workers=1,
            start_max_wait=60.0,
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
        self.queue_get_timeout = 1
        #
        # None means "wait forever": ZeroMQ mesh peers may legitimately be down at boot,
        # so there the only hang signal stays the "not connected yet" warning
        self.start_max_wait = start_max_wait
        self.worker_errors = {}
        #
        # Retry defaults for _connect_with_retry, overridden by transports that have them
        self.retry_interval = 3.0
        self.mute_first_failed_connections = 0
        self.failed_connections = 0
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
        self.worker_errors.clear()
        #
        # One deadline for the whole start(): connecting and worker readiness share it
        deadline = None if self.start_max_wait is None \
            else time.monotonic() + self.start_max_wait
        #
        try:
            self._connect_transport(deadline)
            self._start_workers(emit_only, deadline)
        except:  # pylint: disable=W0702
            self._abort_start()
            raise
        #
        self.started = True

    def _start_workers(self, emit_only, deadline):
        """ Spawn worker threads and wait until they report readiness """
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
        self._wait_until_ready("emitting", self.emitting_ready_event, deadline)
        self._wait_until_ready("listening", self.listening_ready_event, deadline)

    def _abort_start(self):
        """ Give up on this start: stop workers, stop emitting, drop the transport """
        # can_emit guards the unbounded emit_queue, which now has no consumer
        self.can_emit = False
        self.stop_event.set()
        #
        try:
            self._close_transport()
        except:  # pylint: disable=W0702
            if self.log_errors:
                log.exception("Failed to close transport on aborted start")

    def _connect_transport(self, deadline):
        """ Connect transport before workers start, for backends that need it """
        _ = deadline

    def _close_transport(self):
        """ Release transport resources, if any """

    def _connect_with_retry(self, source, connect, deadline):
        """ Retry connect() until it succeeds, the node stops or the deadline expires """
        while self.running:
            try:
                return connect()
            except:  # pylint: disable=W0702
                self.record_worker_error(source)
                #
                if self.log_errors and \
                        self.failed_connections >= self.mute_first_failed_connections:
                    log.exception(
                        "Failed to create connection. Retrying in %s seconds", self.retry_interval
                    )
                #
                self.failed_connections += 1
                #
                if self._deadline_expired(deadline):
                    raise EventNodeStartTimeout(
                        f"{type(self).__name__}: {source} did not succeed "
                        f"in {self.start_max_wait}s, cause: {self._describe_error(source)}"
                    )
                #
                self.stop_event.wait(self._retry_sleep(deadline))
        #
        # Never return a missing connection: callers would treat None as usable
        raise EventNodeStartTimeout(
            f"{type(self).__name__}: {source} gave up, node stopped before it succeeded"
        )

    def _wait_until_ready(self, worker_kind, ready_event, deadline):
        """ Wait for worker readiness, raise instead of blocking forever """
        if ready_event.wait(self._time_left(deadline)):
            return
        #
        raise EventNodeStartTimeout(
            f"{type(self).__name__}: {worker_kind} worker not ready "
            f"in {self.start_max_wait}s, cause: {self._describe_error(worker_kind)}"
        )

    def _retry_sleep(self, deadline):
        """ Retry pause, never overshooting the start deadline """
        if deadline is None:
            return self.retry_interval
        #
        return min(self.retry_interval, self._time_left(deadline))

    @staticmethod
    def _deadline_expired(deadline):
        """ Check whether the start deadline is reached """
        return deadline is not None and time.monotonic() >= deadline

    @staticmethod
    def _time_left(deadline):
        """ Seconds left until deadline, None meaning "wait forever" """
        if deadline is None:
            return None
        #
        return max(0.0, deadline - time.monotonic())

    def record_worker_error(self, source):
        """ Remember formatted error so a start() timeout can name the cause """
        exc = sys.exc_info()[1]
        #
        # Formatted, not the exception itself: its traceback pins frame locals (event payloads)
        self.worker_errors[source] = f"{type(exc).__name__}: {exc}"

    def _describe_error(self, source):
        """ Describe the error recorded for source, without attributing another one to it """
        # Snapshot: retrying workers keep recording while this formats
        errors = dict(self.worker_errors)
        #
        if source in errors:
            return errors[source]
        #
        if errors:
            others = ", ".join(f"{key}: {value}" for key, value in errors.items())
            return f"none from {source}, other errors: {others}"
        #
        return "no error recorded, worker still blocked"

    def _put_sync_data(self, data):
        """ Hand received data to callback workers, unless the node is stopping """
        # A listener that un-wedges after a failed start must not fill an undrained queue
        if not self.running:
            return
        #
        self.sync_queue.put(data)

    def stop(self):
        """ Stop event node """
        self.stop_event.set()
        #
        # FIXME: wait for threads?

    @property
    def running(self):
        """ Check if it is time to stop """
        return not self.stop_event.is_set()

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
        """ Callback thread: call subscribers in separate thread """
        while self.running:
            try:
                data = self.sync_queue.get(timeout=self.queue_get_timeout)
                #
                spawned_thread = threading.Thread(
                    target=self.process_data,
                    args=[data],
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

    def process_data(self, data):  # pylint: disable=R0912
        """ Process: call subscribers """
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
                    return
            #
            event = pickle.loads(gzip.decompress(data))
            #
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
