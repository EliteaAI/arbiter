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

import time
import queue
import threading
import struct

from arbiter import log

from .base import EventNodeBase
from ..tools.pylon import is_runtime_gevent


class ZeroMQEventNode(EventNodeBase):  # pylint: disable=R0902
    """ Event node (ZMQ) - allows to subscribe to events and to emit new events """

    socket_poll_timeout = 1000

    def __init__(
            self, connect_sub, connect_push, topic="events", topic_format="[{}]",
            hmac_key=None, hmac_digest="sha512", callback_workers=1,
            mute_first_failed_connections=0,
            log_errors=True,
            retry_interval=3.0,
            #
            shutdown_send_timeout=15.0,
            #
            join_threads_on_stop=True,
            shutdown_in_thread=False,
            shutdown_join_timeout=5.0,
            shutdown_via_destroy=False,
            #
            sockopt_linger=1000,
            #
            sockopt_tcp_keepalive=None,
            sockopt_tcp_keepalive_cnt=None,
            sockopt_tcp_keepalive_idle=None,
            sockopt_tcp_keepalive_intvl=None,
            #
            sockopt_immediate=None,
            #
            sockopt_heartbeat_ivl=None,
            sockopt_heartbeat_ttl=None,
            sockopt_heartbeat_timeout=None,
            #
            connection_wait_interval=5.0,
    ):  # pylint: disable=R0913,R0914
        super().__init__(
            hmac_key, hmac_digest, callback_workers, log_errors,
            use_emit_queue=True,
        )
        #
        self.clone_config = {
            "type": "ZeroMQEventNode",
            "connect_sub": connect_sub,
            "connect_push": connect_push,
            "topic": topic,
            "topic_format": topic_format,
            "hmac_key": hmac_key,
            "hmac_digest": hmac_digest,
            "callback_workers": callback_workers,
            "mute_first_failed_connections": mute_first_failed_connections,
            "log_errors": log_errors,
            "retry_interval": retry_interval,
            "shutdown_send_timeout": shutdown_send_timeout,
            "join_threads_on_stop": join_threads_on_stop,
            "shutdown_in_thread": shutdown_in_thread,
            "shutdown_join_timeout": shutdown_join_timeout,
            "shutdown_via_destroy": shutdown_via_destroy,
            "sockopt_linger": sockopt_linger,
            "sockopt_tcp_keepalive": sockopt_tcp_keepalive,
            "sockopt_tcp_keepalive_cnt": sockopt_tcp_keepalive_cnt,
            "sockopt_tcp_keepalive_idle": sockopt_tcp_keepalive_idle,
            "sockopt_tcp_keepalive_intvl": sockopt_tcp_keepalive_intvl,
            "sockopt_immediate": sockopt_immediate,
            "sockopt_heartbeat_ivl": sockopt_heartbeat_ivl,
            "sockopt_heartbeat_ttl": sockopt_heartbeat_ttl,
            "sockopt_heartbeat_timeout": sockopt_heartbeat_timeout,
            "connection_wait_interval": connection_wait_interval,
        }
        #
        self.retry_interval = retry_interval
        self.mute_first_failed_connections = mute_first_failed_connections
        self.failed_connections = 0
        #
        self.zmq_gevent = is_runtime_gevent()
        #
        self.shutdown_send_timeout = shutdown_send_timeout
        self.join_threads_on_stop = join_threads_on_stop
        self.shutdown_in_thread = shutdown_in_thread
        self.shutdown_join_timeout = shutdown_join_timeout
        self.shutdown_via_destroy = shutdown_via_destroy or self.zmq_gevent
        #
        self.sockopt_linger = sockopt_linger
        #
        self.sockopt_tcp_keepalive = sockopt_tcp_keepalive
        self.sockopt_tcp_keepalive_cnt = sockopt_tcp_keepalive_cnt
        self.sockopt_tcp_keepalive_idle = sockopt_tcp_keepalive_idle
        self.sockopt_tcp_keepalive_intvl = sockopt_tcp_keepalive_intvl
        #
        self.sockopt_immediate = sockopt_immediate
        #
        self.sockopt_heartbeat_ivl = sockopt_heartbeat_ivl
        self.sockopt_heartbeat_ttl = sockopt_heartbeat_ttl
        self.sockopt_heartbeat_timeout = sockopt_heartbeat_timeout
        #
        self.connection_wait_interval = connection_wait_interval
        #
        self.zeromq_connect_sub = connect_sub
        self.zeromq_connect_push = connect_push
        #
        self.zeromq_topic = topic_format.format(topic).encode("utf-8")
        #
        self.zmq_ctx = None
        self.pending_emit_count = 0
        self.pending_emit_lock = threading.Lock()

    def start(self, emit_only=False):
        """ Start event node """
        if self.started:
            return
        #
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        self.zmq_ctx = zmq.Context()
        #
        super().start(emit_only)

    def stop(self):
        """ Stop event node """
        self.can_emit = False
        super().stop()
        #
        if self.started:
            log.debug("Stop initiated")
            #
            if self.shutdown_in_thread:
                shutdown_thread = threading.Thread(target=self.shutdown, daemon=True)
                shutdown_thread.start()
                shutdown_thread.join(timeout=self._get_shutdown_wait_timeout())
            else:
                self.shutdown()
            #
            # FIXME: should set started to false?

    def shutdown(self):
        """ Perform stop actions """
        join_timeout = self._get_shutdown_wait_timeout()
        #
        for emitting_thread in self.emitting_threads:
            if emitting_thread.is_alive():
                emitting_thread.join(timeout=join_timeout)
        #
        if self.join_threads_on_stop:
            if self.listening_thread.is_alive():
                self.listening_thread.join(timeout=join_timeout)
            #
            for callback_thread in self.callback_threads:
                if callback_thread.is_alive():
                    callback_thread.join(timeout=join_timeout)
        #
        if self.shutdown_via_destroy:
            self.zmq_ctx.destroy(self._get_shutdown_linger())
        else:
            self.zmq_ctx.term()
        #
        if self.pending_emit_count > 0:
            log.warning(
                "ZeroMQ event node stopped with %s pending messages unsent",
                self.pending_emit_count,
            )

    def _get_shutdown_linger(self):
        if self.sockopt_linger is None:
            return None
        #
        if self.shutdown_send_timeout is None:
            return self.sockopt_linger
        #
        return max(self.sockopt_linger, int(self.shutdown_send_timeout * 1000))

    def _get_shutdown_wait_timeout(self):
        shutdown_linger = self._get_shutdown_linger()
        linger_timeout = 0.0
        #
        if shutdown_linger is not None:
            linger_timeout = (shutdown_linger * 1.5) / 1000.0
        #
        if self.shutdown_send_timeout is None:
            return max(self.shutdown_join_timeout, linger_timeout)
        #
        return max(self.shutdown_join_timeout, self.shutdown_send_timeout, linger_timeout)

    def _add_pending_emit(self):
        with self.pending_emit_lock:
            self.pending_emit_count += 1

    def _finish_pending_emit(self):
        with self.pending_emit_lock:
            if self.pending_emit_count > 0:
                self.pending_emit_count -= 1

    def _has_pending_emits(self):
        with self.pending_emit_lock:
            return self.pending_emit_count > 0

    def emit(self, event_name, payload=None):
        """ Emit event with payload data """
        if not self.can_emit:
            return
        #
        data = self.make_event_data(event_name, payload)
        self._add_pending_emit()
        self.emit_queue.put(data)

    def _set_sockopts(self, zmq, zmq_socket):
        if self.sockopt_linger is not None:
            zmq_socket.setsockopt(zmq.LINGER, self.sockopt_linger)
        #
        if self.sockopt_tcp_keepalive is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE, self.sockopt_tcp_keepalive)
        if self.sockopt_tcp_keepalive_cnt is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, self.sockopt_tcp_keepalive_cnt)
        if self.sockopt_tcp_keepalive_idle is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, self.sockopt_tcp_keepalive_idle)
        if self.sockopt_tcp_keepalive_intvl is not None:
            zmq_socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, self.sockopt_tcp_keepalive_intvl)
        #
        if self.sockopt_immediate is not None:
            zmq_socket.setsockopt(zmq.IMMEDIATE, self.sockopt_immediate)
        #
        if self.sockopt_heartbeat_ivl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_IVL, self.sockopt_heartbeat_ivl)
        if self.sockopt_heartbeat_ttl is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TTL, self.sockopt_heartbeat_ttl)
        if self.sockopt_heartbeat_timeout is not None:
            zmq_socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, self.sockopt_heartbeat_timeout)

    def emitting_worker(self):
        """ Emitting thread: emit event data from emit_queue """
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        zmq_socket_push = self.zmq_ctx.socket(zmq.PUSH)  # pylint: disable=E1101
        self._set_sockopts(zmq, zmq_socket_push)
        zmq_socket_push.connect(self.zeromq_connect_push)
        #
        zmq_socket_monitor = zmq_socket_push.get_monitor_socket()
        zmq_monitor_thread = ZeroMQMonitorThread(
            self, zmq_socket_monitor, self.emitting_ready_event
        )
        zmq_monitor_thread.start()
        #
        while self.running:
            wait_result = self.emitting_ready_event.wait(self.connection_wait_interval)
            #
            if wait_result is True:
                break
            #
            log.warning("Emitting worker is not connected yet, waiting")
        #
        while self.running or self._has_pending_emits():
            data = None
            try:
                if self.running:
                    data = self.emit_queue.get(timeout=self.queue_get_timeout)
                else:
                    data = self.emit_queue.get_nowait()
                #
                zmq_socket_push.send_multipart([self.zeromq_topic, data])
            except queue.Empty:
                pass
            except:  # pylint: disable=W0702
                if self.running and self.log_errors:
                    log.exception("Error during event emitting, skipping")
            finally:
                if data is not None:
                    self._finish_pending_emit()
        #
        log.debug("ZeroMQ emitting thread stopping")
        #
        zmq_socket_push.close(linger=self._get_shutdown_linger())
        #
        log.debug("ZeroMQ emitting thread exiting")

    def listening_worker(self):  # pylint: disable=R0912
        """ Listening thread: push event data to sync_queue """
        if self.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401
        else:
            import zmq  # pylint: disable=C0415,E0401
        #
        zmq_socket_sub = self.zmq_ctx.socket(zmq.SUB)  # pylint: disable=E1101
        self._set_sockopts(zmq, zmq_socket_sub)
        zmq_socket_sub.connect(self.zeromq_connect_sub)
        zmq_socket_sub.subscribe(self.zeromq_topic)
        zmq_poller = zmq.Poller()  # pylint: disable=E1101
        zmq_poller.register(zmq_socket_sub, zmq.POLLIN)
        #
        zmq_socket_monitor = zmq_socket_sub.get_monitor_socket()
        zmq_monitor_thread = ZeroMQMonitorThread(
            self, zmq_socket_monitor, self.listening_ready_event
        )
        zmq_monitor_thread.start()
        #
        while self.running:
            wait_result = self.listening_ready_event.wait(self.connection_wait_interval)
            #
            if wait_result is True:
                break
            #
            log.warning("Listening worker is not connected yet, waiting")
        #
        while self.running:
            try:
                if not dict(zmq_poller.poll(timeout=self.socket_poll_timeout)).get(zmq_socket_sub):
                    continue
                #
                topic, message = zmq_socket_sub.recv_multipart(flags=zmq.NOBLOCK)  # pylint: disable=W0632
                #
                if topic != self.zeromq_topic:
                    continue
                #
                if not message:
                    continue
                #
                self.sync_queue.put(message)
            except:  # pylint: disable=W0702
                if self.running:
                    if self.log_errors:
                        log.exception(
                            "Exception in listening thread. Retrying in %s seconds",
                            self.retry_interval,
                        )
                    #
                    time.sleep(self.retry_interval)
        #
        log.debug("ZeroMQ listening thread stopping")
        #
        zmq_socket_sub.close(linger=self._get_shutdown_linger())
        #
        log.debug("ZeroMQ listening thread exiting")


class ZeroMQMonitorThread(threading.Thread):  # pylint: disable=R0903
    """ ZeroMQ: monitor """

    def __init__(self, node, monitor_socket, ready_event):
        super().__init__(daemon=True)
        #
        self.node = node
        self.monitor_socket = monitor_socket
        self.ready_event = ready_event

    def run(self):
        """ Run thread """
        if self.node.zmq_gevent:
            import zmq.green as zmq  # pylint: disable=C0415,E0401,E1101
        else:
            import zmq  # pylint: disable=C0415,E0401,E1101
        #
        import zmq.error  # pylint: disable=C0415,E0401,E1101
        from zmq.utils.monitor import recv_monitor_message  # pylint: disable=C0415,E0401,E1101
        #
        monitor_stopped = False
        #
        while self.node.running and not monitor_stopped:
            try:
                while self.monitor_socket.poll():
                    event_data = recv_monitor_message(self.monitor_socket)
                    #
                    log.debug("ZeroMQ event: %s", event_data)
                    #
                    if event_data["event"] == zmq.EVENT_MONITOR_STOPPED:
                        monitor_stopped = True
                        break
                    #
                    if event_data["event"] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                        self.ready_event.set()
            except Exception as exc:  # pylint: disable=W0718
                if not isinstance(exc, zmq.error.ContextTerminated) and self.node.log_errors:
                    log.exception("Monitor exception")
        #
        log.debug("ZeroMQ monitoring thread stopping")
        #
        self.monitor_socket.close()
        #
        log.debug("ZeroMQ monitoring thread exiting")
