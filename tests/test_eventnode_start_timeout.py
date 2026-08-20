#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0114,C0115,C0116,C0411,C0103

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

import os
import gc
import time
import weakref
import threading

import pytest  # pylint: disable=E0401

from arbiter.eventnode.base import EventNodeBase, EventNodeStartTimeout
from arbiter.eventnode.redis import RedisEventNode
from arbiter.eventnode.zeromq import ZeroMQEventNode
from arbiter.eventnode.mock import MockEventNode
from arbiter.eventnode.rabbitmq import EventNode as RabbitMQEventNode
from arbiter.eventnode.socketio import SocketIOEventNode
from arbiter.eventnode import socketio as socketio_node
from arbiter.eventnode.tools import make_event_node


class WedgedListenerNode(EventNodeBase):
    """ Listener that blocks before signalling readiness.

    Stands in for the live failure: the listener is stuck inside subscribe()
    (glibc resolver, unreachable Redis, ...) so listening_ready_event is never set.
    """

    # Short so the tests measure the timeout, not the cleanup bound behind it
    cleanup_max_wait = 0.3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.release = threading.Event()
        self.entered = threading.Event()

    def listening_worker(self):
        self.entered.set()
        self.release.wait()
        self.listening_ready_event.set()


class FailingListenerNode(EventNodeBase):
    """ Listener whose first attempt raises, like a refused connection """

    def listening_worker(self):
        while self.running:
            try:
                raise RuntimeError("subscribe blew up")
            except:  # pylint: disable=W0702
                self.record_worker_error("listening")
                time.sleep(0.05)


class SlowConnectNode(EventNodeBase):
    """ Node whose transport connect never succeeds.

    Stands in for SocketIOEventNode, which connects on the CALLING thread before any
    worker starts - a retry loop the worker-readiness bound cannot see.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.retry_interval = 0.05
        self.attempts = 0

    def _connect_transport(self, deadline):
        def connect():
            self.attempts += 1
            raise ConnectionRefusedError("broker down")
        #
        self._connect_with_retry("connect", connect, deadline)

    def listening_worker(self):
        self.listening_ready_event.set()


class FlakyConnectWedgedListenerNode(WedgedListenerNode):
    """ Transport connect fails once then succeeds, and the listener then wedges """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.retry_interval = 0.01
        self.attempts = 0

    def _connect_transport(self, deadline):
        def connect():
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("stale pool hiccup")
            #
            return object()
        #
        self._connect_with_retry("connect", connect, deadline)


class BlockingConnectNode(EventNodeBase):
    """ Node whose transport connect blocks inside a single call that never returns.

    This is the #6279 mechanism on the connect path instead of the listener path: one
    getaddrinfo() stuck behind a fork-inherited resolver lock, so a retry loop that only
    inspects the deadline between attempts never gets to inspect it at all.
    """

    cleanup_max_wait = 0.3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.release = threading.Event()
        self.entered = threading.Event()

    def _connect_transport(self, deadline):
        self.entered.set()
        self.release.wait()

    def listening_worker(self):
        self.listening_ready_event.set()


class LateConnectNode(EventNodeBase):
    """ Transport that does connect successfully, but only after the deadline passed """

    cleanup_max_wait = 0.3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.connect_delay = 0.4
        self.connection = None

    def _connect_transport(self, deadline):
        time.sleep(self.connect_delay)
        self.connection = object()

    def _close_transport(self):
        self.connection = None

    def listening_worker(self):
        self.listening_ready_event.set()


class BlockingCloseNode(WedgedListenerNode):
    """ Wedged listener whose cleanup blocks too, delaying delivery of the failure """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.close_entered = threading.Event()
        self.close_release = threading.Event()

    def _close_transport(self):
        self.close_entered.set()
        self.close_release.wait()


class BlockedCloseAndWorkerNode(BlockingCloseNode):
    """ Both cleanup steps block: close never returns and the listener never releases.

    Used to check the advertised bound, so the cleanup budget is the dominant term here
    and the settle allowance is deliberately small.
    """

    cleanup_max_wait = 1.0
    deadline_settle_wait = 0.1


class LateReadyEvent:
    """ Readiness stand-in that reports success, but only after the deadline lapsed """

    def __init__(self, delay):
        self.delay = delay

    def wait(self, timeout=None):
        _ = timeout
        time.sleep(self.delay)
        #
        return True

    def set(self):
        pass

    def is_set(self):
        return True


class LateReadyNode(EventNodeBase):
    """ Node whose listener reports readiness only after the start deadline has passed """

    cleanup_max_wait = 0.3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.listening_ready_event = LateReadyEvent(0.3)

    def listening_worker(self):
        self.listening_ready_event.set()


class LateListenerNode(EventNodeBase):
    """ Listener that only reaches sync_queue after start() already gave up """

    cleanup_max_wait = 0.3

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.release = threading.Event()
        self.closed = False

    def _close_transport(self):
        self.closed = True

    def listening_worker(self):
        self.release.wait()
        self.listening_ready_event.set()
        self._put_sync_data(b"late message")


class WedgedEmitterNode(EventNodeBase):
    """ Emit-queue node whose emitting worker never signals readiness """

    def __init__(self, **kwargs):
        super().__init__(use_emit_queue=True, **kwargs)

    def emitting_worker(self):
        while self.running:
            time.sleep(0.05)

    def listening_worker(self):
        self.listening_ready_event.set()


def _timed_start(node, emit_only=False):
    started_at = time.time()
    with pytest.raises(EventNodeStartTimeout) as excinfo:
        node.start(emit_only)
    return time.time() - started_at, str(excinfo.value)


class TestStartTimeout:
    def test_wedged_listener_raises_instead_of_hanging(self):
        node = WedgedListenerNode(start_max_wait=0.5)
        #
        elapsed, message = _timed_start(node)
        #
        assert 0.5 <= elapsed < 5
        assert "listening worker not ready" in message
        assert "worker still blocked" in message  # nothing raised, it just never returned
        assert node.started is False
        # already-spawned daemon workers must be told to leave their loops
        assert node.running is False
        #
        node.release.set()
        node.stop()

    def test_wedged_emitting_worker_raises_too(self):
        node = WedgedEmitterNode(start_max_wait=0.5)
        #
        _, message = _timed_start(node)
        #
        assert "emitting worker not ready" in message
        assert node.started is False

    def test_timeout_message_names_the_underlying_error(self):
        node = FailingListenerNode(start_max_wait=0.5)
        #
        _, message = _timed_start(node)
        #
        assert "subscribe blew up" in message

    def test_unbounded_wait_keeps_legacy_behaviour(self):
        node = WedgedListenerNode(start_max_wait=None)
        #
        starter = threading.Thread(target=node.start, daemon=True)
        starter.start()
        #
        assert node.entered.wait(timeout=5) is True
        time.sleep(0.5)
        assert starter.is_alive() is True  # still waiting, no timeout
        #
        node.release.set()
        starter.join(timeout=5)
        #
        assert starter.is_alive() is False
        assert node.started is True
        #
        node.stop()

    def test_healthy_start_is_unaffected(self):
        node = WedgedListenerNode(start_max_wait=5)
        node.release.set()
        #
        node.start()
        #
        assert node.started is True
        #
        node.start()  # second call is still a no-op
        assert node.started is True
        #
        node.stop()

    def test_emit_only_start_does_not_wait_for_listener(self):
        node = WedgedListenerNode(start_max_wait=0.5)
        #
        node.start(emit_only=True)
        #
        assert node.started is True
        assert node.entered.is_set() is False
        #
        node.stop()


class TestLateReadiness:
    """ Readiness that arrives after the deadline is a failed start, not a healthy one """

    def test_readiness_after_the_deadline_is_rejected(self):
        node = LateReadyNode(start_max_wait=0.1)
        #
        _, message = _timed_start(node)
        #
        assert "became ready only after" in message
        assert node.started is False
        assert node.can_emit is False

    def test_already_set_event_is_still_checked_against_the_deadline(self):
        # Event.wait() returns true immediately for an event that is already set, so the
        # deadline has to be re-checked afterwards - the same rule the connect path follows
        node = MockEventNode(start_max_wait=0.1)
        ready_event = threading.Event()
        ready_event.set()
        #
        with pytest.raises(EventNodeStartTimeout) as excinfo:
            node._wait_until_ready(  # pylint: disable=W0212
                "listening", ready_event, time.monotonic() - 0.01,
            )
        #
        assert "became ready only after" in str(excinfo.value)


class TestAbortedStartCleanup:
    def test_transport_connect_loop_is_bounded_too(self):
        # Regression: only worker readiness was bounded, a connect retry loop on the
        # calling thread (SocketIO) still hung the caller forever
        node = SlowConnectNode(start_max_wait=0.4)
        #
        elapsed, message = _timed_start(node)
        #
        assert elapsed < 5
        assert "connect did not succeed" in message
        assert "ConnectionRefusedError: broker down" in message
        assert node.attempts > 1  # it did retry, it did not fail on the first attempt

    def test_recorded_cause_is_not_the_exception_object(self):
        # Holding the exception would pin its traceback -> frame locals -> event payload
        node = FailingListenerNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        assert isinstance(node.worker_errors["listening"], str)

    def test_connect_error_is_not_blamed_on_the_listener(self):
        # A transient connect error must not be reported as the listener's cause
        node = FlakyConnectWedgedListenerNode(start_max_wait=0.3)
        #
        _, message = _timed_start(node)
        #
        assert node.attempts == 2  # connect recovered, so its error is stale
        #
        assert "listening worker not ready" in message
        assert "none from listening" in message  # named as someone else's error
        assert "stale pool hiccup" in message  # still surfaced, just not misattributed
        #
        node.release.set()

    def test_aborted_start_stops_emitting_and_closes_transport(self):
        node = LateListenerNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        assert node.can_emit is False  # emit_queue has no consumer left
        assert node.closed is True
        #
        node.emit("some_event", {"payload": "dropped"})
        assert node.sync_queue.empty() is True

    def test_timed_out_node_must_be_discarded(self):
        # Documented contract on EventNodeStartTimeout: worker threads are spent, so a
        # retry on the same object cannot work. Pinned here so it stays explicit.
        node = WedgedListenerNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        with pytest.raises(RuntimeError):
            node.start()
        #
        node.release.set()

    def test_stop_during_connect_raises_instead_of_yielding_no_connection(self):
        # A stop() landing while connect is still retrying must not hand back a "connected"
        # node with no connection - the first emit would die on None
        node = SlowConnectNode(start_max_wait=5)
        node.stop_event.set()
        #
        with pytest.raises(EventNodeStartTimeout, match="gave up"):
            node._connect_transport(None)  # pylint: disable=W0212

    def test_late_listener_does_not_fill_an_undrained_queue(self):
        node = LateListenerNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        # listener un-wedges only now, after start() gave up and callback workers exited
        node.release.set()
        time.sleep(0.2)
        #
        assert node.sync_queue.empty() is True


class TestBlockingOperationsAreBounded:
    """ A single blocking call must not outlast the deadline it was given """

    def test_blocking_connect_does_not_outlast_the_deadline(self):
        node = BlockingConnectNode(start_max_wait=0.4)
        #
        elapsed, message = _timed_start(node)
        #
        assert node.entered.is_set() is True
        # nothing releases connect here: only the deadline can have ended the wait
        assert elapsed < 3
        assert "connect did not return" in message
        assert node.started is False
        #
        node.release.set()

    def test_connection_arriving_after_the_deadline_is_released(self):
        # It connects successfully, just too late. The late connection must not be left
        # dangling: no later stop() knows about it, since start() already failed.
        node = LateConnectNode(start_max_wait=0.1)
        #
        _timed_start(node)
        #
        assert node.started is False
        #
        # connect completes on its own thread only now
        time.sleep(node.connect_delay + 0.3)
        assert node.connection is None

    def test_blocking_cleanup_does_not_delay_the_failure(self):
        node = BlockingCloseNode(start_max_wait=0.3)
        #
        elapsed, message = _timed_start(node)
        #
        assert node.close_entered.is_set() is True
        # neither the listener nor the cleanup is released before this assertion
        assert elapsed < 3
        assert "listening worker not ready" in message
        #
        node.close_release.set()
        node.release.set()

    def test_cleanup_spends_one_budget_and_respects_the_stated_bound(self):
        # Both cleanup steps block, which is what makes the budgeting visible: close and the
        # worker join have to share cleanup_max_wait, not take one each
        node = BlockedCloseAndWorkerNode(start_max_wait=0.4)
        #
        elapsed, message = _timed_start(node)
        #
        assert node.close_entered.is_set() is True
        assert node.listening_thread.is_alive() is True  # neither step could complete
        assert "listening worker not ready" in message
        #
        bound = node.start_max_wait + node.deadline_settle_wait + node.cleanup_max_wait
        #
        assert elapsed >= node.start_max_wait
        assert elapsed <= bound + 0.3  # slack for scheduling, not for a second budget
        #
        node.close_release.set()
        node.release.set()

    def test_unbounded_start_still_connects_on_the_calling_thread(self):
        # start_max_wait=None keeps the legacy path: no helper thread, no changed
        # thread affinity for transports that care (ZeroMQ contexts, pika connections)
        node = LateConnectNode(start_max_wait=None)
        node.connect_delay = 0
        connect_thread = []
        #
        original = node._connect_transport  # pylint: disable=W0212
        #
        def record(deadline):
            connect_thread.append(threading.current_thread())
            original(deadline)
        #
        node._connect_transport = record  # pylint: disable=W0212
        node.start()
        #
        assert connect_thread == [threading.current_thread()]
        #
        node.stop()


class TestWorkerCleanup:
    """ A failed start must not leave workers holding the application graph """

    def test_workers_terminate_within_the_cleanup_interval(self):
        # Whenever the blocking primitive is releasable, cleanup must actually reap them
        node = WedgedEmitterNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        for thread in [node.listening_thread] + node.callback_threads + node.emitting_threads:
            assert thread.is_alive() is False

    def test_discarded_failed_node_becomes_collectible(self):
        node = WedgedEmitterNode(start_max_wait=0.3)
        #
        _timed_start(node)
        #
        node_ref = weakref.ref(node)
        del node
        gc.collect()
        #
        assert node_ref() is None

    def test_stuck_worker_does_not_pin_pre_start_subscribers(self):
        # The listener here stays wedged forever, so the node itself cannot be collected.
        # What must still be collectible is everything the application handed it.
        class Subscriber:  # pylint: disable=R0903
            def __init__(self):
                self.payload = bytearray(1024 * 1024)

            def on_event(self, event_name, payload):
                _ = event_name, payload
        #
        subscriber = Subscriber()
        subscriber_ref = weakref.ref(subscriber)
        #
        node = WedgedListenerNode(start_max_wait=0.3)
        node.subscribe("some_event", subscriber.on_event)
        node.subscribe(..., subscriber.on_event)
        node.sync_queue.put(b"payload nobody will consume")
        #
        _timed_start(node)
        #
        assert node.listening_thread.is_alive() is True  # still wedged, as in the live case
        #
        del subscriber
        gc.collect()
        #
        assert subscriber_ref() is None
        assert node.sync_queue.empty() is True
        #
        node.release.set()

    def test_repeated_releasable_failed_starts_do_not_accumulate_threads(self):
        # Scoped to releasable failures on purpose: this emitter polls running and leaves,
        # so cleanup really does reap it. See the irrecoverable case below for the limit.
        baseline = threading.active_count()
        #
        for _ in range(3):
            node = WedgedEmitterNode(start_max_wait=0.3)
            _timed_start(node)
            del node
        #
        gc.collect()
        #
        assert threading.active_count() <= baseline + 1

    def test_repeated_irrecoverable_starts_keep_application_state_collectible(self):
        # An unkillable native call leaves one abandoned daemon thread per failed node - the
        # documented limit of this mitigation. What must not grow is application state, so
        # the sentinels have to be collectible however many times the start fails.
        class Sentinel:  # pylint: disable=R0903
            def __init__(self):
                self.payload = bytearray(1024 * 1024)

            def on_event(self, event_name, payload):
                _ = event_name, payload
        #
        baseline = threading.active_count()
        attempts = 3
        refs, nodes = [], []
        #
        for _ in range(attempts):
            node = BlockingConnectNode(start_max_wait=0.2)
            sentinel = Sentinel()
            #
            refs.append(weakref.ref(sentinel))
            node.subscribe(..., sentinel.on_event)
            node.sync_queue.put(b"payload nobody will consume")
            #
            _timed_start(node)
            #
            del sentinel
            nodes.append(node)  # kept on purpose: a wedged node cannot be collected
        #
        gc.collect()
        #
        assert [ref() for ref in refs] == [None] * attempts
        assert all(node.sync_queue.empty() for node in nodes)
        assert all(not node.catch_all_callbacks for node in nodes)
        #
        # One abandoned helper thread per wedged call, and nothing beyond that
        assert threading.active_count() <= baseline + attempts
        #
        for node in nodes:
            node.release.set()


BOUNDED_BACKENDS = [
    lambda timeout: RedisEventNode(host="127.0.0.1", start_max_wait=timeout),
    lambda timeout: MockEventNode(start_max_wait=timeout),
    lambda timeout: RabbitMQEventNode(
        host="127.0.0.1", port=5672, user="u", password="p", start_max_wait=timeout,
    ),
    lambda timeout: SocketIOEventNode(
        url="http://127.0.0.1", password="p", start_max_wait=timeout,
    ),
]

BOUNDED_BACKEND_IDS = ["redis", "mock", "rabbitmq", "socketio"]


class TestBackendDefaults:
    def test_zeromq_stays_unbounded(self):
        # The mesh connects to another pylon that may not be up yet
        node = ZeroMQEventNode(
            connect_sub="tcp://127.0.0.1:5010",
            connect_push="tcp://127.0.0.1:5011",
            topic="mesh",
        )
        #
        assert node.start_max_wait is None
        assert node.clone_config["start_max_wait"] is None

    @pytest.mark.parametrize("factory", BOUNDED_BACKENDS, ids=BOUNDED_BACKEND_IDS)
    def test_other_backends_are_bounded(self, factory):
        node = factory(60.0)
        #
        assert node.start_max_wait == 60.0
        assert node.clone_config["start_max_wait"] == 60.0

    @pytest.mark.parametrize("factory", BOUNDED_BACKENDS, ids=BOUNDED_BACKEND_IDS)
    def test_clone_keeps_the_configured_timeout(self, factory):
        node = factory(7.5)
        #
        assert node.clone().start_max_wait == 7.5


class TestEnvironmentConfig:
    @staticmethod
    def _clean_env(monkeypatch, value):
        for key in list(os.environ):
            if key.startswith("EVENTNODE_"):
                monkeypatch.delenv(key)
        #
        monkeypatch.setenv("EVENTNODE_TYPE", "MockEventNode")
        monkeypatch.setenv("EVENTNODE_START_MAX_WAIT", value)

    def test_finite_timeout_is_parsed(self, monkeypatch):
        self._clean_env(monkeypatch, "12.5")
        #
        assert make_event_node().start_max_wait == 12.5

    def test_none_keeps_the_unbounded_wait(self, monkeypatch):
        self._clean_env(monkeypatch, "None")
        #
        assert make_event_node().start_max_wait is None


class StubPubSub:
    def __init__(self, block_event, listen_block=None):
        self.block_event = block_event
        self.listen_block = listen_block
        self.listen_entered = threading.Event()
        self.closed = False
        self.close_calls = 0

    def subscribe(self, *args, **kwargs):
        _ = args, kwargs
        self.block_event.wait()

    def listen(self):
        self.listen_entered.set()
        #
        # Blocks like a real subscription would: only close() gets us out of here
        if self.listen_block is not None:
            self.listen_block.wait()
        #
        return []

    def close(self):
        self.closed = True
        self.close_calls += 1
        #
        # Closing is what releases a blocked listen(), as with a real subscription
        if self.listen_block is not None:
            self.listen_block.set()


def _pause_handoff(node, at_handoff, resume):
    """ Park the listening worker in the window between its blocking call and the handoff """
    publish = node._publish_listening_resource  # pylint: disable=W0212
    #
    def paused_publish(resource):
        at_handoff.set()
        resume.wait(5)
        #
        return publish(resource)
    #
    node._publish_listening_resource = paused_publish  # pylint: disable=W0212


class StubRedis:
    """ Redis stand-in whose subscribe() blocks, as getaddrinfo did in the live case """

    def __init__(self, block_event, pubsub=None):
        self.block_event = block_event
        self.stub_pubsub = pubsub
        self.closed = False

    def pubsub(self, *args, **kwargs):
        _ = args, kwargs
        #
        if self.stub_pubsub is not None:
            return self.stub_pubsub
        #
        return StubPubSub(self.block_event)

    def close(self):
        self.closed = True


class StubPool:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestRedisNode:
    def test_blocked_subscribe_fails_start_and_releases_connection(self):
        block_event = threading.Event()
        stub_redis, stub_pool = StubRedis(block_event), StubPool()
        #
        node = RedisEventNode(host="127.0.0.1", start_max_wait=0.5)
        node.cleanup_max_wait = 0.3
        node._get_connection_and_pool = \
            lambda deadline=None: (stub_redis, stub_pool)  # pylint: disable=W0212
        #
        with pytest.raises(EventNodeStartTimeout):
            node.start()
        #
        assert node.started is False
        assert stub_redis.closed is True
        assert stub_pool.closed is True
        # cleared, so stop() below cannot close them a second time
        assert node.redis is None
        assert node.redis_pool is None
        #
        block_event.set()
        node.stop()  # must not blow up after a failed start

    def test_released_subscribe_does_not_enter_listen(self):
        # subscribe() is what blocked in the live case. When it finally returns, start() has
        # already failed, so the worker must not go on to block in listen() as well
        block_event = threading.Event()
        stub_pubsub = StubPubSub(block_event, listen_block=threading.Event())
        stub_redis, stub_pool = StubRedis(block_event, pubsub=stub_pubsub), StubPool()
        #
        node = RedisEventNode(host="127.0.0.1", start_max_wait=0.4)
        node.cleanup_max_wait = 0.3
        node._get_connection_and_pool = \
            lambda deadline=None: (stub_redis, stub_pool)  # pylint: disable=W0212
        #
        with pytest.raises(EventNodeStartTimeout):
            node.start()
        #
        block_event.set()
        node.listening_thread.join(timeout=5)
        #
        assert node.listening_thread.is_alive() is False
        assert stub_pubsub.listen_entered.is_set() is False
        assert stub_pubsub.closed is True

    def test_abort_during_the_handoff_window_keeps_the_listener_out_of_listen(self):
        # The interleaving that made publish-after-check racy: the worker sits between a
        # returned subscribe() and handing the subscription over while abort runs to the end.
        # Either abort closes it or the worker is refused - it must never reach listen().
        subscribe_returns = threading.Event()
        subscribe_returns.set()
        #
        stub_pubsub = StubPubSub(subscribe_returns, listen_block=threading.Event())
        stub_redis, stub_pool = StubRedis(subscribe_returns, pubsub=stub_pubsub), StubPool()
        #
        node = RedisEventNode(host="127.0.0.1", start_max_wait=0.4)
        node.cleanup_max_wait = 0.3
        node._get_connection_and_pool = \
            lambda deadline=None: (stub_redis, stub_pool)  # pylint: disable=W0212
        #
        at_handoff, resume = threading.Event(), threading.Event()
        _pause_handoff(node, at_handoff, resume)
        #
        with pytest.raises(EventNodeStartTimeout):
            node.start()
        #
        assert at_handoff.is_set() is True  # the worker is parked inside the window
        #
        resume.set()
        node.listening_thread.join(timeout=5)
        #
        assert node.listening_thread.is_alive() is False
        assert stub_pubsub.listen_entered.is_set() is False
        assert stub_pubsub.close_calls == 1  # closed once, by the side that kept it

    def test_stop_closes_the_published_subscription_exactly_once(self):
        # The accepted half of the handoff: the subscription is live, so cleanup is the side
        # that closes it and the worker must not close it a second time on its way out
        subscribe_returns = threading.Event()
        subscribe_returns.set()
        #
        stub_pubsub = StubPubSub(subscribe_returns, listen_block=threading.Event())
        stub_redis, stub_pool = StubRedis(subscribe_returns, pubsub=stub_pubsub), StubPool()
        #
        node = RedisEventNode(host="127.0.0.1", start_max_wait=5.0)
        node._get_connection_and_pool = \
            lambda deadline=None: (stub_redis, stub_pool)  # pylint: disable=W0212
        #
        node.start()
        #
        assert node.started is True
        assert stub_pubsub.listen_entered.wait(5) is True
        #
        node.stop()
        node.stop()  # must not close the subscription again
        #
        node.listening_thread.join(timeout=5)
        #
        assert node.listening_thread.is_alive() is False
        assert stub_pubsub.close_calls == 1


class StubPikaChannel:
    def __init__(self, consume_block):
        self.consume_block = consume_block
        self.consume_entered = threading.Event()
        self.stop_calls = 0

    class _DeclareResult:  # pylint: disable=R0903
        class method:  # pylint: disable=C0103,R0903
            queue = "stub-queue"

    def queue_declare(self, *args, **kwargs):
        _ = args, kwargs
        #
        return self._DeclareResult()

    def queue_bind(self, *args, **kwargs):
        _ = args, kwargs

    def basic_consume(self, *args, **kwargs):
        _ = args, kwargs

    def start_consuming(self):
        self.consume_entered.set()
        self.consume_block.wait()

    def stop_consuming(self):
        self.stop_calls += 1
        self.consume_block.set()


class StubPikaConnection:
    def __init__(self, channel):
        self.stub_channel = channel
        self.callbacks = []
        self.close_calls = 0

    def add_callback_threadsafe(self, callback):
        self.callbacks.append(callback)

    def close(self):
        self.close_calls += 1


def _stubbed_rabbitmq_node(connection, channel, **kwargs):
    node = RabbitMQEventNode(host="127.0.0.1", port=5672, user="u", password="p", **kwargs)
    #
    node._get_connection = lambda deadline=None: connection  # pylint: disable=W0212
    node._get_channel = lambda conn: channel  # pylint: disable=W0212
    #
    return node


class TestRabbitMQNode:
    def test_abort_during_the_handoff_window_keeps_the_consumer_out_of_consuming(self):
        # Same race as the Redis one: cleanup must not miss a consumer that is about to
        # start, and start_consuming() blocks with no running check inside it
        channel = StubPikaChannel(threading.Event())
        connection = StubPikaConnection(channel)
        #
        node = _stubbed_rabbitmq_node(connection, channel, start_max_wait=0.4)
        node.cleanup_max_wait = 0.3
        #
        at_handoff, resume = threading.Event(), threading.Event()
        _pause_handoff(node, at_handoff, resume)
        #
        with pytest.raises(EventNodeStartTimeout):
            node.start()
        #
        assert at_handoff.is_set() is True
        #
        resume.set()
        node.listening_thread.join(timeout=5)
        #
        assert node.listening_thread.is_alive() is False
        assert channel.consume_entered.is_set() is False
        assert channel.stop_calls == 0  # nothing to stop: consuming never began
        assert connection.close_calls == 1  # released once, by the worker that owns it

    def test_stop_posts_exactly_one_consumer_stop_callback(self):
        # pika is not thread-safe, so stopping goes through the connection's own thread:
        # the callback has to be posted once, whatever the caller does
        channel = StubPikaChannel(threading.Event())
        connection = StubPikaConnection(channel)
        #
        node = _stubbed_rabbitmq_node(connection, channel, start_max_wait=5.0)
        #
        node.start()
        #
        assert node.started is True
        assert channel.consume_entered.wait(5) is True
        #
        node.stop()
        node.stop()
        #
        assert len(connection.callbacks) == 1
        #
        connection.callbacks[0]()  # pika would run this on the consumer's own thread
        node.listening_thread.join(timeout=5)
        #
        assert node.listening_thread.is_alive() is False
        assert channel.stop_calls == 1
        assert connection.close_calls == 1


class TestSocketIONode:
    def test_blocking_connect_fails_start_and_leaves_no_connection(self, monkeypatch):
        # The shipped path #6279 hit: connect() -> getaddrinfo() with no way back
        release, entered = threading.Event(), threading.Event()
        #
        class StubSioClient:  # pylint: disable=R0903
            def __init__(self, **kwargs):
                _ = kwargs
                self.disconnected = False

            def connect(self, **kwargs):
                _ = kwargs
                entered.set()
                release.wait()

            def emit(self, *args, **kwargs):
                _ = args, kwargs

            def on(self, *args, **kwargs):
                _ = args, kwargs

            def wait(self):
                pass

            def disconnect(self):
                self.disconnected = True
        #
        monkeypatch.setattr(socketio_node.socketio, "Client", StubSioClient)
        #
        node = SocketIOEventNode(url="http://127.0.0.1", password="p", start_max_wait=0.4)
        node.cleanup_max_wait = 0.3
        #
        elapsed, message = _timed_start(node)
        #
        assert entered.is_set() is True
        assert elapsed < 3  # nothing releases connect: only the deadline ended the wait
        assert "connect did not return" in message
        assert node.started is False
        assert node.sio is None
        #
        # connect returns only now, long after start() gave up: it must release the client
        release.set()
        time.sleep(0.3)
        assert node.sio is None


class TestZeroMQNode:
    @staticmethod
    def _live_monitor_threads(node):
        # Scoped to this node: other tests in the same process have monitors of their own
        return [
            thread for thread in threading.enumerate()
            if thread.name == "eventnode-zmq-monitor" and getattr(thread, "node", None) is node
        ]

    def test_finite_timeout_releases_the_context(self):
        # ZeroMQ readiness needs a real handshake, so an absent peer never sets it. Nothing
        # is listening on these ports, which is exactly the no-peer boot case.
        node = ZeroMQEventNode(
            connect_sub="tcp://127.0.0.1:1",
            connect_push="tcp://127.0.0.1:2",
            topic="mesh",
            start_max_wait=0.5,
            connection_wait_interval=0.2,
        )
        node.cleanup_max_wait = 3.0
        #
        elapsed, message = _timed_start(node)
        #
        assert elapsed < 10
        assert "worker not ready" in message
        assert node.started is False
        assert node.zmq_ctx is None  # context released, not leaked with the failed node
        #
        for thread in [node.listening_thread] + node.callback_threads + node.emitting_threads:
            assert thread.is_alive() is False
        #
        assert node._has_pending_emits() is False  # pylint: disable=W0212
        #
        # monitor threads leave once the context is gone
        deadline = time.time() + 5
        while self._live_monitor_threads(node) and time.time() < deadline:
            time.sleep(0.1)
        #
        assert self._live_monitor_threads(node) == []
        #
        node.stop()
        node.stop()  # must not term an already-destroyed context
