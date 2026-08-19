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

import time
import threading

import pytest  # pylint: disable=E0401

from arbiter.eventnode.base import EventNodeBase, EventNodeStartTimeout
from arbiter.eventnode.redis import RedisEventNode
from arbiter.eventnode.zeromq import ZeroMQEventNode
from arbiter.eventnode.mock import MockEventNode


class WedgedListenerNode(EventNodeBase):
    """ Listener that blocks before signalling readiness.

    Stands in for the live failure: the listener is stuck inside subscribe()
    (glibc resolver, unreachable Redis, ...) so listening_ready_event is never set.
    """

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
                self.record_worker_error()
                time.sleep(0.05)


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

    @pytest.mark.parametrize("node", [
        RedisEventNode(host="127.0.0.1"),
        MockEventNode(),
    ])
    def test_other_backends_are_bounded(self, node):
        assert node.start_max_wait == 60.0
        assert node.clone_config["start_max_wait"] == 60.0

    def test_clone_keeps_the_configured_timeout(self):
        node = RedisEventNode(host="127.0.0.1", start_max_wait=7.5)
        #
        assert node.clone().start_max_wait == 7.5


class StubPubSub:
    def __init__(self, block_event):
        self.block_event = block_event

    def subscribe(self, *args, **kwargs):
        _ = args, kwargs
        self.block_event.wait()

    def listen(self):
        return []

    def close(self):
        pass


class StubRedis:
    """ Redis stand-in whose subscribe() blocks, as getaddrinfo did in the live case """

    def __init__(self, block_event):
        self.block_event = block_event
        self.closed = False

    def pubsub(self, *args, **kwargs):
        _ = args, kwargs
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
        node._get_connection_and_pool = lambda: (stub_redis, stub_pool)  # pylint: disable=W0212
        #
        with pytest.raises(EventNodeStartTimeout):
            node.start()
        #
        assert node.started is False
        assert stub_redis.closed is True
        assert stub_pool.closed is True
        #
        block_event.set()
        node.stop()  # must not blow up after a failed start
