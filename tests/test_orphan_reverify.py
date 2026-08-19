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

import pytest  # pylint: disable=E0401,W0611

from arbiter.tasknode.tasknode import TaskNode
from arbiter.tasknode.housekeeper import TaskNodeHousekeeper
from arbiter.tasknode.watcher import TaskNodeWatcher


class RecordingEventNode:
    """ Event node stand-in: records emits, lets tests answer queries inline.

    Nothing is delivered over a real transport, so a test can decide exactly
    which peers "answer" a targeted state query and when.
    """

    def __init__(self):
        self.emitted = []
        self.subscribers = {}
        self.started = True
        self.on_emit = None

    def emit(self, event_name, payload):
        self.emitted.append((event_name, payload))
        #
        if self.on_emit is not None:
            self.on_emit(event_name, payload)

    def subscribe(self, event_name, callback):
        self.subscribers.setdefault(event_name, []).append(callback)

    def unsubscribe(self, event_name, callback):
        if callback in self.subscribers.get(event_name, []):
            self.subscribers[event_name].remove(callback)

    def deliver(self, event_name, payload):
        """ Push an event to whoever is subscribed right now """
        for callback in list(self.subscribers.get(event_name, [])):
            callback(event_name, payload)

    def emitted_names(self):
        return [name for name, _ in self.emitted]


def make_node(**kwargs):
    """ A TaskNode wired to a fake bus and never actually started """
    kwargs.setdefault("query_wait", 0.2)
    kwargs.setdefault("orphan_grace_period", 0)
    #
    event_node = RecordingEventNode()
    node = TaskNode(event_node, **kwargs)
    node.ident = "node_under_test"
    #
    return node


def _payloads_in(value):
    """ Every dict reachable one or two levels down, for retention checks """
    found = []
    #
    if isinstance(value, dict):
        found.append(value)
        #
        for item in value.values():
            found.extend(_payloads_in(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            found.extend(_payloads_in(item))
    #
    return found


def add_gossip_row(node, task_id, status="running", runner="peer_runner"):
    """ A row this node only learned about from the bus: nothing local backs it """
    node.global_task_state[task_id] = {
        "task_id": task_id,
        "requestor": "peer_requestor",
        "runner": runner,
        "status": status,
        "result": None,
        "meta": {},
    }
    node.known_task_ids.add(task_id)


class TestStateReplyAuthority:
    """ How a bulk task_state_reply is merged, with the flag off and on """

    @staticmethod
    def test_off_keeps_legacy_truncation():
        # Default behaviour must be untouched, so an existing fleet sees no change
        node = make_node()
        add_gossip_row(node, "kept")
        add_gossip_row(node, "dropped")
        #
        node.on_state_reply("task_state_reply", {
            "for_requestor": node.ident,
            "global_task_state": {
                "kept": {"task_id": "kept", "status": "running"},
            },
        })
        #
        assert "kept" in node.global_task_state
        assert "dropped" not in node.global_task_state

    @staticmethod
    def test_on_merges_instead_of_replacing():
        # One peer's answer is partial knowledge, not the whole truth
        node = make_node(state_reply_authority=True)
        add_gossip_row(node, "known_by_peer")
        add_gossip_row(node, "not_in_this_reply")
        #
        node.on_state_reply("task_state_reply", {
            "for_requestor": node.ident,
            "global_task_state": {
                "known_by_peer": {"task_id": "known_by_peer", "status": "running"},
            },
        })
        #
        assert "known_by_peer" in node.global_task_state
        assert "not_in_this_reply" in node.global_task_state


class TestOrphanReverify:
    """ Retirement of rows that merging alone would keep forever """

    @staticmethod
    def test_unclaimed_row_is_retired():
        node = make_node(state_reply_authority=True)
        add_gossip_row(node, "ghost")
        node.state_events["ghost"] = {
            "event": threading.Event(),
            "timestamp": None,
        }
        #
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        assert "ghost" not in node.global_task_state
        assert "ghost" not in node.state_events
        assert "ghost" not in node.known_task_ids
        assert ("task_status_change", {"task_id": "ghost", "status": "pruned"}) \
            in node.event_node.emitted

    @staticmethod
    def test_row_survives_when_its_runner_answers():
        node = make_node(state_reply_authority=True)
        add_gossip_row(node, "alive", runner="peer_runner")
        #
        def answer(event_name, payload):
            if event_name != "task_state_query":
                return
            #
            node.event_node.deliver("task_state_announce", {
                "task_id": payload["task_id"],
                "for_requestor": payload["requestor"],
                "runner": "peer_runner",
                "announcer": "peer_runner",
                "status": "running",
            })
        #
        node.event_node.on_emit = answer
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        assert "alive" in node.global_task_state

    @staticmethod
    def test_echo_from_a_non_owner_does_not_save_the_row():
        # Another observer repeating a row it also merely copied is not proof of life
        node = make_node(state_reply_authority=True)
        add_gossip_row(node, "echoed", runner="peer_runner")
        #
        def answer(event_name, payload):
            if event_name != "task_state_query":
                return
            #
            node.event_node.deliver("task_state_announce", {
                "task_id": payload["task_id"],
                "for_requestor": payload["requestor"],
                "runner": "peer_runner",
                "announcer": "some_other_observer",
                "status": "running",
            })
        #
        node.event_node.on_emit = answer
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        assert "echoed" not in node.global_task_state

    @staticmethod
    def test_task_handed_to_us_mid_query_is_not_retired():
        # The query wait is long enough for this node to be given the task. Dropping
        # the row then would strand it in local/running_tasks with nothing to clear it
        node = make_node(state_reply_authority=True, query_wait=0.6)
        add_gossip_row(node, "becomes_ours", status="pending", runner=None)
        #
        def assign_mid_wait():
            time.sleep(0.2)
            #
            with node.lock:
                node.local_tasks["becomes_ours"] = {"name": "t", "meta": {}}
                node.running_tasks["becomes_ours"] = {"thread": None, "result": None}
        #
        threading.Thread(target=assign_mid_wait, daemon=True).start()
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        assert "becomes_ours" in node.global_task_state
        assert "becomes_ours" in node.running_tasks

    @staticmethod
    def test_newer_state_wins_over_the_collected_snapshot():
        # An authoritative announce can land after suspects were picked. The row we
        # would retire is no longer the row we judged, so the newer state must win
        node = make_node(state_reply_authority=True)
        add_gossip_row(node, "changed", status="pending", runner=None)
        #
        def restate(event_name, payload):
            if event_name != "task_state_query":
                return
            #
            node.global_task_state["changed"]["status"] = "running"
        #
        node.event_node.on_emit = restate
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        assert "changed" in node.global_task_state
        assert node.global_task_state["changed"]["status"] == "running"

    @staticmethod
    def test_late_callback_state_is_not_retained_across_passes():
        # process_data copies the callback list and invokes it later, so unsubscribe
        # is not a barrier. A reply still in flight when the pass closes must be
        # dropped, not parked in state that a pass finding no suspects never clears
        node = make_node(state_reply_authority=True)
        housekeeper = TaskNodeHousekeeper(node)
        #
        add_gossip_row(node, "first")
        housekeeper.reverify_orphans()
        #
        late_reply = {
            "task_id": "first",
            "for_requestor": node.ident,
            "runner": "peer_runner",
            "announcer": "peer_runner",
            "status": "running",
        }
        housekeeper.on_reverify_reply("task_state_announce", late_reply)
        #
        # Nothing is a suspect now, so a pass that bails early must still leave no
        # task-state payload reachable from the housekeeper
        housekeeper.reverify_orphans()
        #
        retained = [
            value for name, value in vars(housekeeper).items()
            if name != "node" and late_reply in _payloads_in(value)
        ]
        #
        assert not retained

    @staticmethod
    def test_pass_is_bounded_by_batch_limit():
        node = make_node(state_reply_authority=True, orphan_batch_limit=2)
        #
        for index in range(5):
            add_gossip_row(node, f"ghost_{index}")
        #
        TaskNodeHousekeeper(node).reverify_orphans()
        #
        queries = [
            payload for name, payload in node.event_node.emitted
            if name == "task_state_query"
        ]
        #
        assert len(queries) == 2
        assert len(node.global_task_state) == 3


class TestWatcherResilience:
    """ Finishing a task must always free its slot """

    @staticmethod
    def test_stopped_announce_survives_a_missing_global_row():
        # Housekeeping can retire a row while the task is finishing. The watcher
        # swallows exceptions, so a raise here would leak the slot permanently
        node = make_node()
        node.local_tasks["finishing"] = {"name": "t", "meta": {"m": 1}}
        node.running_tasks["finishing"] = {"thread": None, "result": None}
        node.have_running_tasks.set()
        #
        TaskNodeWatcher(node)._announce_task_stopped("finishing", "result")  # pylint: disable=W0212
        #
        assert "finishing" not in node.local_tasks
        assert "finishing" not in node.running_tasks
        assert not node.have_running_tasks.is_set()
        #
        announces = [
            payload for name, payload in node.event_node.emitted
            if name == "task_state_announce"
        ]
        #
        assert len(announces) == 1
        assert announces[0]["status"] == "stopped"
        assert announces[0]["result"] == "result"
