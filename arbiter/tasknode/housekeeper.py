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
    Task node
"""

import time
import datetime
import threading

from arbiter import log


class TaskNodeHousekeeper(threading.Thread):  # pylint: disable=R0903
    """ Perform cleanup """

    def __init__(self, node):
        super().__init__(daemon=True)
        self.node = node
        self.orphan_first_seen = {}
        self.replies_lock = threading.Lock()
        self.active_pass = None

    def run(self):
        """ Run housekeeper thread """
        while not self.node.stop_event.is_set():
            time.sleep(self.node.housekeeping_interval)
            #
            if self.node.state_reply_authority:
                try:
                    self.reverify_orphans()
                except:  # pylint: disable=W0702
                    log.exception("Orphan reverify failed, continuing")
            #
            expired = []
            #
            with self.node.lock:
                for task_id in list(self.node.state_events):
                    data = self.node.state_events[task_id]
                    #
                    if not data["event"].is_set():
                        continue
                    #
                    age = (datetime.datetime.now() - data["timestamp"]).total_seconds()
                    #
                    if age < self.node.task_retention_period:
                        continue
                    #
                    expired.append(task_id)
            #
            for task_id in expired:
                self._prune(task_id)

    #
    # Orphan reverify: retire rows nobody on the bus still owns
    #

    def reverify_orphans(self):
        """ Re-ask the bus about non-local rows that pruning can never reach """
        suspects, snapshot = self._collect_suspects()
        #
        with self.replies_lock:
            # Drop any collector a previous pass left behind, so late callbacks
            # cannot keep task-state payloads reachable once their pass is over
            self.active_pass = None
        #
        if not suspects:
            return
        #
        # Several nodes may answer one query and global_task_state keeps only the
        # last, so replies are collected as they arrive instead of read back after
        current_pass = {
            "expected": set(suspects),
            "replies": {},
            "closed": False,
        }
        #
        with self.replies_lock:
            self.active_pass = current_pass
        #
        self.node.event_node.subscribe("task_state_announce", self.on_reverify_reply)
        #
        try:
            # Emits are non-blocking to us, but the Redis transport publishes once
            # per call, so orphan_batch_limit is what actually bounds a pass
            for task_id in suspects:
                self.node.event_node.emit(
                    "task_state_query",
                    {
                        "task_id": task_id,
                        "requestor": self.node.ident,
                    }
                )
            #
            time.sleep(self.node.query_wait)
        finally:
            # Close before unsubscribing: unsubscribe is not a barrier for callbacks
            # already selected for dispatch, so they must find the pass closed
            with self.replies_lock:
                current_pass["closed"] = True
                self.active_pass = None
                replies = current_pass["replies"]
            #
            self.node.event_node.unsubscribe("task_state_announce", self.on_reverify_reply)
        #
        for task_id in suspects:
            if self._owner_replied(replies.get(task_id, [])):
                self.orphan_first_seen.pop(task_id, None)
                continue
            #
            if not self._retire(task_id, snapshot[task_id]):
                # Refused: give it a fresh grace period instead of retrying at once
                self.orphan_first_seen[task_id] = time.time()

    def on_reverify_reply(self, event_name, event_payload):
        """ Collect targeted-query answers addressed to us """
        _ = event_name
        #
        if event_payload.get("for_requestor", None) != self.node.ident:
            return
        #
        task_id = event_payload.get("task_id", None)
        #
        if task_id is None:
            return
        #
        with self.replies_lock:
            current_pass = self.active_pass
            #
            if current_pass is None or current_pass["closed"]:
                return
            #
            if task_id not in current_pass["expected"]:
                return
            #
            current_pass["replies"].setdefault(task_id, []).append(event_payload)

    def _collect_suspects(self):
        """ Non-local unfinished rows that have outlived the grace period """
        now = time.time()
        suspects = []
        snapshot = {}
        #
        with self.node.lock:
            candidates = []
            #
            for task_id, state in self.node.global_task_state.items():
                if task_id in self.node.running_tasks or task_id in self.node.local_tasks:
                    continue
                #
                status = state.get("status", "unknown")
                #
                if status == "stopped":
                    continue
                #
                candidates.append(task_id)
                snapshot[task_id] = status
        #
        for task_id in list(self.orphan_first_seen):
            if task_id not in candidates:
                self.orphan_first_seen.pop(task_id, None)
        #
        capped = False
        #
        for task_id in candidates:
            first_seen = self.orphan_first_seen.setdefault(task_id, now)
            #
            if now - first_seen < self.node.orphan_grace_period:
                continue
            #
            suspects.append(task_id)
            #
            if len(suspects) >= self.node.orphan_batch_limit:
                capped = True
                break
        #
        if capped:
            # Retired rows leave global_task_state, so the remainder moves up next
            # pass: this drains over several passes rather than starving anyone
            log.warning(
                "Orphan reverify hit batch limit: %s suspects this pass, %s candidates remain",
                len(suspects), len(candidates) - len(suspects),
            )
        #
        return suspects, snapshot

    def _owner_replied(self, replies):
        """ True if a node with authority over this task answered our query """
        for state in replies:
            # An observer echoing a row it merely copied is not proof of life:
            # only the runner counts, or the requestor while no runner is assigned
            runner = state.get("runner", None)
            announcer = state.get("announcer", None)
            #
            if runner is not None:
                if announcer == runner:
                    return True
                continue
            #
            if announcer == state.get("requestor", None):
                return True
        #
        return False

    def _drop_locked(self, task_id):
        """ Forget a task locally. Caller must hold node.lock """
        self.node.state_events.pop(task_id, None)
        self.node.global_task_state.pop(task_id, None)
        self.node.known_task_ids.discard(task_id)

    def _prune(self, task_id):
        """ Drop all local trace of a task and tell the bus """
        with self.node.lock:
            self._drop_locked(task_id)
        #
        self.orphan_first_seen.pop(task_id, None)
        self._emit_pruned(task_id)

    def _retire(self, task_id, expected_status):
        """ Drop an unclaimed row, unless it came back to life while we queried """
        with self.node.lock:
            # The query wait is long enough for this node to be handed the task,
            # and dropping the row then would strand it in local/running_tasks
            if task_id in self.node.running_tasks or task_id in self.node.local_tasks:
                log.info("Not retiring %s: task became local while querying", task_id)
                return False
            #
            state = self.node.global_task_state.get(task_id, None)
            #
            if state is None:
                return False
            #
            if state.get("status", "unknown") != expected_status:
                log.info("Not retiring %s: state changed while querying", task_id)
                return False
            #
            log.warning("Retiring orphaned task state: %s", task_id)
            self._drop_locked(task_id)
        #
        self.orphan_first_seen.pop(task_id, None)
        self._emit_pruned(task_id)
        #
        return True

    def _emit_pruned(self, task_id):
        """ Tell the bus we no longer track this task """
        self.node.event_node.emit(
            "task_status_change",
            {
                "task_id": task_id,
                "status": "pruned",
            }
        )
