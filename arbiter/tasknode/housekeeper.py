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
        self.replies = {}
        self.replies_lock = threading.Lock()

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
                    self.node.state_events.pop(task_id, None)
                    self.node.global_task_state.pop(task_id, None)
                    self.node.known_task_ids.discard(task_id)
                    #
                    self.node.event_node.emit(
                        "task_status_change",
                        {
                            "task_id": task_id,
                            "status": "pruned",
                        }
                    )

    #
    # Orphan reverify: retire rows nobody on the bus still owns
    #

    def reverify_orphans(self):
        """ Re-ask the bus about non-local rows that pruning can never reach """
        suspects = self._collect_suspects()
        #
        if not suspects:
            return
        #
        # Several nodes may answer one query and global_task_state keeps only the
        # last, so replies are collected as they arrive instead of read back after
        with self.replies_lock:
            self.replies = {}
        #
        self.node.event_node.subscribe("task_state_announce", self.on_reverify_reply)
        #
        try:
            # Emits are non-blocking, so one wait covers the whole batch
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
            self.node.event_node.unsubscribe("task_state_announce", self.on_reverify_reply)
            #
            with self.replies_lock:
                replies = self.replies
                self.replies = {}
        #
        for task_id in suspects:
            if self._owner_replied(replies.get(task_id, [])):
                self.orphan_first_seen.pop(task_id, None)
                continue
            #
            self._retire(task_id)

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
            self.replies.setdefault(task_id, []).append(event_payload)

    def _collect_suspects(self):
        """ Non-local unfinished rows that have outlived the grace period """
        now = time.time()
        suspects = []
        #
        with self.node.lock:
            candidates = [
                task_id for task_id, state in self.node.global_task_state.items()
                if task_id not in self.node.running_tasks
                and task_id not in self.node.local_tasks
                and state.get("status", "unknown") != "stopped"
            ]
        #
        for task_id in list(self.orphan_first_seen):
            if task_id not in candidates:
                self.orphan_first_seen.pop(task_id, None)
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
                break
        #
        return suspects

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

    def _retire(self, task_id):
        with self.node.lock:
            self.node.state_events.pop(task_id, None)
            self.node.global_task_state.pop(task_id, None)
            self.node.known_task_ids.discard(task_id)
        #
        self.orphan_first_seen.pop(task_id, None)
        #
        log.warning("Retiring orphaned task state: %s", task_id)
        #
        self.node.event_node.emit(
            "task_status_change",
            {
                "task_id": task_id,
                "status": "pruned",
            }
        )
