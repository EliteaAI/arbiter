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


class TaskNodeHousekeeper(threading.Thread):  # pylint: disable=R0903
    """ Perform cleanup """

    def __init__(self, node):
        super().__init__(daemon=True)
        self.node = node

    def run(self):
        """ Run housekeeper thread """
        while not self.node.stop_event.is_set():
            time.sleep(self.node.housekeeping_interval)
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
