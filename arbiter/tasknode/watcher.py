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

import os
import time
import datetime
import threading
import multiprocessing

from arbiter import log


class TaskNodeWatcher(threading.Thread):  # pylint: disable=R0903
    """ Watch running tasks """

    def __init__(self, node):
        super().__init__(daemon=True)
        self.node = node

    def run(self):
        """ Run watcher thread """
        #
        while not self.node.stop_event.is_set():
            try:
                if self.node.multiprocessing_context in ["threading"]:
                    self._watch_stopped_tasks__threading()
                else:
                    self._watch_stopped_tasks__multiprocessing()
            except:  # pylint: disable=W0702
                log.exception("Exception in watcher thread, continuing")

    def _watch_stopped_tasks__threading(self):  # pylint: disable=R0912,R0914,R0915
        #
        # Wait until we have tasks to watch
        #
        watcher_max_wait = self.node.watcher_max_wait
        self.node.have_running_tasks.wait(watcher_max_wait)
        #
        time.sleep(self.node.thread_scan_interval)  # Additional wait interval to avoid busy looping
        #
        if self.node.result_transport == "events":
            #
            # Check stopped tasks with missing results
            #
            with self.node.lock:
                done_tasks = []
                #
                for task_id, data in self.node.running_tasks.items():
                    if data["thread"] is not None:
                        continue
                    #
                    if data["result"] is not None:
                        done_task = (task_id, data["result"])
                        done_tasks.append(done_task)
                        continue
                    #
                    age = (datetime.datetime.now() - data["timestamp"]).total_seconds()
                    if age < self.node.result_max_wait:
                        continue
                    #
                    done_task = (task_id, data["result"])
                    done_tasks.append(done_task)
            #
            # Announce late and expired
            #
            for task_id, result in done_tasks:
                self._announce_task_stopped(task_id, result)
        #
        # Collect task IDs of stopped tasks
        #
        with self.node.lock:
            stopped_tasks = []
            #
            for task_id, data in self.node.running_tasks.items():
                if data["thread"] is None:
                    continue  # task is stopped, awaiting result
                #
                if data["thread"].is_alive():
                    continue
                #
                stopped_tasks.append(task_id)
        #
        # Process newly stopped tasks
        #
        for task_id in stopped_tasks:
            task_data = self.node.running_tasks.get(task_id, None)
            #
            if task_data is None:
                continue
            #
            try:
                task_data["thread"].join(1)
            except:  # pylint: disable=W0702
                log.exception("Failed to join thread, continuing")
            finally:
                task_data["thread"] = None
            #
            task_payload = self.node.local_tasks.get(task_id, None)
            #
            if task_payload is None:
                task_payload = {
                    "durable": False,
                }
            #
            if task_payload.get("durable", False):
                # Task is durable, restart it
                self.node.execute_local_task(
                    task_id,
                    name=task_payload.get("name"),
                    meta=task_payload.get("meta"),
                    args=task_payload.get("args"),
                    kwargs=task_payload.get("kwargs"),
                    durable=task_payload.get("durable"),
                    pool=task_payload.get("pool"),
                )
                #
                continue
            #
            if self.node.result_transport == "files":
                try:
                    result_path = os.path.join(self.node.tmp_path, f'{task_id}.bin')
                    with open(result_path, "rb") as file:
                        task_data["result"] = file.read()
                    os.remove(result_path)
                except:  # pylint: disable=W0702
                    log.exception("Failed to load/remove result, continuing")
            #
            elif self.node.result_transport == "events" and task_data["result"] is None:
                # Result event is not processed (or process crashed badly)
                task_data["timestamp"] = datetime.datetime.now()
                continue
            elif self.node.result_transport == "memory":
                try:
                    result = task_data["result"].get(timeout=self.node.result_max_wait)
                except:  # pylint: disable=W0702
                    result = None
                #
                task_data["result"] = result
            #
            self._announce_task_stopped(task_id, task_data["result"])

    def _watch_stopped_tasks__multiprocessing(self):  # pylint: disable=R0912,R0914,R0915
        #
        # Wait until we have tasks to watch
        #
        watcher_max_wait = self.node.watcher_max_wait
        self.node.have_running_tasks.wait(watcher_max_wait)
        #
        if self.node.result_transport == "events":
            #
            # Check stopped tasks with missing results
            #
            with self.node.lock:
                done_tasks = []
                #
                for task_id, data in self.node.running_tasks.items():
                    if data["process"] is not None:
                        continue
                    #
                    if data["result"] is not None:
                        done_task = (task_id, data["result"])
                        done_tasks.append(done_task)
                        continue
                    #
                    age = (datetime.datetime.now() - data["timestamp"]).total_seconds()
                    if age < self.node.result_max_wait:
                        continue
                    #
                    done_task = (task_id, data["result"])
                    done_tasks.append(done_task)
            #
            # Announce late and expired
            #
            for task_id, result in done_tasks:
                self._announce_task_stopped(task_id, result)
        #
        # Collect sentinels of running tasks
        #
        with self.node.lock:
            sentinel_map = {}
            #
            for task_id, data in self.node.running_tasks.items():
                if data["process"] is None:
                    continue  # task is stopped, awaiting result
                sentinel_map[data["process"].sentinel] = task_id
        #
        # Wait for tasks to stop
        #
        ready_sentinels = multiprocessing.connection.wait(
            list(sentinel_map), timeout=watcher_max_wait
        )
        #
        # Process newly stopped tasks
        #
        for sentinel in ready_sentinels:
            task_id = sentinel_map[sentinel]
            task_data = self.node.running_tasks.get(task_id, None)
            #
            if task_data is None:
                continue
            #
            process_pid = task_data["process"].pid
            if process_pid is not None:
                try:
                    import pylon  # pylint: disable=C0415,E0401,W0611
                    from tools import context  # pylint: disable=C0415,E0401
                    #
                    context.zombie_reaper.external_pids.discard(process_pid)
                except:  # pylint: disable=W0702
                    pass
            #
            try:
                task_data["process"].join(1)
                task_data["process"].close()
            except:  # pylint: disable=W0702
                log.exception("Failed to close process, continuing")
            finally:
                task_data["process"] = None
            #
            task_payload = self.node.local_tasks.get(task_id, None)
            #
            if task_payload is None:
                task_payload = {
                    "durable": False,
                }
            #
            if task_payload.get("durable", False):
                # Task is durable, restart it
                self.node.execute_local_task(
                    task_id,
                    name=task_payload.get("name"),
                    meta=task_payload.get("meta"),
                    args=task_payload.get("args"),
                    kwargs=task_payload.get("kwargs"),
                    durable=task_payload.get("durable"),
                    pool=task_payload.get("pool"),
                )
                #
                continue
            #
            if self.node.result_transport == "files":
                try:
                    result_path = os.path.join(self.node.tmp_path, f'{task_id}.bin')
                    with open(result_path, "rb") as file:
                        task_data["result"] = file.read()
                    os.remove(result_path)
                except:  # pylint: disable=W0702
                    log.exception("Failed to load/remove result, continuing")
            #
            elif self.node.result_transport == "events" and task_data["result"] is None:
                # Result event is not processed (or process crashed badly)
                task_data["timestamp"] = datetime.datetime.now()
                continue
            elif self.node.result_transport == "memory":
                try:
                    result = task_data["result"].get(timeout=self.node.result_max_wait)
                except:  # pylint: disable=W0702
                    result = None
                #
                try:
                    task_data["result"].close()
                except:  # pylint: disable=W0702
                    log.exception("Failed to close result, continuing")
                #
                task_data["result"] = result
            #
            self._announce_task_stopped(task_id, task_data["result"])

    def _announce_task_stopped(self, task_id, result):
        task_state = self.node.global_task_state[task_id].copy()
        #
        task_state["status"] = "stopped"
        task_state["result"] = result
        #
        with self.node.lock:
            self.node.local_tasks.pop(task_id, None)
            self.node.running_tasks.pop(task_id, None)
            if not self.node.running_tasks:
                self.node.have_running_tasks.clear()
        #
        self.node.event_node.emit(
            "task_node_announce",
            {
                "ident": self.node.ident,
                "pool": self.node.pool,
                "task_limit": self.node.task_limit,
                "running_tasks": len(self.node.running_tasks),
            }
        )
        #
        self.node.announce_task_state(task_state)
        #
        self.node.event_node.emit(
            "task_status_change",
            {
                "task_id": task_id,
                "status": "stopped",
            }
        )
