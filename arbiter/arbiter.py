#!/usr/bin/python3
# coding=utf-8
# pylint: disable=C0114,C0115,C0116

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

from uuid import uuid4

from arbiter import log

from .task import Task
from .tasknode import TaskNode


class Arbiter:  # pylint: disable=R0902
    def __init__(self, event_node, finalizer_check_interval=10):
        self.raw_task_node = TaskNode(event_node, task_limit=0)
        self.finalizer_check_interval = finalizer_check_interval
        #
        self.task_state = {}
        self.group_state = {}
        self.callbacks = {}
        self.finalizers = {}
        #
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.no_waiting_tasks = threading.Event()

    @property
    def task_node(self):
        if not self.raw_task_node.started:
            self.stop_event.clear()
            self.no_waiting_tasks.clear()
            self.raw_task_node.start()
            self.raw_task_node.subscribe_to_task_statuses(self.on_task_change)
            ArbiterFinalizer(self).start()
        #
        return self.raw_task_node

    def on_task_change(self, event, data):
        _ = event
        #
        task_id = data.get("task_id", None)
        status = data.get("status", "unknown")
        #
        if task_id is None or status == "unknown":
            return
        #
        status_map = {
            "pending": "initiated",
            "running": "running",
            "stopped": "done",
        }
        #
        with self.lock:
            if task_id not in self.task_state:
                self.task_state[task_id] = {
                    "task_type": "task",
                }
            #
            self.task_state[task_id]["state"] = status_map[status]
            #
            if status == "stopped":
                result = self.task_node.get_task_result(task_id)
                self.task_state[task_id]["result"] = result

    def wait_for_tasks(self, tasks):
        for task in tasks:
            result = self.task_node.join_task(task)
            #
            with self.lock:
                if task not in self.task_state:
                    self.task_state[task] = {
                        "task_type": "task",
                    }
                #
                self.task_state[task]["state"] = "done"
                self.task_state[task]["result"] = result
            #
            yield self.task_state[task].copy()

    def add_task(self, task, sync=False):
        tasks = []
        #
        for _ in range(task.tasks_count):
            task_key = self.task_node.start_task(
                name=task.name,
                args=task.task_args,
                kwargs=task.task_kwargs,
                pool=task.queue
            )
            #
            with self.lock:
                if task_key not in self.task_state:
                    self.task_state[task_key] = {
                        "task_type": task.task_type,
                        "state": "initiated",
                    }
                else:
                    self.task_state[task_key]["task_type"] = task.task_type
            #
            tasks.append(task_key)
            yield task_key
        #
        if sync:
            for message in self.wait_for_tasks(tasks):
                yield message

    def apply(self, task_name, queue="default", tasks_count=1, task_args=None, task_kwargs=None, sync=False):  # pylint: disable=C0301,R0913
        task = Task(name=task_name, queue=queue, tasks_count=tasks_count,
                    task_args=task_args, task_kwargs=task_kwargs)
        return list(self.add_task(task, sync=sync))

    def kill(self, task_key, sync=True):
        self.task_node.stop_task(task_key)
        if sync:
            self.task_node.wait_for_task(task_key)

    def kill_group(self, group_id):
        tasks = []
        #
        for task_id in self.group_state[group_id]:
            if task_id in self.task_state:
                tasks.append(task_id)
                self.kill(task_id, sync=False)
        #
        log.info("Terminating ...")
        for task in tasks:
            self.task_node.wait_for_task(task)

    def status(self, task_key):
        if task_key in self.task_state:
            return self.task_state[task_key]
        #
        if task_key in self.group_state:
            group_results = {
                "state": "done",
                "initiated": 0,
                "running": 0,
                "done": 0,
                "tasks": []
            }
            #
            for task_id in self.group_state[task_key]:
                if task_id in self.task_state:
                    if self.task_state[task_id]["state"] in ["running", "initiated"]:
                        group_results["state"] = self.task_state[task_id]["state"]
                    group_results[self.task_state[task_id]["state"]] += 1
                    group_results["tasks"].append(self.task_state[task_id])
                else:
                    log.info(f"[Group status] {task_id} is missing")
                    group_results["state"] = "running"
            #
            with self.lock:
                for callback in self.callbacks.values():
                    if callback["group_id"] == task_key:
                        group_results["state"] = "running"
                        group_results["initiated"] += 1
                #
                for finalizer in self.finalizers.values():
                    if finalizer["group_id"] == task_key:
                        group_results["state"] = "running"
                        group_results["initiated"] += 1
            #
            return group_results
        #
        raise NameError("Task or Group not found")

    def close(self, waiting_tasks_timeout=None):
        self.no_waiting_tasks.wait(waiting_tasks_timeout)
        self.stop_event.set()
        if self.raw_task_node.started:
            self.raw_task_node.stop()

    def workers(self):
        self.task_node.query_pool_state()
        result = {}
        #
        for pool, nodes in self.task_node.global_pool_state.items():
            if pool not in result:
                result[pool] = {
                    "total": 0,
                    "active": 0,
                    "available": 0,
                }
            #
            for state in nodes.values():
                total = state.get("task_limit", None)
                if total is None:
                    total = 1000  # FIXME: unlimited by task_node, using some constant here
                #
                active = state.get("running_tasks", 0)
                available = total - active
                #
                result[pool]["total"] += total
                result[pool]["active"] += active
                result[pool]["available"] += available
        #
        return result

    def squad(self, tasks, callback=None):
        """
        Set of tasks that need to be executed together
        """
        workers_count = {}
        for each in tasks:
            if each.task_type != "finalize":
                if each.queue not in list(workers_count.keys()):  # pylint: disable=C0201
                    workers_count[each.queue] = 0
                workers_count[each.queue] += each.tasks_count
        #
        stats = self.workers()
        log.info(f"Workers: {stats}")
        log.info(f"Tests to run {workers_count}")
        #
        for key in workers_count.keys():  # pylint: disable=C0201,C0206
            if not stats.get(key) or stats[key]["available"] < workers_count[key]:
                raise NameError(f"Not enough of {key} workers")
        #
        return self.group(tasks, callback)

    def group(self, tasks, callback=None):
        """
        Set of tasks that need to be executed regardless of order
        """
        group_id = str(uuid4())
        self.group_state[group_id] = []
        #
        finalizers = []
        #
        for each in tasks:
            if each.task_type == "finalize":
                finalizers.append(each)
                continue
            #
            for task in self.add_task(each):
                self.group_state[group_id].append(task)
        #
        if callback:
            callback_id = f'callback-{str(uuid4())}'
            callback.task_type = "callback"
            #
            with self.lock:
                self.callbacks[callback_id] = {
                    "callback": callback,
                    "group_id": group_id,
                }
        #
        with self.lock:
            for finalizer in finalizers:
                finalizer_id = f'finalizer-{str(uuid4())}'
                #
                self.finalizers[finalizer_id] = {
                    "finalizer": finalizer,
                    "group_id": group_id,
                }
        #
        return group_id

    def pipe(self, tasks, persistent_args=None, persistent_kwargs=None):
        """
        Set of tasks that need to be executed sequentially
        NOTE: Persistent args always before the task args
              Task itself need to have **kwargs if you want to ignore upstream results
        """
        pipe_id = str(uuid4())
        self.group_state[pipe_id] = []
        #
        if not persistent_args:
            persistent_args = []
        if not persistent_kwargs:
            persistent_kwargs = {}
        #
        res = {}
        yield {"pipe_id": pipe_id}
        #
        for task in tasks:
            task.task_args = persistent_args + task.task_args
            #
            for key, value in persistent_kwargs:
                if key not in task.task_kwargs:
                    task.task_kwargs[key] = value
            #
            if res:
                task.task_kwargs['upstream'] = res.get("result")
            #
            res = list(self.add_task(task, sync=True))
            self.group_state[pipe_id].append(res[0])
            #
            res = res[1]
            yield res


class ArbiterFinalizer(threading.Thread):  # pylint: disable=R0903
    """ Perform finalizer timeout checks """

    def __init__(self, arbiter):
        super().__init__(daemon=True)
        self.arbiter = arbiter

    def run(self):
        """ Run checker thread """
        while not self.arbiter.stop_event.is_set():
            time.sleep(self.arbiter.finalizer_check_interval)
            #
            # Callbacks
            #
            with self.arbiter.lock:
                active_callbacks = list(self.arbiter.callbacks)
            #
            for callback_id in active_callbacks:
                callback = self.arbiter.callbacks[callback_id]
                #
                if not all(
                    self.arbiter.status(task_id)["state"] == "done"
                    for task_id in self.arbiter.group_state[callback["group_id"]]
                ):
                    continue
                #
                for task_id in self.arbiter.add_task(callback["callback"]):
                    with self.arbiter.lock:
                        self.arbiter.group_state[callback["group_id"]].append(task_id)
                #
                with self.arbiter.lock:
                    self.arbiter.callbacks.pop(callback_id, None)
            #
            # Finalizers
            #
            with self.arbiter.lock:
                active_finalizers = list(self.arbiter.finalizers)
            #
            for finalizer_id in active_finalizers:
                finalizer = self.arbiter.finalizers[finalizer_id]
                #
                if not all(
                    self.arbiter.status(task_id)["state"] == "done"
                    for task_id in self.arbiter.group_state[finalizer["group_id"]]
                ):
                    continue
                #
                if any(
                    callback["group_id"] == finalizer["group_id"]
                    for callback in self.arbiter.callbacks.values()
                ):
                    continue
                #
                for task_id in self.arbiter.add_task(finalizer["finalizer"]):
                    with self.arbiter.lock:
                        self.arbiter.group_state[finalizer["group_id"]].append(task_id)
                #
                with self.arbiter.lock:
                    self.arbiter.finalizers.pop(finalizer_id, None)
            #
            # Check event
            #
            with self.arbiter.lock:
                if not self.arbiter.callbacks and not self.arbiter.finalizers:
                    self.arbiter.no_waiting_tasks.set()
                else:
                    self.arbiter.no_waiting_tasks.clear()
