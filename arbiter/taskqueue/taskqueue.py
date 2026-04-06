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

""" TaskQueue """

import queue
import threading

from arbiter import log
from arbiter.tasknode.tools import InterruptTaskThread


class TaskQueue(threading.Thread):  # pylint: disable=R0902
    """ TaskQueue over TaskNode """

    def __init__(self, task_node, debug=False, wait_timeout=3.0, max_pending_tasks=None):
        super().__init__(daemon=True)
        #
        self.task_node = task_node
        self.debug = debug
        self.wait_timeout = wait_timeout
        self.max_pending_tasks = max_pending_tasks
        #
        self.stop_event = threading.Event()
        self.condition = threading.Condition()
        self.lock = threading.Lock()
        #
        self.tasks = []

    def run(self):
        """ Thread entrypoint """
        self.task_node.subscribe_to_task_statuses(self.on_worker_change)
        #
        while not self.stop_event.is_set():
            with self.condition:
                self.condition.wait(timeout=self.wait_timeout)
            #
            while self.tasks:
                with self.lock:
                    args, kwargs, task_id_queue = self.tasks.pop(0)
                #
                task_id = self.task_node.start_task(*args, **kwargs)
                #
                if task_id is not None:
                    if self.debug:
                        log.info("Task started by queue")
                    #
                    task_id_queue.put(task_id)
                else:
                    with self.lock:
                        self.tasks.insert(
                            0,
                            (args, kwargs, task_id_queue)
                        )
                    #
                    break

    def stop(self):
        """ Request queue to stop """
        self.stop_event.set()

    def add(self, *args, **kwargs):
        """ Add task to queue. Get 'promise' queue back """
        with self.lock:
            if self.max_pending_tasks is not None and \
                    len(self.tasks) >= self.max_pending_tasks:
                raise RuntimeError("Max pending tasks limit reached")
            #
            task_id_queue = queue.SimpleQueue()
            self.tasks.append(
                (args, kwargs, task_id_queue)
            )
        #
        self.wake()
        #
        return task_id_queue

    def wake(self):
        """ Notify thread on possible new task slot """
        with self.condition:
            self.condition.notify_all()

    def cancel(self, task_id_queue):
        """ Remove task from queue """
        with self.lock:
            for idx, task_data in enumerate(self.tasks):
                if task_data[-1] == task_id_queue:
                    self.tasks.pop(idx)
                    #
                    if self.debug:
                        log.info("Cancelled task at idx: %s", idx)
                    #
                    return

    def on_worker_change(self, event, payload):
        """ Called when TaskNode task status changes """
        _ = event
        if payload["status"] == "stopped":
            self.wake()

    def proxy(self, **rewrites):
        """ Run task via queue transparently for caller """
        _task_queue = self
        _rewrites = rewrites.copy()
        #
        def _rewrite(key, value):
            if key in _rewrites:
                return _rewrites[key](value)
            return value
        #
        def _proxy(*args, **kwargs):
            task_id_queue = None
            task_id = None
            #
            try:
                import tasknode_task  # pylint: disable=E0401,C0415
                #
                task_id_queue = _task_queue.add(
                    name=_rewrite("name", tasknode_task.name),
                    args=_rewrite("args", args),
                    kwargs=_rewrite("kwargs", kwargs),
                    pool=_rewrite("pool", tasknode_task.pool),
                    meta=_rewrite("meta", tasknode_task.meta.copy()),
                )
                #
                while True:
                    try:
                        task_id = task_id_queue.get(timeout=1)
                        break
                    except queue.Empty:
                        continue
                #
                return _task_queue.task_node.join_task(task_id)
            #
            except InterruptTaskThread:
                # Note: possible race condition with queue thread in the middle of task start
                if _task_queue.debug:
                    log.exception("Proxy task cancelled")
                #
                if task_id is not None:
                    if _task_queue.debug:
                        log.info("Stopping subtask: %s", task_id)
                    #
                    _task_queue.task_node.stop_task(task_id)
                #
                elif task_id_queue is not None:
                    if _task_queue.debug:
                        log.info("Cancelling queued subtask")
                    #
                    _task_queue.cancel(task_id_queue)
                #
                return ...
        #
        return _proxy
