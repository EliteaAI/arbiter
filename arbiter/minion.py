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


from arbiter import log

from .task import Task
from .tasknode import TaskNode


class Minion:
    def __init__(self, event_node, queue="default"):
        self.queue = queue
        self.raw_task_node = TaskNode(event_node, pool=self.queue, task_limit=0)

    @property
    def task_node(self):
        if not self.raw_task_node.started:
            self.raw_task_node.start()
        return self.raw_task_node

    def wait_for_tasks(self, tasks):
        for task in tasks:
            result = self.task_node.join_task(task)
            yield {
                "task_type": "task",
                "state": "done",
                "result": result,
            }

    def add_task(self, task, sync=False):
        tasks = []
        for _ in range(task.tasks_count):
            task_key = self.task_node.start_task(
                name=task.name,
                args=task.task_args,
                kwargs=task.task_kwargs,
                pool=task.queue
            )
            tasks.append(task_key)
            yield task_key
        if sync:
            for message in self.wait_for_tasks(tasks):
                yield message

    def apply(self, task_name, queue=None, tasks_count=1, task_args=None, task_kwargs=None, sync=True):  # pylint: disable=C0301,R0913
        task = Task(task_name, queue=queue if queue else self.queue,
                    tasks_count=tasks_count, task_args=task_args, task_kwargs=task_kwargs)
        for message in self.add_task(task, sync=sync):
            yield message

    def task(self, *args, **kwargs):  # pylint: disable=W0613
        """ Task decorator """
        def inner_task(func):
            def create_task(**kwargs):
                def _create_task(func):
                    return self._create_task_from_callable(func, **kwargs)
                return _create_task
            if callable(func):
                return create_task(**kwargs)(func)
            raise TypeError('@task decorated function must be callable')
        return inner_task

    def _create_task_from_callable(self, func, name=None, **kwargs):  # pylint: disable=W0613
        name = name if name else f"{func.__name__}.{func.__module__}"
        self.raw_task_node.register_task(func, name)
        return func

    def run(self, workers, block=True):
        log.info("Starting '%s' worker", self.queue)
        #
        self.raw_task_node.task_limit = workers
        self.raw_task_node.start()
        #
        if block:
            self.raw_task_node.stop_event.wait()
