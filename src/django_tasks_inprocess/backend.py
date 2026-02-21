"""This module contains the actual backend implementation."""

import asyncio
from collections import deque
from collections.abc import Mapping, Sequence
from contextvars import ContextVar
from traceback import format_exception
from typing import Any, Final, TypedDict, override

from django.core.signals import request_finished
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskContext, TaskError, TaskResult, TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json


class TaskBackendParams(TypedDict, total=False):
    """TypedDict representing arguments to the backend constructor."""

    QUEUES: Sequence[str]
    """Specify the queue names supported by the backend."""

    OPTIONS: Mapping[str, Any]
    """Extra parameters to pass to the Task backend."""


class InProcessTaskBackend(BaseTaskBackend):
    """
    A Task Backend that executes tasks at the end of the current request.

    Tasks are attached to the current request, and will be executed once the response has been sent.
    """

    supports_defer: Final[bool] = False
    supports_async_task: Final[bool] = True
    supports_get_result: Final[bool] = False
    supports_priority: Final[bool] = False

    def __init__(self, alias: str, params: TaskBackendParams) -> None:
        """Initialize the backend."""
        super().__init__(alias, params)
        self.worker_id = get_random_string(32)
        self.tasks = ContextVar[deque[TaskResult]](f"{alias}_inprocess_tasks")

    def _enqueue(self, task: Task, args: list[Any], kwargs: dict[str, Any]) -> TaskResult:
        self.validate_task(task)

        task_result = TaskResult(
            task=task,
            id=get_random_string(32),
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        try:
            queued_tasks = self.tasks.get()
        except LookupError:
            queued_tasks = deque()
            self.tasks.set(queued_tasks)

        queued_tasks.append(task_result)
        return task_result

    @override
    def enqueue(self, task: Task, args: list[Any], kwargs: dict[str, Any]) -> TaskResult:
        """Schedule the task to run at the end of the current request."""
        task_result: TaskResult = self._enqueue(task, args, kwargs)
        task_enqueued.send(type(self), task_result=task_result)
        request_finished.connect(self.execute_tasks, dispatch_uid="_in_process_task_backend")
        return task_result

    @override
    async def aenqueue(self, task: Task, args: list[Any], kwargs: dict[str, Any]) -> TaskResult:
        """
        Schedule the task to run at the end of the current request.

        Async version of :meth:`InProcessTaskBackend.enqueue`.
        """
        task_result: TaskResult = self._enqueue(task, args, kwargs)
        await task_enqueued.asend(type(self), task_result=task_result)
        request_finished.connect(self.aexecute_tasks, dispatch_uid="_in_process_task_backend")
        return task_result

    def _execute_task(self, task_result: TaskResult) -> None:
        """Execute the Task for the given TaskResult, mutating it with the outcome."""
        task = task_result.task
        task_start_time = timezone.now()
        object.__setattr__(task_result, "status", TaskResultStatus.RUNNING)
        object.__setattr__(task_result, "started_at", task_start_time)
        object.__setattr__(task_result, "last_attempted_at", task_start_time)
        task_result.worker_ids.append(self.worker_id)
        task_started.send(sender=type(self), task_result=task_result)

        try:
            if task.takes_context:
                raw_return_value = task.call(
                    TaskContext(task_result=task_result),
                    *task_result.args,
                    **task_result.kwargs,
                )
            else:
                raw_return_value = task.call(*task_result.args, **task_result.kwargs)

            object.__setattr__(
                task_result,
                "_return_value",
                normalize_json(raw_return_value),
            )
        except KeyboardInterrupt:
            # If the user tried to terminate, let them
            raise
        except BaseException as e:  # noqa: BLE001
            object.__setattr__(task_result, "finished_at", timezone.now())
            object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
            exception_type = type(e)
            task_result.errors.append(
                TaskError(
                    exception_class_path=(f"{exception_type.__module__}.{exception_type.__qualname__}"),
                    traceback="".join(format_exception(e)),
                )
            )
            task_finished.send(type(self), task_result=task_result)
        else:
            object.__setattr__(task_result, "finished_at", timezone.now())
            object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)
            task_finished.send(type(self), task_result=task_result)

    def execute_tasks(self, **_kwargs: Any) -> None:  # noqa: ANN401
        """Execute each task."""
        try:
            queued_tasks = self.tasks.get()
        except LookupError:
            pass
        else:
            while queued_tasks:
                self._execute_task(queued_tasks.pop())

    async def _aexecute_task(self, task_result: TaskResult) -> None:
        """Execute the Task for the given TaskResult, mutating it with the outcome."""
        task = task_result.task
        task_start_time = timezone.now()
        object.__setattr__(task_result, "status", TaskResultStatus.RUNNING)
        object.__setattr__(task_result, "started_at", task_start_time)
        object.__setattr__(task_result, "last_attempted_at", task_start_time)
        task_result.worker_ids.append(self.worker_id)
        await task_started.asend(sender=type(self), task_result=task_result)

        try:
            if task.takes_context:
                raw_return_value = await task.acall(
                    TaskContext(task_result=task_result),
                    *task_result.args,
                    **task_result.kwargs,
                )
            else:
                raw_return_value = await task.acall(*task_result.args, **task_result.kwargs)

            object.__setattr__(
                task_result,
                "_return_value",
                normalize_json(raw_return_value),
            )
        except KeyboardInterrupt:
            # If the user tried to terminate, let them
            raise
        except BaseException as e:  # noqa: BLE001
            object.__setattr__(task_result, "finished_at", timezone.now())
            object.__setattr__(task_result, "status", TaskResultStatus.FAILED)
            exception_type = type(e)
            task_result.errors.append(
                TaskError(
                    exception_class_path=(f"{exception_type.__module__}.{exception_type.__qualname__}"),
                    traceback="".join(format_exception(e)),
                )
            )
        else:
            object.__setattr__(task_result, "finished_at", timezone.now())
            object.__setattr__(task_result, "status", TaskResultStatus.SUCCESSFUL)

        await task_finished.asend(type(self), task_result=task_result)

    async def aexecute_tasks(self, **_kwargs: Any) -> None:  # noqa: ANN401
        """Execute the queued tasks.

        Async version of :meth:`InProcessTaskBackend.execute_tasks`.
        """
        try:
            queued_tasks = self.tasks.get()
        except LookupError:
            pass
        else:
            async with asyncio.TaskGroup() as tg:
                while queued_tasks:
                    tg.create_task(self._aexecute_task(queued_tasks.pop()))
