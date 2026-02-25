import asyncio
import time
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from django.core.signals import request_finished
from django.tasks import TaskResult, TaskResultStatus, task_backends
from django.utils import timezone

from django_tasks_inprocess.backend import InProcessTaskBackend

from .tasks import async_noop, sync_noop

if TYPE_CHECKING:
    from django.tasks.backends.base import BaseTaskBackend

pytestmark = pytest.mark.django_db


class TestInProcessTaskBackend:
    """Tests for InProcessTaskBackend."""

    def test_backend_introspection(self) -> None:
        backend = task_backends["default"]

        assert isinstance(backend, InProcessTaskBackend)
        assert backend.supports_async_task is True
        assert backend.supports_defer is False
        assert backend.supports_get_result is False
        assert backend.supports_priority is False

    def test_enqueue_task(self) -> None:
        backend: BaseTaskBackend = task_backends["default"]
        assert isinstance(backend, InProcessTaskBackend)
        now = timezone.now()
        time.sleep(1)  # to avoid resolution issues

        task_result = backend.enqueue(sync_noop, (), {})

        assert request_finished.disconnect(dispatch_uid="_in_process_task_backend") is True

        time.sleep(1)  # to avoid resolution issues
        assert isinstance(task_result, TaskResult)
        assert task_result.task is sync_noop
        assert task_result.status == TaskResultStatus.READY
        assert now < task_result.enqueued_at < timezone.now()
        assert task_result.started_at is None
        assert task_result.last_attempted_at is None
        assert task_result.finished_at is None
        assert task_result.args == []
        assert task_result.kwargs == {}
        assert task_result.backend == backend.alias
        assert task_result.errors == []
        assert task_result.worker_ids == []

    @pytest.mark.asyncio
    async def test_aenqueue_task(self) -> None:
        backend: BaseTaskBackend = task_backends["default"]
        assert isinstance(backend, InProcessTaskBackend)
        now = timezone.now()
        await asyncio.sleep(1)  # to avoid resolution issues

        task_result = await backend.aenqueue(sync_noop, (), {})

        assert request_finished.disconnect(dispatch_uid="_in_process_task_backend") is True

        await asyncio.sleep(1)
        assert isinstance(task_result, TaskResult)
        assert task_result.task is sync_noop
        assert task_result.status == TaskResultStatus.READY
        assert now < task_result.enqueued_at < timezone.now()
        assert task_result.started_at is None
        assert task_result.last_attempted_at is None
        assert task_result.finished_at is None
        assert task_result.args == []
        assert task_result.kwargs == {}
        assert task_result.backend == backend.alias
        assert task_result.errors == []
        assert task_result.worker_ids == []

    @mock.patch("django_tasks_inprocess.backend.close_old_connections")
    def test_execute_tasks(self, mock_close_old_connections: mock.Mock, subtests: pytest.Subtests) -> None:
        backend: BaseTaskBackend = task_backends["default"]
        assert isinstance(backend, InProcessTaskBackend)

        try:
            with subtests.test("Single sync task"):
                mock_close_old_connections.reset_mock()
                task_result = backend.enqueue(sync_noop, (), {})

                got = request_finished.send(None)

                assert task_result.status == TaskResultStatus.SUCCESSFUL
                assert any(result is None for receiver, result in got if receiver == backend.execute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Single async task"):
                mock_close_old_connections.reset_mock()
                task_result = backend.enqueue(async_noop, (), {})

                got = request_finished.send(None)

                assert task_result.status == TaskResultStatus.SUCCESSFUL
                assert any(result is None for receiver, result in got if receiver == backend.execute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Multiple tasks"):
                mock_close_old_connections.reset_mock()
                task_results = [backend.enqueue(sync_noop, (), {}) for _ in range(5)]
                task_results.extend([backend.enqueue(async_noop, (), {}) for _ in range(5)])

                got = request_finished.send(None)

                assert all(task_result.status == TaskResultStatus.SUCCESSFUL for task_result in task_results)
                assert any(result is None for receiver, result in got if receiver == backend.execute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Without tasks"):
                mock_close_old_connections.reset_mock()
                got = request_finished.send(None)

                assert any(result is None for receiver, result in got if receiver == backend.execute_tasks)
                mock_close_old_connections.assert_called_once_with()

        finally:
            assert request_finished.disconnect(dispatch_uid="_in_process_task_backend") is True

    @pytest.mark.asyncio
    @mock.patch("django_tasks_inprocess.backend.close_old_connections")
    async def test_execute_tasks_async(self, mock_close_old_connections: mock.Mock, subtests: pytest.Subtests) -> None:
        backend: BaseTaskBackend = task_backends["default"]
        assert isinstance(backend, InProcessTaskBackend)
        try:
            with subtests.test("Single sync task"):
                mock_close_old_connections.reset_mock()
                task_result = await backend.aenqueue(sync_noop, (), {})

                got = await request_finished.asend(None)

                assert task_result.status == TaskResultStatus.SUCCESSFUL
                assert any(result is None for receiver, result in got if receiver == backend.aexecute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Single async task"):
                mock_close_old_connections.reset_mock()
                task_result = await backend.aenqueue(async_noop, (), {})

                got = await request_finished.asend(None)

                assert task_result.status == TaskResultStatus.SUCCESSFUL
                assert any(result is None for receiver, result in got if receiver == backend.aexecute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Multiple tasks"):
                mock_close_old_connections.reset_mock()
                task_results = [backend.enqueue(sync_noop, (), {}) for _ in range(5)]
                task_results.extend([backend.enqueue(async_noop, (), {}) for _ in range(5)])

                got = await request_finished.asend(None)

                assert all(task_result.status == TaskResultStatus.SUCCESSFUL for task_result in task_results)
                assert any(result is None for receiver, result in got if receiver == backend.aexecute_tasks)
                mock_close_old_connections.assert_called_once_with()

            with subtests.test("Without tasks"):
                mock_close_old_connections.reset_mock()
                got = await request_finished.asend(None)

                assert any(result is None for receiver, result in got if receiver == backend.aexecute_tasks)
                mock_close_old_connections.assert_called_once_with()

        finally:
            assert request_finished.disconnect(dispatch_uid="_in_process_task_backend") is True
