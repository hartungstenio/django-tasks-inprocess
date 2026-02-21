"""Test tasks."""

from django.tasks import task


@task
def sync_noop() -> None:
    """Do nothing."""


@task
async def async_noop() -> None:
    """Do nothing."""
