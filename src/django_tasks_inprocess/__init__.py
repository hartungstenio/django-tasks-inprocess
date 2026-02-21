"""A Django Tasks backend inspired by Starlette's In-process Background Workers."""

from .backend import InProcessTaskBackend

__all__ = ["InProcessTaskBackend"]
