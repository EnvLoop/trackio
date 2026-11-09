"""Bounded cloud database workloads; local SQLite keeps one engine."""

from contextlib import contextmanager
from contextvars import ContextVar

_workload = ContextVar("trackio_database_workload", default="write")


@contextmanager
def database_workload(name):
    if name not in {"write", "read"}:
        raise ValueError("INVALID_DATABASE_WORKLOAD")
    token = _workload.set(name)
    try:
        yield
    finally:
        _workload.reset(token)


def current_workload():
    return _workload.get()


def validate_pool_sizes(write_pool_size, read_pool_size):
    if any(
        type(value) is not int or not 5 <= value <= 1000
        for value in (write_pool_size, read_pool_size)
    ):
        raise ValueError("DATABASE_POOL_SIZE_MUST_BE_BETWEEN_5_AND_1000")
