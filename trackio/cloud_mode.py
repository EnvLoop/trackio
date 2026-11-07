"""Managed execution boundaries shared by public SDK entry points."""

import os

from trackio import context_vars
from trackio.cloud_client import CloudRun


def managed_run():
    run = context_vars.current_run.get()
    return run if isinstance(run, CloudRun) else None


def is_managed():
    return managed_run() is not None or bool(
        os.environ.get("TRACKIO_ENVLOOP_CONTEXT_FILE")
    )


def flush_managed():
    run = managed_run()
    if run is not None:
        run.flush()
