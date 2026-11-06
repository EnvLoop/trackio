import httpx2
import pytest

import trackio
from trackio.cloud_client import CloudRun


@pytest.mark.parametrize("operation", ["sync", "freeze"])
def test_managed_publish_flushes_without_sqlite_or_hub(
    tmp_path, monkeypatch, operation
):
    def forbidden(*args, **kwargs):
        raise AssertionError("managed publication reached local SQLite or HF")

    monkeypatch.setattr(trackio.deploy.SQLiteStorage, "get_space_id", forbidden)
    monkeypatch.setattr(trackio.deploy.huggingface_hub, "HfApi", forbidden)
    transport = httpx2.Client(
        transport=httpx2.MockTransport(
            lambda request: httpx2.Response(200, json={"committed": True})
        )
    )
    run = CloudRun(
        "http://localhost", "t" * 32, "r", tmp_path / "spool", transport=transport
    )
    token = trackio.context_vars.current_run.set(run)
    try:
        arguments = {"project": "synthetic"}
        if operation == "freeze":
            arguments["space_id"] = "example/forbidden"
        assert getattr(trackio, operation)(**arguments) is None
    finally:
        trackio.context_vars.current_run.reset(token)
        transport.close()


@pytest.mark.parametrize("operation", ["sync", "freeze"])
def test_managed_context_remains_bound_after_finish(monkeypatch, operation):
    def forbidden(*args, **kwargs):
        raise AssertionError("managed publication reached HF")

    monkeypatch.setenv("TRACKIO_ENVLOOP_CONTEXT_FILE", "/synthetic/private-context")
    monkeypatch.setattr(trackio.deploy.huggingface_hub, "HfApi", forbidden)
    token = trackio.context_vars.current_run.set(None)
    try:
        arguments = {"project": "synthetic"}
        if operation == "freeze":
            arguments["space_id"] = "example/forbidden"
        assert getattr(trackio, operation)(**arguments) is None
    finally:
        trackio.context_vars.current_run.reset(token)
