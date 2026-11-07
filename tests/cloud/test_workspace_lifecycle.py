import importlib

import pytest

from trackio.cloud import MetricsRepository
from trackio.cloud_workspace import WorkspaceStore


def test_scoped_list_filter_compare_export_rename_move_delete(workspace_database):
    url, schema = workspace_database
    repository = MetricsRepository(url, schema=schema)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    other = {**scope, "run_id": "s"}
    for identity, lr in ((scope, 0.1), (other, 0.2)):
        repository.bind(**identity, config={"lr": lr})
    store = WorkspaceStore(repository, initialize=True)
    assert callable(getattr(store, "list_runs", None)), (
        "Scoped run management is missing"
    )
    first = store.list_runs(
        {key: scope[key] for key in ("workspace_id", "project_id", "job_id")}, limit=1
    )
    assert [row["run_id"] for row in first["runs"]] == ["r"]
    second = store.list_runs(
        {key: scope[key] for key in ("workspace_id", "project_id", "job_id")},
        limit=1,
        after=first["next"],
    )
    assert [row["run_id"] for row in second["runs"]] == ["s"]
    assert store.list_runs({**scope, "workspace_id": "other"})["runs"] == []
    assert len(store.list_runs(scope, filters={"config": {"lr": 0.1}})["runs"]) == 1
    renamed = store.mutate(scope, "rename", {"name": "renamed"}, operation_id="rename")
    assert renamed["name"] == "renamed"
    assert (
        store.mutate(scope, "rename", {"name": "renamed"}, operation_id="rename")
        == renamed
    )
    assert store.export(scope)["run"]["name"] == "renamed"
    assert len(store.compare([scope, other])["runs"]) == 2
    destination = {**scope, "project_id": "destination"}
    moved = store.mutate(
        scope, "move", {"destination": destination}, operation_id="move"
    )
    assert (
        store.mutate(scope, "move", {"destination": destination}, operation_id="move")
        == moved
    )
    with pytest.raises(PermissionError):
        store.export(scope)
    assert store.export(destination)["scope"] == destination
    store.mutate(destination, "delete", {}, operation_id="delete")
    assert store.list_runs(destination)["runs"] == []
    with pytest.raises(PermissionError):
        store.export(destination)
    repository.close()


def test_deleted_or_expired_runs_cannot_keep_writing_after_capability_fence(
    workspace_database,
):
    module = importlib.import_module("trackio.cloud_workspace")
    assert hasattr(module, "WorkspaceRepository"), (
        "Lifecycle fence integration is missing"
    )
    url, schema = workspace_database
    repository = module.WorkspaceRepository(
        url, schema=schema, initialize_workspace=True
    )
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    store = repository.workspace
    store.mutate(scope, "retention", {"expires_at": 1}, operation_id="retention")
    assert store.purge(scope)["deleted"] == 1
    with pytest.raises(PermissionError):
        repository.append(
            **scope,
            batch={
                "batch_id": "b",
                "events": [
                    {
                        "event_id": "e",
                        "writer_id": "writer",
                        "trial_id": None,
                        "step": 1,
                        "observed_at": 100,
                        "category": "experiment",
                        "metrics": {"loss": 1},
                    }
                ],
            },
            capability_id=capability["capability_id"],
        )
    with pytest.raises(PermissionError):
        store.publish(
            {**scope, "trial_id": None, "capability_id": capability["capability_id"]},
            "logbook",
            "notes",
            {"pages": {}},
            operation_id="late",
        )
    repository.close()
