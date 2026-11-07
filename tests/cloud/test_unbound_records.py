import pytest
from sqlalchemy import func, select
from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, create_app
from trackio.cloud_workspace import WorkspaceRepository


def test_unbound_records_are_empty_then_show_the_same_run_after_bind(
    workspace_database,
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    service = {"Authorization": "Bearer " + "s" * 32}
    try:
        with TestClient(create_app(config, repository)) as client:
            response = client.get("/v1/records", headers=service, params=scope)
            assert response.status_code == 200, response.json()
            assert response.json() == {"records": [], "truncated": False, "next": None}
            with repository.engine.connect() as connection:
                for table in [repository.runs, repository.workspace.state]:
                    assert (
                        connection.execute(
                            select(func.count()).select_from(table)
                        ).scalar_one()
                        == 0
                    )
            assert client.get("/v1/records", params=scope).status_code == 403
            binding = client.post(
                "/v1/runs", headers=service, json={**scope, "config": {}}
            )
            binding.raise_for_status()
            record = dict(
                record_id="e",
                kind="history",
                observed_at=100,
                step=1,
                trial_id=None,
                writer_id="writer",
                payload={"loss": 1.0},
            )
            client.post(
                "/v1/runs/r/records",
                headers={"Authorization": "Bearer " + binding.json()["token"]},
                json=record,
            ).raise_for_status()
            assert client.get("/v1/records", headers=service, params=scope).json()[
                "records"
            ] == [record]
    finally:
        repository.close()


@pytest.mark.parametrize("field", ["workspace_id", "project_id", "job_id", "run_id"])
def test_existing_run_records_still_reject_a_mismatched_scope(
    workspace_database, field
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    repository.bind(
        workspace_id="foreign", project_id="p", job_id="j", run_id="other", config={}
    )
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    try:
        with TestClient(create_app(config, repository)) as client:
            response = client.get(
                "/v1/records",
                headers={"Authorization": "Bearer " + "s" * 32},
                params={**scope, field: "other"},
            )
            assert response.status_code == 403
            assert response.json() == {"error": "RUN_SCOPE_MISMATCH"}
    finally:
        repository.close()


def test_deleted_run_records_do_not_become_an_empty_success(workspace_database):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    repository.workspace.mutate(scope, "delete", {}, operation_id="delete")
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    try:
        with TestClient(create_app(config, repository)) as client:
            response = client.get(
                "/v1/records",
                headers={"Authorization": "Bearer " + "s" * 32},
                params=scope,
            )
            assert response.status_code != 200
            assert response.json() == {"error": "RUN_DELETED"}
    finally:
        repository.close()
