import pytest
from sqlalchemy import select
from starlette.testclient import TestClient

from trackio.cloud import SCHEMA_VERSION, CloudConfig, MetricsRepository, create_app
from trackio.cloud_workspace import WORKSPACE_SCHEMA_VERSION, WorkspaceRepository


@pytest.mark.parametrize("workspace", [False, True])
def test_ready_reports_actual_workspace_schema_version(workspace_database, workspace):
    url, schema = workspace_database
    repository = (
        WorkspaceRepository(url, schema=schema, initialize_workspace=True)
        if workspace
        else MetricsRepository(url, schema=schema)
    )
    try:
        config = CloudConfig("local", url, "s" * 32, "k" * 32)
        with TestClient(create_app(config, repository)) as client:
            response = client.get("/ready")
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == "ready"
        assert result["schema_version"] == SCHEMA_VERSION
        if workspace:
            with repository.engine.connect() as connection:
                actual_version = connection.execute(
                    select(repository.workspace.schema.c.version)
                ).scalar_one()
            assert actual_version == WORKSPACE_SCHEMA_VERSION
            assert result["workspace_schema_version"] == actual_version
        else:
            assert result["workspace_schema_version"] is None
    finally:
        repository.close()
