import hashlib

import pytest
from starlette.testclient import TestClient

import trackio
from trackio.cloud import CloudConfig, create_app
from trackio.cloud_client import CloudRun
from trackio.cloud_objects import FileObjects
from trackio.cloud_workspace import WorkspaceRepository


@pytest.fixture
def sdk(tmp_path, workspace_database):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    config = CloudConfig("local", repository.url, "s" * 32, "k" * 32)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    objects = FileObjects(tmp_path / "objects")
    with TestClient(create_app(config, repository, objects=objects)) as client:
        bound = client.post(
            "/v1/runs",
            json={**scope, "config": {}},
            headers={"Authorization": "Bearer " + "s" * 32},
        ).json()
        run = CloudRun(
            "http://testserver",
            bound["token"],
            "r",
            tmp_path / "spool",
            transport=client,
        )
        token = trackio.context_vars.current_run.set(run)
        try:
            yield run, repository, scope, client
        finally:
            trackio.context_vars.current_run.reset(token)
    repository.close()


def test_native_artifact_origin_wait_download_registry_and_logbook(sdk, tmp_path):
    run, repository, scope, _ = sdk
    path = tmp_path / "synthetic.bin"
    path.write_bytes(b"synthetic")
    original = trackio.Artifact("model", "model")
    original.add_file(path, "weights.bin")
    artifact = trackio.log_artifact(original, aliases=["prod"])
    assert artifact.wait().version == "v0"
    assert set(artifact.aliases) == {"latest", "prod"}
    assert original.wait().version == artifact.version
    output = original.download(tmp_path / "download")
    assert (tmp_path / "download" / "weights.bin").read_bytes() == b"synthetic"
    assert output == str(tmp_path / "download")
    assert trackio.use_artifact("model:prod").digest == artifact.digest
    linked = artifact.link("research/model", aliases=["candidate"])
    run.flush()
    assert linked["source"]["digest"] == artifact.digest
    assert (
        repository.workspace.resolve(scope, "registry", "research.model", "candidate")[
            "content"
        ]["source"]["digest"]
        == artifact.digest
    )
    operation = run.log_logbook(
        "notes", {"index.md": "# Synthetic"}, {"plot.bin": path}
    )
    assert run._workspace_query("operation", operation_id=operation)["committed"]
    assert (
        repository.workspace.resolve(scope, "logbook", "notes")["content"][
            "attachments"
        ][0]["_object"]
        == hashlib.sha256(b"synthetic").hexdigest()
    )


def test_managed_api_routes_to_bound_scope_without_sqlite(sdk, monkeypatch):
    run, _, _, _ = sdk

    def forbidden(*args, **kwargs):
        raise AssertionError("Managed Api touched upstream SQLite")

    monkeypatch.setattr(
        trackio.sqlite_storage.SQLiteStorage, "get_project_db_path", forbidden
    )
    api = trackio.Api()
    runs = list(api.runs("p"))
    assert len(runs) == 1 and runs[0].id == run.run_id
    with pytest.raises(PermissionError):
        list(api.runs("other-project"))


def test_worker_cannot_manage_or_export_another_scope(sdk):
    run, _, scope, client = sdk
    headers = {"Authorization": "Bearer " + run.token}
    assert (
        client.post(
            "/v1/workspace/mutate",
            headers=headers,
            json={"action": "delete", "scope": scope, "operation_id": "delete"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/v1/workspace/query",
            headers=headers,
            json={"action": "export", "scope": {**scope, "job_id": "other"}},
        ).status_code
        == 403
    )


def test_native_registry_collection_link_unlink_and_lineage(sdk, tmp_path, monkeypatch):
    run, repository, scope, _ = sdk

    def forbidden(*args, **kwargs):
        raise AssertionError("Managed registry touched upstream SQLite")

    monkeypatch.setattr(
        trackio.sqlite_storage.SQLiteStorage, "get_project_db_path", forbidden
    )
    api = trackio.Api()
    registry = api.create_registry("models", description="Synthetic registry")
    assert registry.description == "Synthetic registry"
    assert registry.create_collection("weights", "model").type == "model"
    path = tmp_path / "weights.bin"
    path.write_bytes(b"weights")
    original = trackio.Artifact("weights", "model")
    original.add_file(path)
    trackio.log_artifact(original).wait()
    linked = original.link("registry-models/weights", aliases=["staging"])
    assert linked.is_link and linked.version == "v0"
    assert registry.collection("weights").num_links == 1
    assert linked.download(tmp_path / "linked") == str(tmp_path / "linked")
    trackio.use_artifact("weights:latest")
    run.flush()
    assert repository.workspace.lineage(scope, "weights", "latest")
    linked.unlink()
    assert registry.collection("weights").num_links == 0
    assert api.registry("models").collections()[0].name == "weights"
    assert registry.events()
    wrong_type = run.log_artifact(path, name="wrong-type", type="dataset").wait()
    with pytest.raises(ValueError, match="REGISTRY_ARTIFACT_TYPE_CONFLICT"):
        wrong_type.link("registry-models/weights")


def test_managed_save_registry_constructor_and_project_delete_never_use_sqlite(
    sdk, tmp_path, monkeypatch
):
    run, _, _, _ = sdk

    def forbidden(*args, **kwargs):
        raise AssertionError("Managed SDK used legacy storage")

    monkeypatch.setattr(
        trackio.sqlite_storage.SQLiteStorage, "get_project_db_path", forbidden
    )
    path = tmp_path / "config.txt"
    path.write_text("synthetic")
    saved = trackio.save(path)
    run.flush()
    assert saved.startswith("cloud-artifact:")
    registry = trackio.Api().create_registry("models")
    assert trackio.Registry("models").name == registry.name
    with pytest.raises(PermissionError):
        trackio.delete_project("p", force=True)


def test_export_json_parquet_readback_and_cached_download_reauthorization(
    sdk, tmp_path
):
    run, repository, scope, _ = sdk
    from trackio.cloud_export import read

    run.log({"loss": 0.5}, step=7)
    run.flush()
    api_run = next(iter(trackio.Api().runs("p")))
    for format in ("json", "parquet"):
        path = tmp_path / ("export." + format)
        assert api_run.export(path, format=format) == str(path)
        snapshot = read(path, format=format)
        assert snapshot["scope"] == scope and snapshot["events"]
    path = tmp_path / "content.bin"
    path.write_bytes(b"private-content")
    artifact = run.log_artifact(path, name="private").wait()
    repository.revoke(scope)
    with pytest.raises(Exception) as denied:
        artifact.download(tmp_path / "unauthorized")
    assert denied.value.response.status_code in (401, 403)


def test_download_rechecks_revocation_after_initial_authorization(
    sdk, tmp_path, monkeypatch
):
    run, repository, scope, client = sdk
    path = tmp_path / "content.bin"
    path.write_bytes(b"race-content")
    artifact = run.log_artifact(path, name="race").wait()
    digest = artifact.manifest[0]["digest"]
    original = repository.authorize_capability

    def revoke_after_authorization(capability_id):
        result = original(capability_id)
        repository.revoke(scope)
        return result

    monkeypatch.setattr(repository, "authorize_capability", revoke_after_authorization)
    response = client.get(
        "/v1/workspace/objects/" + digest,
        headers={"Authorization": "Bearer " + run.token},
    )
    assert response.status_code in (401, 403)


def test_worker_foreign_digest_and_registry_scope_status_contract(sdk, tmp_path):
    run, repository, scope, client = sdk
    foreign = {**scope, "project_id": "foreign", "run_id": "foreign"}
    repository.bind(**foreign, config={})
    digest = repository.put_import_object(
        foreign, b"foreign-only", FileObjects(tmp_path / "objects")
    )
    headers = {"Authorization": "Bearer " + run.token}
    denied = client.get("/v1/workspace/objects/" + digest, headers=headers)
    assert denied.status_code == 403 and denied.json()["error"] == "RUN_SCOPE_MISMATCH"
    denied = client.post(
        "/v1/workspace/publish",
        headers=headers,
        json={
            "kind": "registry",
            "name": "foreign",
            "operation_id": "foreign",
            "content": {
                "source": {
                    "scope": foreign,
                    "name": "foreign",
                    "version": "v0",
                    "digest": digest,
                }
            },
        },
    )
    assert (
        denied.status_code == 401
        and denied.json()["error"] == "CROSS_RUN_LINK_REQUIRES_SERVICE_AUTH"
    )
