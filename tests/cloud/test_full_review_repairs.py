import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, current_thread

import pytest
from sqlalchemy import event, select, text
from starlette.testclient import TestClient

import trackio
from tests.cloud import test_workspace_sdk
from trackio.cloud import CloudConfig, create_app, issue_token
from trackio.cloud_client import CloudRun
from trackio.cloud_objects import FileObjects
from trackio.cloud_protocol import content_digest
from trackio.cloud_workspace import WorkspaceRepository

sdk = test_workspace_sdk.sdk


def claims_for(repository, scope):
    capability = repository.capability(scope, None)
    return {**scope, "trial_id": None, "capability_id": capability["capability_id"]}


def append(repository, scope, step):
    repository.append(
        **scope,
        batch={
            "batch_id": f"batch-{scope['run_id']}-{step}",
            "events": [
                {
                    "event_id": f"event-{scope['run_id']}-{step}",
                    "writer_id": "writer",
                    "trial_id": None,
                    "step": step,
                    "observed_at": step,
                    "category": "experiment",
                    "metrics": {"loss": step},
                }
            ],
        },
    )


def test_full01_partial_gc_failure_cannot_restore_missing_catalog(sdk, tmp_path):
    _, repository, scope, _ = sdk
    objects = FileObjects(tmp_path / "gc")
    digests = []
    for payload in (b"first", b"second"):
        digests.append(hashlib.sha256(payload).hexdigest())
        repository.put_import_object(scope, payload, objects)
    with repository.engine.begin() as connection:
        connection.execute(repository.workspace.uploads.update().values(created_at=1))
        connection.execute(
            repository.workspace.upload_locations.update().values(created_at=1)
        )
    deleted = []

    class FailingDelete:
        def delete(self, key):
            if deleted:
                raise OSError("second physical deletion failed")
            objects.delete(key)
            deleted.append(key)

    with pytest.raises(OSError):
        repository.workspace.collect_orphans(scope, FailingDelete())
    digest = deleted[0].split("/")[-1]
    with pytest.raises(ValueError, match="OBJECT"):
        repository.workspace.publish(
            claims_for(repository, scope),
            "artifact",
            "dead",
            {
                "artifact_type": "file",
                "manifest": [
                    {
                        "path": "dead",
                        "_object": digest,
                        "size": 5 if digest == digests[0] else 6,
                    }
                ],
            },
            operation_id="dead-publication",
        )
    repository.workspace.collect_orphans(scope, objects)
    repository.put_import_object(scope, b"first", objects)
    assert repository.read_object(scope, digests[0], objects) == b"first"


@pytest.mark.parametrize(
    "action",
    [
        "export",
        "list",
        "compare",
        "resources",
        "operation",
        "lineage",
        "resource",
        "legacy_rows",
        "legacy_receipt",
    ],
)
def test_full02_query_revocation_while_waiting_for_body(sdk, action):
    run, repository, scope, client = sdk

    def body():
        repository.revoke(scope)
        yield json.dumps(
            {
                "action": action,
                "scope": scope,
                "scopes": [scope],
                "kind": "artifact",
                "name": "missing",
                "operation_id": "missing",
                "source_id": "missing",
                "database": "missing",
                "table": "missing",
            }
        ).encode()

    response = client.post(
        "/v1/workspace/query",
        content=body(),
        headers={"Authorization": "Bearer " + run.token},
    )
    assert response.status_code == 401, response.text


def test_full04_move_retry_retains_each_physical_upload_location(sdk, tmp_path):
    _, repository, scope, _ = sdk
    objects = FileObjects(tmp_path / "gc")
    payload = b"same-digest"
    digest = hashlib.sha256(payload).hexdigest()

    class LostResponse:
        def put(self, *args):
            objects.put(*args)
            raise OSError("response lost")

    with pytest.raises(OSError):
        repository.put_import_object(scope, payload, LostResponse())
    destination = {**scope, "project_id": "destination"}
    repository.workspace.mutate(
        scope, "move", {"destination": destination}, operation_id="move"
    )
    repository.put_import_object(destination, payload, objects)
    with repository.engine.begin() as connection:
        connection.execute(repository.workspace.uploads.update().values(created_at=1))
        connection.execute(
            repository.workspace.upload_locations.update().values(created_at=1)
        )
    repository.workspace.collect_orphans(destination, objects)
    for identity in (scope, destination):
        with pytest.raises(FileNotFoundError):
            objects.get(content_digest(identity) + "/" + digest, digest, len(payload))


def test_full05_native_registry_use_after_same_run_move(sdk, tmp_path):
    run, repository, scope, client = sdk
    path = tmp_path / "weights"
    path.write_bytes(b"weights")
    artifact = trackio.Artifact("weights", "model")
    artifact.add_file(path)
    trackio.Api().create_registry("models")
    run.log_artifact(artifact).wait().link(
        "registry-models/weights", aliases=["staging"]
    )
    run.flush()
    before = repository.workspace.resolve(
        scope, "registry", "models.weights", "staging"
    )
    destination = {**scope, "project_id": "destination"}
    repository.workspace.mutate(
        scope, "move", {"destination": destination}, operation_id="move"
    )
    bound = client.post(
        "/v1/runs",
        json={**destination, "trial_id": "new-trial", "config": {}},
        headers={"Authorization": "Bearer " + "s" * 32},
    )
    assert bound.status_code == 200, bound.text
    newer = CloudRun(
        "http://testserver",
        bound.json()["token"],
        "r",
        tmp_path / "new-spool",
        transport=client,
    )
    used = newer.use_artifact("registry-models/weights:staging")
    assert (tmp_path / "download" / "weights").exists() is False
    used.download(tmp_path / "download")
    assert (tmp_path / "download" / "weights").read_bytes() == b"weights"
    after = repository.workspace.resolve(
        destination, "registry", "models.weights", "staging"
    )
    assert after["digest"] == before["digest"]
    assert after["content"] == before["content"]
    lineage = newer._workspace_query("lineage", name="weights", version="v0")["links"]
    assert lineage and all(
        link["resolved_target_scope"] == destination for link in lineage
    )
    assert (
        client.post(
            "/v1/workspace/query",
            json={"action": "export"},
            headers={"Authorization": "Bearer " + run.token},
        ).status_code
        == 401
    )


def test_full06_service_cross_run_lineage_and_worker_visibility(sdk):
    run, repository, scope, client = sdk
    target = {**scope, "run_id": "other"}
    repository.bind(**target, config={})
    repository.workspace.publish(
        claims_for(repository, scope),
        "artifact",
        "weights",
        {"artifact_type": "file", "manifest": []},
        operation_id="publish",
    )
    repository.workspace.link(
        scope,
        target,
        "weights",
        "v0",
        "registry",
        "models.weights",
        operation_id="link",
    )
    assert len(repository.workspace.lineage(scope, "weights", "v0")) == 1
    body = {"action": "lineage", "name": "weights", "scope": scope}
    response = client.post(
        "/v1/workspace/query",
        json=body,
        headers={"Authorization": "Bearer " + run.token},
    )
    assert response.status_code == 200
    assert response.json()["links"] == []
    service = client.post(
        "/v1/workspace/query",
        json=body,
        headers={"Authorization": "Bearer " + "s" * 32},
    )
    assert service.status_code == 200, service.text
    assert service.json()["links"][0]["resolved_target_scope"] == target


def test_full07_step_filter_precedes_result_limit(sdk):
    _, repository, scope, _ = sdk
    for step in range(20):
        append(repository, scope, step)
    result = repository.workspace.compare(
        [scope], min_step=19, max_step=19, max_points=1
    )
    assert [point["step"] for point in result["runs"][0]["points"]] == [19]
    with pytest.raises(ValueError, match="COMPARE_POINT_LIMIT"):
        repository.workspace.compare([scope], min_step=18, max_points=1)


def test_full08_scalar_metrics_hide_deleted_run_keep_live_scoped_run(sdk):
    _, repository, scope, client = sdk
    other = {**scope, "run_id": "other"}
    repository.bind(**other, config={})
    append(repository, scope, 0)
    append(repository, other, 1)
    repository.workspace.mutate(scope, "delete", {}, operation_id="delete")
    response = client.get(
        "/v1/metrics",
        params={key: scope[key] for key in ("workspace_id", "project_id", "job_id")},
        headers={"Authorization": "Bearer " + "s" * 32},
    )
    assert response.status_code == 200, response.text
    assert [event["run_id"] for event in response.json()["events"]] == ["other"]
    assert repository.events("foreign", "p", "j") == []
    assert [entry["run_id"] for entry in repository.events("w", "p", "j", limit=1)] == [
        "other"
    ]
    assert repository.events("w", "p", "j", start=0, end=0) == []


def test_full01_database_failure_after_physical_delete_recovers_after_reopen(
    sdk, tmp_path
):
    _, repository, scope, _ = sdk
    objects = FileObjects(tmp_path / "recovery")
    digest = repository.put_import_object(scope, b"recover", objects)
    with repository.engine.begin() as connection:
        connection.execute(
            repository.workspace.upload_locations.update().values(created_at=1)
        )
    deleted = []

    class Storage:
        def delete(self, key):
            objects.delete(key)
            deleted.append(key)

    def fault(connection, cursor, statement, parameters, context, many):
        if deleted and statement.startswith("UPDATE "):
            raise RuntimeError("post-delete database fault")

    event.listen(repository.engine, "before_cursor_execute", fault)
    try:
        with pytest.raises(RuntimeError, match="post-delete database fault"):
            repository.workspace.collect_orphans(scope, Storage())
    finally:
        event.remove(repository.engine, "before_cursor_execute", fault)
    reopened = WorkspaceRepository(
        repository.url, schema=repository.runs.schema, initialize=False
    )
    try:
        with reopened.engine.connect() as connection:
            assert connection.execute(select(reopened.objects)).first() is None
            assert (
                connection.execute(
                    select(reopened.workspace.upload_locations.c.status)
                ).scalar_one()
                == "collecting"
            )
        with pytest.raises(ValueError, match="OBJECT"):
            reopened.workspace.publish(
                claims_for(reopened, scope),
                "artifact",
                "missing",
                {
                    "artifact_type": "file",
                    "manifest": [{"path": "file", "_object": digest, "size": 7}],
                },
                operation_id="missing",
            )
        assert reopened.workspace.collect_orphans(scope, objects)["removed"] == 1
        assert reopened.workspace.collect_orphans(scope, objects)["removed"] == 0
        reopened.put_import_object(scope, b"recover", objects)
        assert reopened.read_object(scope, digest, objects) == b"recover"
    finally:
        reopened.close()


def test_full04_late_put_cannot_ack_after_collection_and_remains_collectable(
    sdk, tmp_path
):
    _, repository, scope, _ = sdk
    objects = FileObjects(tmp_path / "late")
    payload = b"late"
    digest = hashlib.sha256(payload).hexdigest()

    class DelayedPut:
        def put(self, *args):
            with repository.engine.begin() as connection:
                connection.execute(
                    repository.workspace.upload_locations.update().values(created_at=1)
                )
            assert repository.workspace.collect_orphans(scope, objects)["removed"] == 1
            objects.put(*args)

    with pytest.raises(RuntimeError, match="UPLOAD_LEASE_EXPIRED"):
        repository.put_import_object(scope, payload, DelayedPut())
    with repository.engine.connect() as connection:
        assert connection.execute(select(repository.objects)).first() is None
    with repository.engine.begin() as connection:
        connection.execute(
            repository.workspace.upload_locations.update().values(created_at=1)
        )
    assert repository.workspace.collect_orphans(scope, objects)["removed"] == 1
    with pytest.raises(FileNotFoundError):
        objects.get(content_digest(scope) + "/" + digest, digest, len(payload))


def test_full02_legacy_query_revocation_while_waiting_for_body(sdk):
    _, repository, scope, client = sdk
    expires = time.time() + 600
    with repository.engine.begin() as connection:
        connection.execute(
            repository.migrations.update().values(legacy_tokens_until=expires)
        )
    token = issue_token("k" * 32, **scope, trial_id=None, expires_at=expires)
    assert (
        client.post(
            "/v1/workspace/query",
            json={"action": "export"},
            headers={"Authorization": "Bearer " + token},
        ).status_code
        == 200
    )

    def body():
        repository.revoke(scope)
        yield b'{"action":"export"}'

    response = client.post(
        "/v1/workspace/query",
        content=body(),
        headers={"Authorization": "Bearer " + token},
    )
    assert response.status_code == 401, response.text


def test_full04_v1_lost_prefix_recovered_from_move_receipt_with_bounded_inventory(
    sdk, tmp_path
):
    _, repository, scope, _ = sdk
    import os

    objects = FileObjects(tmp_path / "historical")
    payload = b"historical"
    digest = hashlib.sha256(payload).hexdigest()
    key = content_digest(scope) + "/" + digest
    objects.put(key, payload, digest, "application/octet-stream")
    os.utime(objects._path(key), (1, 1))
    destination = {**scope, "project_id": "destination"}
    repository.workspace.mutate(
        scope, "move", {"destination": destination}, operation_id="historical-move"
    )
    repository.put_import_object(destination, payload, objects)
    with repository.engine.begin() as connection:
        repository.workspace.upload_locations.drop(connection)
        connection.execute(repository.workspace.schema.update().values(version=1))
    upgraded = WorkspaceRepository(
        repository.url,
        schema=repository.runs.schema,
        initialize=False,
        initialize_workspace=True,
    )
    try:
        after = None
        for page in range(4):
            result = upgraded.workspace.collect_orphans(
                destination, objects, after=after, limit=1
            )
            after = result["next"]
            if after is None:
                break
        else:
            pytest.fail("historical inventory pagination did not terminate")
        with pytest.raises(FileNotFoundError):
            objects.get(key, digest, len(payload))
        assert upgraded.read_object(destination, digest, objects) == payload
    finally:
        upgraded.close()


def test_full02_postgres_holds_run_lock_until_query_read_completes(
    postgres_database, tmp_path
):
    url, schema = postgres_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    reading, release, revoking = Event(), Event(), Event()
    revoker_pid = []

    def observe(connection, cursor, statement, parameters, context, many):
        if (
            statement.startswith("SELECT ")
            and "trackio_workspace_resources" in statement
            and not reading.is_set()
        ):
            reading.set()
            assert release.wait(10)
        if (
            statement.startswith("UPDATE ")
            and "trackio_cloud_runs" in statement
            and current_thread().name.startswith("revoke")
        ):
            revoker_pid.append(
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            )
            revoking.set()

    with TestClient(
        create_app(config, repository, objects=FileObjects(tmp_path / "objects"))
    ) as client:
        bound = client.post(
            "/v1/runs",
            json={**scope, "config": {}},
            headers={"Authorization": "Bearer " + "s" * 32},
        ).json()
        for engine in set(repository._engines.values()):
            event.listen(engine, "before_cursor_execute", observe)
        try:
            with (
                ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="query"
                ) as queries,
                ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="revoke"
                ) as revokers,
            ):
                query = queries.submit(
                    client.post,
                    "/v1/workspace/query",
                    json={"action": "resources", "kind": "artifact"},
                    headers={"Authorization": "Bearer " + bound["token"]},
                )
                try:
                    assert reading.wait(10)
                    revoke = revokers.submit(repository.revoke, scope)
                    assert revoking.wait(10)
                    deadline = time.monotonic() + 5
                    blocked = False
                    while time.monotonic() < deadline:
                        with repository.engine.connect() as connection:
                            blocked = bool(
                                connection.execute(
                                    text("SELECT pg_blocking_pids(:pid)"),
                                    {"pid": revoker_pid[0]},
                                ).scalar_one()
                            )
                        if blocked:
                            break
                        time.sleep(0.01)
                    assert blocked, (
                        "revoke did not wait for the query transaction's run lock"
                    )
                    assert not revoke.done()
                finally:
                    release.set()
                assert query.result(timeout=10).status_code == 200
                revoke.result(timeout=10)
            assert (
                client.post(
                    "/v1/workspace/query",
                    json={"action": "export"},
                    headers={"Authorization": "Bearer " + bound["token"]},
                ).status_code
                == 401
            )
        finally:
            release.set()
            for engine in set(repository._engines.values()):
                event.remove(engine, "before_cursor_execute", observe)
            repository.close()


def test_full01_fresh_upload_generation_survives_older_gc_claim(
    sdk, tmp_path, monkeypatch
):
    _, repository, scope, _ = sdk
    store = repository.workspace
    objects = FileObjects(tmp_path / "replacement")
    payload = b"replacement"
    digest = repository.put_import_object(scope, payload, objects)
    with repository.engine.begin() as connection:
        connection.execute(store.upload_locations.update().values(created_at=1))
    transaction = store.transaction
    replacing = True

    @contextmanager
    def replace_after_claim(*args, **kwargs):
        nonlocal replacing
        with transaction(*args, **kwargs) as connection:
            yield connection
        if replacing:
            replacing = False
            repository.put_import_object(scope, payload, objects)

    monkeypatch.setattr(store, "transaction", replace_after_claim)
    assert store.collect_orphans(scope, objects)["removed"] == 0
    assert repository.read_object(scope, digest, objects) == payload
    assert store.collect_orphans(scope, objects)["removed"] == 0


def test_full01_deleted_run_collection_also_invalidates_before_partial_delete(
    sdk, tmp_path
):
    _, repository, scope, _ = sdk
    objects = FileObjects(tmp_path / "deleted")
    for payload in (b"one", b"two"):
        repository.put_import_object(scope, payload, objects)
    repository.workspace.mutate(scope, "delete", {}, operation_id="delete")
    deleted = []

    class FailingDelete:
        def delete(self, key):
            if deleted:
                raise OSError("partial deleted-run collection")
            objects.delete(key)
            deleted.append(key)

    with pytest.raises(OSError):
        repository.workspace.collect_objects(scope, FailingDelete())
    with repository.engine.connect() as connection:
        assert connection.execute(select(repository.objects)).first() is None
    assert repository.workspace.collect_objects(scope, objects)["removed"] == 1
    assert repository.workspace.collect_objects(scope, objects)["removed"] == 0
