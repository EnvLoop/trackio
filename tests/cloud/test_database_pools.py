import hashlib
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import TimeoutError as PoolTimeout
from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, Conflict, MetricsRepository, create_app
from trackio.cloud_objects import FileObjects
from trackio.cloud_pools import database_workload
from trackio.cloud_workspace import WorkspaceRepository


def test_bulk_append_bounds_round_trips_and_keeps_idempotency_and_atomic_conflicts(
    workspace_database,
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    events = [
        dict(
            event_id=f"event{index}",
            writer_id="writer",
            trial_id=None,
            step=index,
            observed_at=index,
            category="experiment",
            metrics={"loss": index},
        )
        for index in range(100)
    ]
    statements = []

    def observe(connection, cursor, statement, parameters, context, many):
        statements.append(statement)

    try:
        event.listen(repository.engine, "before_cursor_execute", observe)
        assert (
            repository.append(
                **scope,
                batch={"batch_id": "first", "events": events},
                capability_id=capability["capability_id"],
            )
            == 100
        )
        assert len(statements) <= 10
        event.remove(repository.engine, "before_cursor_execute", observe)
        assert (
            repository.append(
                **scope,
                batch={"batch_id": "first", "events": events},
                capability_id=capability["capability_id"],
            )
            == 0
        )
        extra = {**events[0], "event_id": "extra"}
        assert (
            repository.append(
                **scope,
                batch={"batch_id": "overlap", "events": [*events, extra]},
                capability_id=capability["capability_id"],
            )
            == 1
        )
        with pytest.raises(Conflict, match="EVENT_CONTENT_CONFLICT"):
            repository.append(
                **scope,
                batch={
                    "batch_id": "conflict",
                    "events": [
                        {**extra, "event_id": "must_rollback"},
                        {**events[0], "metrics": {"loss": -1}},
                    ],
                },
                capability_id=capability["capability_id"],
            )
        assert (
            len(repository.events(workspace_id="w", project_id="p", job_id="j")) == 101
        )
    finally:
        repository.close()


def test_postgres_reads_and_writes_use_fixed_independent_pools(postgres_database):
    url, schema = postgres_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    try:
        write = repository.engine
        assert write.pool.size() == 10
        with database_workload("read"):
            read = repository.engine
            assert read is not write and read.pool.size() == 5
            assert repository.workspace.engine is read
        assert repository.workspace.engine is write
        held = [write.connect() for _ in range(10)]
        try:
            with database_workload("read"), repository.engine.connect() as connection:
                assert connection.scalar(text("SELECT 1")) == 1
        finally:
            for connection in held:
                connection.close()
        assert write.pool.checkedout() == read.pool.checkedout() == 0
        assert write.pool._max_overflow == read.pool._max_overflow == 0
    finally:
        repository.close()


def test_migration_timeout_is_local_and_runtime_connections_keep_short_budgets(
    postgres_database,
):
    url, schema = postgres_database
    observed = []

    def observe(repository, connection):
        observed.append(
            connection.scalar(
                text("SELECT setting FROM pg_settings WHERE name='statement_timeout'")
            )
        )

    repository = MetricsRepository(url, schema=schema, _schema_extension=observe)
    try:
        assert observed == ["60000"]
        for workload in ("write", "read"):
            with database_workload(workload), repository.engine.connect() as connection:
                assert (
                    connection.scalar(
                        text(
                            "SELECT setting FROM pg_settings WHERE name='statement_timeout'"
                        )
                    )
                    == "5000"
                )
                assert (
                    connection.scalar(
                        text(
                            "SELECT setting FROM pg_settings WHERE name='lock_timeout'"
                        )
                    )
                    == "1000"
                )
    finally:
        repository.close()


@pytest.mark.parametrize("size", [0, 4, 1001, True, "10"])
def test_invalid_pool_sizes_fail_before_database_access(size):
    with pytest.raises(ValueError, match="DATABASE_POOL_SIZE"):
        MetricsRepository(
            "postgresql+psycopg://synthetic:secret@127.0.0.1/missing",
            read_pool_size=size,
        )


def test_local_sqlite_keeps_one_engine_and_existing_pool_size(tmp_path):
    repository = WorkspaceRepository(
        f"sqlite:///{tmp_path / 'local.sqlite'}", initialize_workspace=True
    )
    try:
        write = repository.engine
        with database_workload("read"):
            assert repository.engine is write and repository.workspace.engine is write
        assert write.pool.size() == 3
    finally:
        repository.close()


@pytest.mark.parametrize("change", ["revoke", "delete"])
def test_workspace_object_reads_release_connections_during_storage_and_recheck_revocation(
    tmp_path, workspace_database, change
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}
    storage = FileObjects(tmp_path / "objects")
    payload = b"synthetic payload"
    digest = hashlib.sha256(payload).hexdigest()
    repository.put_object(claims, digest, payload, "application/octet-stream", storage)

    class CheckedStorage:
        revoke = False

        def get(self, key, digest, size):
            assert all(
                engine.pool.checkedout() == 0 for engine in repository._engines.values()
            )
            if self.revoke:
                if change == "revoke":
                    repository.revoke(scope)
                else:
                    repository.workspace.mutate(
                        scope, "delete", {}, operation_id="delete"
                    )
            return storage.get(key, digest, size)

    try:
        checked = CheckedStorage()
        assert repository.read_object(scope, digest, checked, claims=claims) == payload
        checked.revoke = True
        with pytest.raises(PermissionError):
            repository.read_object(scope, digest, checked, claims=claims)
        assert all(
            engine.pool.checkedout() == 0 for engine in repository._engines.values()
        )
    finally:
        repository.close()


def test_slow_query_keeps_write_requests_responsive_and_read_overload_bounded(
    postgres_database, monkeypatch
):
    url, schema = postgres_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    entered, release = Event(), Event()
    original = repository.events

    def slow_events(**kwargs):
        entered.set()
        assert release.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(repository, "events", slow_events)
    try:
        with (
            TestClient(create_app(config, repository)) as client,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            headers = {"Authorization": "Bearer " + "s" * 32}
            query = executor.submit(
                client.get,
                "/v1/metrics",
                headers=headers,
                params=dict(workspace_id="w", project_id="p", job_id="j"),
            )
            try:
                assert entered.wait(3)
                write = executor.submit(
                    client.post,
                    "/v1/runs",
                    headers=headers,
                    json=dict(
                        workspace_id="w",
                        project_id="p",
                        job_id="j",
                        run_id="r",
                        config={},
                    ),
                )
                assert write.result(timeout=3).status_code == 200
                for _ in range(4):
                    assert repository.request_limits["read"].acquire(blocking=False)
                try:
                    assert client.get("/ready").status_code == 503
                finally:
                    for _ in range(4):
                        repository.request_limits["read"].release()
            finally:
                release.set()
            assert query.result(timeout=3).status_code == 200
            assert client.get("/ready").status_code == 200
    finally:
        repository.close()


def test_workspace_records_never_checkout_a_second_connection(workspace_database):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})

    def reject_nested(connection, record, proxy):
        assert repository.engine.pool.checkedout() == 1

    try:
        event.listen(repository.engine, "checkout", reject_nested)
        assert repository.read_records(scope) == []
    finally:
        event.remove(repository.engine, "checkout", reject_nested)
        repository.close()


def test_http_get_and_post_select_different_workloads_and_busy_is_redacted(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'http.sqlite'}"
    repository = MetricsRepository(url)
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    selected = []

    def record_checkout(connection, record, proxy):
        from trackio.cloud_pools import current_workload

        selected.append(current_workload())

    event.listen(repository.engine, "checkout", record_checkout)
    try:
        with TestClient(create_app(config, repository)) as client:
            assert client.get("/ready").status_code == 200
            assert selected and set(selected) == {"read"}
            selected.clear()
            response = client.post(
                "/v1/runs",
                headers={"Authorization": "Bearer " + "s" * 32},
                json=dict(
                    workspace_id="w", project_id="p", job_id="j", run_id="r", config={}
                ),
            )
            assert response.status_code == 200
            assert selected and set(selected) == {"write"}

            def busy(*args, **kwargs):
                raise PoolTimeout("secret-canary")

            monkeypatch.setattr(repository.engine, "connect", busy)
            response = client.get("/ready")
            assert (
                response.status_code == 503 and response.headers["retry-after"] == "1"
            )
            assert "secret-canary" not in response.text
    finally:
        repository.close()
