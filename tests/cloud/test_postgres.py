import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from trackio.cloud import SCHEMA_VERSION, Conflict, MetricsRepository


def test_explicit_postgres_migration_preserves_bound_run_and_durable_revocation():
    url = Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
    address = urlsplit(url)
    assert address.hostname == "127.0.0.1" and address.path.startswith("/trackio_test_")
    name = "trackio_test_" + uuid4().hex[:12]
    repository = MetricsRepository(url, schema=name)
    identity = repository.bind("w", "p", "j", "r", {})
    try:
        with repository.engine.begin() as connection:
            repository.capabilities.drop(connection)
            repository.migrations.drop(connection)
            connection.execute(repository.schema.update().values(version=1))
        with pytest.raises(ValueError, match="SCHEMA_NOT_INITIALIZED_OR_INCOMPATIBLE"):
            MetricsRepository(url, schema=name, initialize=False)
        assert not inspect(repository.engine).has_table(
            "trackio_cloud_capabilities", schema=name
        )
        migrated = MetricsRepository(url, schema=name, initialize=False, migrate_from=1)
        capability = migrated.capability(identity, "t")
        assert (
            migrated.authorize_capability(capability["capability_id"])["run_id"] == "r"
        )
        migrated.revoke(identity)
        migrated.close()
        reopened = MetricsRepository(url, schema=name, initialize=False)
        with reopened.engine.connect() as connection:
            assert (
                connection.execute(reopened.schema.select()).scalar_one()
                == SCHEMA_VERSION
            )
        with pytest.raises(PermissionError, match="REVOKED_OR_EXPIRED"):
            reopened.authorize_capability(capability["capability_id"])
        reopened.close()
    finally:
        with repository.engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
        repository.close()


def test_real_postgres_restart_concurrency_and_transaction_rollback():
    url = Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
    address = urlsplit(url)
    assert address.hostname == "127.0.0.1" and address.port is not None
    assert address.path.startswith("/trackio_test_")
    repository = MetricsRepository(url)
    job_id, run_id = "job_" + uuid4().hex, "run_" + uuid4().hex
    repository.bind("w1", "p1", job_id, run_id, {"model": "synthetic"})
    event = {
        "event_id": "e1",
        "writer_id": "wr1",
        "trial_id": "t1",
        "step": 7,
        "observed_at": 100,
        "category": "experiment",
        "metrics": {"loss": 0.5},
    }
    batch = {"batch_id": "b1", "events": [event]}
    with ThreadPoolExecutor(max_workers=3) as pool:
        accepted = list(
            pool.map(
                lambda _: repository.append("w1", "p1", job_id, run_id, batch), range(6)
            )
        )
    assert sum(accepted) == 1
    try:
        repository.append(
            "w1",
            "p1",
            job_id,
            run_id,
            {
                "batch_id": "b2",
                "events": [
                    {**event, "event_id": "e2"},
                    {**event, "metrics": {"loss": 9}},
                ],
            },
        )
    except Conflict:
        pass
    else:
        raise AssertionError("Expected conflict and full rollback")
    repository.close()
    reopened = MetricsRepository(url, initialize=False)
    assert len(reopened.events("w1", "p1", job_id, trial_id="t1")) == 1
    assert reopened.events("w1", "p1", job_id, trial_id="other") == []
    assert reopened.events("w2", "p1", job_id) == []
    assert reopened.events("w1", "p1", job_id)[0]["step"] == 7
    reopened.close()


def test_named_schema_does_not_touch_platform_tables_and_app_cannot_create_tables():
    url = Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
    address = urlsplit(url)
    assert address.hostname == "127.0.0.1" and address.path.startswith("/trackio_test_")
    name = "trackio_test_" + uuid4().hex[:12]
    role = name + "_app"
    password = uuid4().hex
    repository = MetricsRepository(url, schema=name)
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE"
            )
        )
        connection.execute(text(f'GRANT USAGE ON SCHEMA "{name}" TO "{role}"'))
        connection.execute(
            text(
                f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "{name}" TO "{role}"'
            )
        )
    try:
        tables = set(inspect(repository.engine).get_table_names(schema=name))
        assert tables == {
            "trackio_cloud_schema",
            "trackio_cloud_runs",
            "trackio_cloud_events",
            "trackio_cloud_batches",
            "trackio_cloud_configs",
            "trackio_cloud_capabilities",
            "trackio_cloud_migrations",
        }
        repository.bind("w", "p", "job", "run", {})
        repository.close()
        reopened = MetricsRepository(url, schema=name, initialize=False)
        assert reopened.events("w", "p", "job") == []
        reopened.close()
        app_url = (
            make_url(url)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
        )
        app = MetricsRepository(app_url, schema=name, initialize=False)
        app.bind("w", "p", "job2", "run2", {})
        assert app.events("w", "p", "job2") == []
        with pytest.raises(DBAPIError) as denied:
            with app.engine.begin() as connection:
                connection.execute(
                    text(f'CREATE TABLE "{name}".forbidden (id integer)')
                )
        assert denied.value.orig.sqlstate == "42501"
        app.close()
        with pytest.raises(ValueError, match="PRIVATE_POSTGRESQL_SCHEMA"):
            MetricsRepository(url, schema="public")
    finally:
        with repository.engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{name}" CASCADE'))
            connection.execute(text(f'DROP OWNED BY "{role}"'))
            connection.execute(text(f'DROP ROLE "{role}"'))
        repository.close()
