import hashlib
import os
import secrets
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from trackio.cloud import SCHEMA_VERSION, MetricsRepository
from trackio.cloud_objects import FileObjects
from trackio.cloud_workspace import WorkspaceRepository


def test_real_v1_owner_upgrade_grants_new_tables_to_restricted_existing_app(tmp_path):
    url = make_url(Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip())
    assert url.host == "127.0.0.1" and url.database.startswith("trackio_test_")
    suffix = uuid4().hex[:12]
    owner, app, schema = (
        prefix + suffix
        for prefix in ("trackio_owner_", "trackio_app_", "trackio_test_")
    )
    password = secrets.token_urlsafe(32)
    engine = create_engine(url, hide_parameters=True)
    try:
        with engine.begin() as connection:
            driver = connection.connection.driver_connection
            driver.execute(
                sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(owner))
            )
            driver.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"
                ).format(sql.Identifier(app), sql.Literal(password))
            )
            driver.execute(
                sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                    sql.Identifier(schema), sql.Identifier(owner)
                )
            )
            driver.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                    sql.Identifier(schema), sql.Identifier(app)
                )
            )
            driver.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
                ).format(
                    sql.Identifier(owner), sql.Identifier(schema), sql.Identifier(app)
                )
            )
        original = MetricsRepository(
            url.render_as_string(hide_password=False), schema=schema, owner_role=owner
        )
        original.bind("w", "p", "j", "r", {})
        with original.engine.begin() as connection:
            connection.execute(text(f'SET LOCAL ROLE "{owner}"'))
            for table in (
                original.capabilities,
                original.migrations,
                original.objects,
                original.documents,
            ):
                table.drop(connection)
            connection.execute(original.schema.update().values(version=1))
        original.close()
        app_url = url.set(username=app, password=password)
        with pytest.raises(ValueError, match="SCHEMA_NOT_INITIALIZED_OR_INCOMPATIBLE"):
            MetricsRepository(
                app_url.render_as_string(hide_password=False),
                schema=schema,
                initialize=False,
            )
        assert len(inspect(engine).get_table_names(schema=schema)) == 5
        upgraded = WorkspaceRepository(
            url.render_as_string(hide_password=False),
            schema=schema,
            initialize=False,
            migrate_from=1,
            owner_role=owner,
            initialize_workspace=True,
        )
        upgraded.close()
        with engine.begin() as connection:
            driver = connection.connection.driver_connection
            for table in (
                "trackio_cloud_schema",
                "trackio_cloud_migrations",
                "trackio_workspace_schema",
            ):
                driver.execute(
                    sql.SQL("REVOKE INSERT, UPDATE, DELETE ON {}.{} FROM {}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(app),
                    )
                )
        restricted = WorkspaceRepository(
            app_url.render_as_string(hide_password=False),
            schema=schema,
            initialize=False,
        )
        try:
            identity = restricted.bind("w", "p", "next-job", "next-run", {})
            capability = restricted.capability(identity, "trial")
            claims = restricted.authorize_capability(capability["capability_id"])
            storage = FileObjects(tmp_path / "objects")
            payload = b"synthetic"
            digest = hashlib.sha256(payload).hexdigest()
            restricted.put_object(
                claims, digest, payload, "application/octet-stream", storage
            )
            record = dict(
                record_id="summary",
                kind="summary",
                writer_id="writer",
                trial_id="trial",
                step=7,
                observed_at=100,
                payload={"accuracy": 0.9, "attachment": {"_object": digest}},
            )
            assert restricted.append_record(claims, record) == 1
            assert restricted.read_records(identity) == [record]
            assert restricted.read_object(identity, digest, storage) == payload
            assert (
                restricted.workspace.mutate(
                    identity,
                    "rename",
                    {"name": "after-migration"},
                    operation_id="rename",
                )["name"]
                == "after-migration"
            )
            assert (
                restricted.workspace.export(identity)["run"]["name"]
                == "after-migration"
            )
            with restricted.engine.connect() as connection:
                assert (
                    connection.execute(restricted.schema.select()).scalar_one()
                    == SCHEMA_VERSION
                )
                for statement in (
                    f'CREATE TABLE "{schema}".forbidden (id int)',
                    f'UPDATE "{schema}".trackio_cloud_schema SET version=99',
                    f'UPDATE "{schema}".trackio_workspace_schema SET version=99',
                ):
                    with pytest.raises(DBAPIError):
                        connection.execute(text(statement))
                    connection.rollback()
        finally:
            restricted.close()
    finally:
        with engine.begin() as connection:
            driver = connection.connection.driver_connection
            driver.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )
            for role in (app, owner):
                if connection.execute(
                    text("SELECT 1 FROM pg_roles WHERE rolname=:name"), {"name": role}
                ).first():
                    driver.execute(
                        sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role))
                    )
                    driver.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        engine.dispose()
