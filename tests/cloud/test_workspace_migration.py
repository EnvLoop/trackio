import importlib
import secrets
import subprocess
import sys
from uuid import uuid4

import pytest
from psycopg import sql
from sqlalchemy import event, inspect, select, text
from sqlalchemy.engine import make_url

from trackio.cloud import MetricsRepository
from trackio.cloud_objects import FileObjects
from trackio.cloud_workspace import WorkspaceRepository


def old_database(url, schema):
    repository = MetricsRepository(url, schema=schema)
    repository.bind("w", "p", "j", "r", {})
    with repository.engine.begin() as connection:
        for table in (
            repository.capabilities,
            repository.migrations,
            repository.objects,
            repository.documents,
        ):
            table.drop(connection)
        connection.execute(repository.schema.update().values(version=1))
    return repository


def test_workspace_v1_requires_explicit_upgrade_and_preserves_upload_journal(
    workspace_database, tmp_path
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    repository.put_import_object(scope, b"migration", FileObjects(tmp_path / "objects"))
    assert hasattr(repository.workspace, "upload_locations")
    with repository.engine.begin() as connection:
        repository.workspace.upload_locations.drop(connection)
        connection.execute(repository.workspace.schema.update().values(version=1))
    with pytest.raises(ValueError, match="INCOMPATIBLE_WORKSPACE_SCHEMA"):
        WorkspaceRepository(url, schema=schema, initialize=False)
    upgraded = WorkspaceRepository(
        url, schema=schema, initialize=False, initialize_workspace=True
    )
    with upgraded.engine.connect() as connection:
        assert (
            connection.execute(select(upgraded.workspace.schema.c.version)).scalar_one()
            == 2
        )
        assert (
            connection.execute(
                select(upgraded.workspace.upload_locations.c.status)
            ).scalar_one()
            == "complete"
        )
    upgraded.close()
    repository.close()


def test_real_cli_explicit_workspace_v1_upgrade_exits_without_listener(
    workspace_database, tmp_path
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    repository.bind("w", "p", "j", "r", {})
    with repository.engine.begin() as connection:
        repository.workspace.upload_locations.drop(connection)
        connection.execute(repository.workspace.schema.update().values(version=1))
    config = tmp_path / "workspace-owner.toml"
    (tmp_path / "service").write_text("s" * 32)
    (tmp_path / "signing").write_text("k" * 32)
    config.write_text(
        f'environment="local"\ndatabase_url="{url}"\nservice_token_file="service"\nsigning_key_file="signing"\n'
        + (f'database_schema="{schema}"\n' if schema else "")
    )
    for path in (config, tmp_path / "service", tmp_path / "signing"):
        path.chmod(0o600)
    arguments = [
        sys.executable,
        "-m",
        "trackio.cloud",
        "--config",
        str(config),
        "--migrate-workspace-from",
        "1",
    ]
    result = subprocess.run(arguments, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr.decode()
    with repository.engine.connect() as connection:
        assert connection.execute(select(repository.schema.c.version)).scalar_one() == 3
        assert (
            connection.execute(
                select(repository.workspace.schema.c.version)
            ).scalar_one()
            == 2
        )
    reopened = WorkspaceRepository(url, schema=schema, initialize=False)
    assert (
        reopened.workspace.list_runs(
            dict(workspace_id="w", project_id="p", job_id="j")
        )["runs"][0]["run_id"]
        == "r"
    )
    reopened.close()
    again = subprocess.run(arguments, capture_output=True, timeout=20)
    assert (
        again.returncode != 0 and b"WORKSPACE_MIGRATION_SOURCE_MISMATCH" in again.stderr
    )
    repository.close()


def test_owner_upgrade_builds_core_and_workspace_once_without_startup_ddl(
    workspace_database, monkeypatch
):
    url, schema = workspace_database
    original = old_database(url, schema)
    before = set(inspect(original.engine).get_table_names(schema=schema))
    with pytest.raises(ValueError, match="SCHEMA_NOT_INITIALIZED"):
        WorkspaceRepository(url, schema=schema, initialize=False)
    assert set(inspect(original.engine).get_table_names(schema=schema)) == before
    upgraded = WorkspaceRepository(
        url, schema=schema, initialize=False, migrate_from=1, initialize_workspace=True
    )
    tables = set(inspect(upgraded.engine).get_table_names(schema=schema))
    assert len(tables) == 21
    upgraded.close()
    statements = []
    module = importlib.import_module("trackio.cloud")
    create_engine = module.create_engine

    def monitored(*args, **kwargs):
        engine = create_engine(*args, **kwargs)
        event.listen(
            engine,
            "before_cursor_execute",
            lambda connection,
            cursor,
            statement,
            parameters,
            context,
            many: statements.append(statement),
        )
        return engine

    monkeypatch.setattr(module, "create_engine", monitored)
    reopened = WorkspaceRepository(url, schema=schema, initialize=False)
    assert (
        reopened.workspace.list_runs(
            dict(workspace_id="w", project_id="p", job_id="j")
        )["runs"][0]["run_id"]
        == "r"
    )
    assert not any(
        statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP ", "GRANT "))
        for statement in statements
    )
    reopened.close()
    original.close()


def test_real_owner_cli_migrates_workspace_for_restricted_staging_app(
    postgres_database, tmp_path
):
    url, schema = postgres_database
    suffix = uuid4().hex[:12]
    owner, app = "trackio_owner_" + suffix, "trackio_app_" + suffix
    password = secrets.token_urlsafe(24)
    module = importlib.import_module("trackio.cloud")
    engine = module.create_engine(url, hide_parameters=True)
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
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {}"
                ).format(
                    sql.Identifier(owner), sql.Identifier(schema), sql.Identifier(app)
                )
            )
        original = MetricsRepository(url, schema=schema, owner_role=owner)
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
        config = tmp_path / "owner.toml"
        (tmp_path / "service").write_text("s" * 32)
        (tmp_path / "signing").write_text("k" * 32)
        config.write_text(
            f'environment="staging"\ndatabase_url="{url}"\ndatabase_schema="{schema}"\nservice_token_file="service"\nsigning_key_file="signing"\n'
        )
        for path in (config, tmp_path / "service", tmp_path / "signing"):
            path.chmod(0o600)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "trackio.cloud",
                "--config",
                str(config),
                "--migrate-from",
                "1",
                "--owner-role",
                owner,
            ],
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr.decode()
        with engine.begin() as connection:
            for table in (
                "trackio_cloud_schema",
                "trackio_cloud_migrations",
                "trackio_workspace_schema",
            ):
                connection.connection.driver_connection.execute(
                    sql.SQL("REVOKE INSERT,UPDATE,DELETE ON {}.{} FROM {}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(app),
                    )
                )
            ownership = (
                connection.execute(
                    text("SELECT tableowner FROM pg_tables WHERE schemaname=:schema"),
                    {"schema": schema},
                )
                .scalars()
                .all()
            )
            assert len(ownership) == 21 and set(ownership) == {owner}
            connection.execute(text(f'SET LOCAL ROLE "{owner}"'))
            connection.connection.driver_connection.execute(
                sql.SQL("DROP TABLE {}.trackio_workspace_upload_locations").format(
                    sql.Identifier(schema)
                )
            )
            connection.connection.driver_connection.execute(
                sql.SQL("UPDATE {}.trackio_workspace_schema SET version=1").format(
                    sql.Identifier(schema)
                )
            )
        workspace_upgrade = subprocess.run(
            [
                sys.executable,
                "-m",
                "trackio.cloud",
                "--config",
                str(config),
                "--migrate-workspace-from",
                "1",
                "--owner-role",
                owner,
            ],
            capture_output=True,
            timeout=20,
        )
        assert workspace_upgrade.returncode == 0, workspace_upgrade.stderr.decode()
        app_url = (
            make_url(url)
            .set(username=app, password=password)
            .render_as_string(hide_password=False)
        )
        restricted = WorkspaceRepository(app_url, schema=schema, initialize=False)
        try:
            scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
            capability = restricted.capability(scope, None)
            claims = {
                **scope,
                "trial_id": None,
                "capability_id": capability["capability_id"],
            }
            objects = FileObjects(tmp_path / "restricted-objects")
            digest = restricted.put_import_object(scope, b"restricted-upload", objects)
            assert (
                restricted.read_object(scope, digest, objects) == b"restricted-upload"
            )
            assert (
                restricted.workspace.publish(
                    claims,
                    "logbook",
                    "notes",
                    {"pages": {"index.md": "synthetic"}},
                    operation_id="write",
                )["version"]
                == "v0"
            )
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
                    text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": role}
                ).first():
                    driver.execute(
                        sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role))
                    )
                    driver.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
        engine.dispose()


def test_workspace_failure_rolls_back_the_entire_postgres_v1_upgrade(
    postgres_database, monkeypatch
):
    url, schema = postgres_database
    original = old_database(url, schema)
    module = importlib.import_module("trackio.cloud_workspace")

    def fail(*args, **kwargs):
        raise ValueError("WORKSPACE_MIGRATION_FAULT")

    monkeypatch.setattr(module, "WorkspaceStore", fail)
    with pytest.raises(ValueError, match="WORKSPACE_MIGRATION_FAULT"):
        WorkspaceRepository(
            url,
            schema=schema,
            initialize=False,
            migrate_from=1,
            initialize_workspace=True,
        )
    with original.engine.connect() as connection:
        assert connection.execute(select(original.schema.c.version)).scalar_one() == 1
    assert len(inspect(original.engine).get_table_names(schema=schema)) == 5
    original.close()
