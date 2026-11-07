import pytest
from sqlalchemy import inspect

from trackio.cloud import MetricsRepository


def test_explicit_v1_migration_preserves_events_and_startup_never_migrates(tmp_path):
    url = f"sqlite:///{tmp_path / 'v1.sqlite'}"
    original = MetricsRepository(url)
    original.bind("w", "p", "j", "r", {"lr": 0.1})
    original.append(
        "w",
        "p",
        "j",
        "r",
        {
            "batch_id": "b",
            "events": [
                dict(
                    event_id="e",
                    writer_id="wr",
                    trial_id=None,
                    step=3,
                    observed_at=100,
                    category="experiment",
                    metrics={"loss": 0.5},
                )
            ],
        },
    )
    with original.engine.begin() as connection:
        original.capabilities.drop(connection)
        original.migrations.drop(connection)
        connection.execute(original.schema.update().values(version=1))
    with pytest.raises(ValueError, match="SCHEMA_NOT_INITIALIZED_OR_INCOMPATIBLE"):
        MetricsRepository(url, initialize=False)
    assert not inspect(original.engine).has_table("trackio_cloud_capabilities")
    original.close()
    migrated = MetricsRepository(url, initialize=False, migrate_from=1)
    assert migrated.events("w", "p", "j")[0]["step"] == 3
    assert inspect(migrated.engine).has_table("trackio_cloud_capabilities")
    migrated.close()
    reopened = MetricsRepository(url, initialize=False)
    assert len(reopened.events("w", "p", "j")) == 1
    reopened.close()


def test_unknown_version_rejects_before_any_additive_ddl(tmp_path):
    url = f"sqlite:///{tmp_path / 'future.sqlite'}"
    original = MetricsRepository(url)
    with original.engine.begin() as connection:
        original.capabilities.drop(connection)
        original.migrations.drop(connection)
        connection.execute(original.schema.update().values(version=99))
    with pytest.raises(ValueError, match="INCOMPATIBLE_SCHEMA"):
        MetricsRepository(url)
    assert not inspect(original.engine).has_table("trackio_cloud_capabilities")
    original.close()
