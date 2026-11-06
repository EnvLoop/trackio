import importlib.util
import subprocess
import sys
import time

from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, MetricsRepository, create_app


def test_actual_v1_issued_worker_keeps_ack_after_upgrade_and_can_be_revoked(tmp_path):
    source = subprocess.run(
        ["git", "show", "cb40b822d72af389c84753d0e48ff196e7319841:trackio/cloud.py"],
        check=True,
        capture_output=True,
    ).stdout
    filename = tmp_path / "v1_cloud.py"
    filename.write_bytes(source)
    spec = importlib.util.spec_from_file_location(
        "trackio_v1_credentials_fixture", filename
    )
    previous = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = previous
    spec.loader.exec_module(previous)
    url = f"sqlite:///{tmp_path / 'v1.sqlite'}"
    config = previous.CloudConfig("local", url, "s" * 32, "k" * 32)
    original = previous.MetricsRepository(url)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    headers = {"Authorization": "Bearer " + "s" * 32}
    batch = {
        "batch_id": "b",
        "events": [
            dict(
                event_id="e",
                writer_id="wr",
                trial_id="t",
                step=7,
                observed_at=100,
                category="experiment",
                metrics={"loss": 0.5},
            )
        ],
    }
    with TestClient(previous.create_app(config, original)) as client:
        bound = client.post(
            "/v1/runs", headers=headers, json={**scope, "config": {}, "trial_id": "t"}
        ).json()
        untouched = client.post(
            "/v1/runs",
            headers=headers,
            json={**scope, "config": {}, "trial_id": "other"},
        ).json()
        worker = {"Authorization": "Bearer " + bound["token"]}
        assert (
            client.post("/v1/runs/r/events", headers=worker, json=batch).status_code
            == 200
        )
    original.close()
    migrated = MetricsRepository(url, initialize=False, migrate_from=1)
    current = CloudConfig("local", url, "s" * 32, "k" * 32)
    with TestClient(create_app(current, migrated)) as client:
        replay = client.post("/v1/runs/r/events", headers=worker, json=batch)
        assert replay.status_code == 200, replay.text
        assert replay.json()["accepted"] == 0
        assert (
            client.put(
                "/v1/runs/r/config", headers=worker, json={"lr": 0.1}
            ).status_code
            == 200
        )
        with migrated.engine.begin() as connection:
            connection.execute(
                migrated.migrations.update().values(legacy_tokens_until=time.time() - 1)
            )
        assert (
            client.post("/v1/runs/r/events", headers=worker, json=batch).status_code
            == 401
        )
        with migrated.engine.begin() as connection:
            connection.execute(
                migrated.migrations.update().values(
                    legacy_tokens_until=time.time() + 60
                )
            )
        assert (
            client.post("/v1/runs/r/events", headers=worker, json=batch).status_code
            == 200
        )
        assert (
            client.post("/v1/runs/r/revoke", headers=headers, json=scope).status_code
            == 200
        )
    migrated.close()
    restarted = MetricsRepository(url, initialize=False)
    with TestClient(create_app(current, restarted)) as client:
        assert (
            client.post("/v1/runs/r/events", headers=worker, json=batch).status_code
            == 401
        )
        later = {
            **batch,
            "batch_id": "other",
            "events": [{**batch["events"][0], "trial_id": "other"}],
        }
        assert (
            client.post(
                "/v1/runs/r/events",
                headers={"Authorization": "Bearer " + untouched["token"]},
                json=later,
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/v1/runs",
                headers=headers,
                json={**scope, "config": {}, "trial_id": "new"},
            ).status_code
            == 401
        )
    restarted.close()
    sys.modules.pop(spec.name, None)


def test_fresh_schema_rejects_unregistered_signed_tokens_without_a_migration_grace(
    tmp_path,
):
    from trackio.cloud import issue_token

    repository = MetricsRepository(f"sqlite:///{tmp_path / 'fresh.sqlite'}")
    scope = repository.bind("w", "p", "j", "r", {})
    token = issue_token("k" * 32, **scope, expires_at=time.time() + 60)
    config = CloudConfig("local", repository.url, "s" * 32, "k" * 32)
    batch = {
        "batch_id": "b",
        "events": [
            dict(
                event_id="e",
                writer_id="wr",
                trial_id=None,
                step=1,
                observed_at=100,
                category="experiment",
                metrics={"loss": 0.5},
            )
        ],
    }
    with TestClient(create_app(config, repository)) as client:
        assert (
            client.post(
                "/v1/runs/r/events",
                headers={"Authorization": "Bearer " + token},
                json=batch,
            ).status_code
            == 401
        )
        assert repository.events("w", "p", "j") == []
    repository.close()
