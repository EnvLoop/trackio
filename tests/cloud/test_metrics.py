import json
import time

import pytest
from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, MetricsRepository, create_app, issue_token


def test_private_server_config_resolves_relative_keys_and_hides_secrets(tmp_path):
    for name in ("service", "signing"):
        path = tmp_path / name
        path.write_text(name * 32)
        path.chmod(0o600)
    path = tmp_path / "server.toml"
    path.write_text(
        'environment="local"\ndatabase_url="sqlite:///synthetic.sqlite"\n'
        'service_token_file="service"\nsigning_key_file="signing"\n'
    )
    path.chmod(0o600)
    config = CloudConfig.from_file(path)
    assert config.service_token == "service" * 32
    assert "synthetic.sqlite" not in repr(config) and "serviceservice" not in repr(
        config
    )
    (tmp_path / "service").chmod(0o644)
    with pytest.raises(ValueError, match="PRIVATE_CONFIG"):
        CloudConfig.from_file(path)


def test_spool_errors_do_not_change_training_success(cloud, tmp_path, monkeypatch):
    from trackio.cloud_client import ManagedCloudRun

    client, _, _ = cloud
    binding = bind(client)
    run = ManagedCloudRun(
        "http://testserver",
        binding["token"],
        "run1",
        tmp_path / "spool",
        transport=client,
    )

    def unavailable(*args):
        raise OSError("synthetic disk full")

    monkeypatch.setattr(run, "_write", unavailable)
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_GAP"):
        run.config.update({"max_steps": 3})
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_GAP"):
        run.log({"loss": 0.5}, step=1)
    with pytest.warns(RuntimeWarning):
        status = run.finish(timeout=0.1)
    assert status["committed"] is False and status["dropped"] == 3


@pytest.fixture
def cloud(tmp_path):
    repository = MetricsRepository(f"sqlite:///{tmp_path / 'metrics.sqlite'}")
    config = CloudConfig(
        environment="local",
        database_url=repository.url,
        service_token="s" * 32,
        signing_key="k" * 32,
    )
    with TestClient(create_app(config, repository)) as client:
        yield client, repository, config
    repository.close()


def bind(client, workspace="w1", project="p1", job="job1", run="run1"):
    response = client.post(
        "/v1/runs",
        headers={"Authorization": "Bearer " + "s" * 32},
        json={
            "workspace_id": workspace,
            "project_id": project,
            "job_id": job,
            "run_id": run,
            "config": {"lr": 0.1},
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def batch(value=0.5, event="event1"):
    return {
        "batch_id": "batch1",
        "events": [
            {
                "event_id": event,
                "writer_id": "writer1",
                "trial_id": None,
                "step": 7,
                "observed_at": 100.0,
                "category": "experiment",
                "metrics": {"loss": value},
            }
        ],
    }


def test_commit_ack_replay_conflict_and_scope(cloud):
    client, repository, _ = cloud
    run = bind(client)
    headers = {"Authorization": "Bearer " + run["token"]}
    response = client.post("/v1/runs/run1/events", json=batch(), headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 1
    assert (
        client.post("/v1/runs/run1/events", json=batch(), headers=headers).json()[
            "accepted"
        ]
        == 0
    )
    assert (
        client.post("/v1/runs/run1/events", json=batch(9), headers=headers).status_code
        == 409
    )
    assert (
        client.post("/v1/runs/other/events", json=batch(), headers=headers).status_code
        == 403
    )
    assert client.get("/v1/runs", headers=headers).status_code == 403
    events = repository.events("w1", "p1", "job1")
    assert len(events) == 1 and events[0]["metrics"]["loss"] == 0.5
    assert repository.events("w2", "p1", "job1") == []


def test_integer_valued_float_replay_preserves_numeric_identity(cloud):
    client, repository, _ = cloud
    binding = bind(client)
    headers = {"Authorization": "Bearer " + binding["token"]}
    original = batch(12.0)
    replay = batch(12)
    assert (
        client.post("/v1/runs/run1/events", json=original, headers=headers).status_code
        == 200
    )
    response = client.post("/v1/runs/run1/events", json=replay, headers=headers)
    assert response.status_code == 200
    assert response.json()["accepted"] == 0
    assert len(repository.events("w1", "p1", "job1")) == 1


def test_atomic_batch_and_immutable_binding(cloud):
    client, repository, _ = cloud
    run = bind(client)
    assert (
        client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={
                "workspace_id": "w2",
                "project_id": "p1",
                "job_id": "job1",
                "run_id": "run1",
                "config": {},
            },
        ).status_code
        == 409
    )
    invalid = batch()
    invalid["events"].append({**invalid["events"][0], "event_id": "bad", "step": -1})
    assert (
        client.post(
            "/v1/runs/run1/events",
            json=invalid,
            headers={"Authorization": "Bearer " + run["token"]},
        ).status_code
        == 422
    )
    assert repository.events("w1", "p1", "job1") == []


def test_expired_token_and_unsupported_operations(cloud):
    client, _, config = cloud
    bind(client)
    expired = issue_token(
        config.signing_key, "w1", "p1", "job1", "run1", expires_at=time.time() - 1
    )
    assert (
        client.post(
            "/v1/runs/run1/events",
            json=batch(),
            headers={"Authorization": "Bearer " + expired},
        ).status_code
        == 401
    )
    assert (
        client.get(
            "/api/query", headers={"Authorization": "Bearer " + "s" * 32}
        ).status_code
        == 404
    )
    assert client.get("/ready").json()["schema_version"] == 1


def test_repository_restart_persists_and_config_is_validated(tmp_path):
    path = f"sqlite:///{tmp_path / 'db.sqlite'}"
    repository = MetricsRepository(path)
    repository.bind("w1", "p1", "j1", "r1", {"model": "synthetic"})
    repository.append("w1", "p1", "j1", "r1", batch())
    repository.close()
    reopened = MetricsRepository(path)
    assert reopened.events("w1", "p1", "j1")[0]["step"] == 7
    reopened.close()
    with pytest.raises(ValueError, match="PostgreSQL"):
        CloudConfig(
            environment="staging",
            database_url=path,
            service_token="s" * 32,
            signing_key="k" * 32,
        )


def test_sdk_spools_before_send_and_replays_without_sqlite(
    cloud, tmp_path, monkeypatch
):
    from trackio import cloud_client

    client, repository, _ = cloud
    binding = bind(client)
    run = cloud_client.CloudRun(
        "http://testserver",
        binding["token"],
        "run1",
        tmp_path / "spool",
        transport=client,
    )
    original = client.post
    monkeypatch.setattr(
        client,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")),
    )
    with pytest.raises(OSError, match="offline"):
        run.log({"loss": 0.25, "train/global_step": 11})
    pending = list((tmp_path / "spool").glob("*.json"))
    assert len(pending) == 1
    monkeypatch.setattr(client, "post", original)
    recovered = cloud_client.CloudRun(
        "http://testserver",
        binding["token"],
        "run1",
        tmp_path / "spool",
        transport=client,
    )
    recovered.flush()
    assert not list((tmp_path / "spool").glob("*.json"))
    assert repository.events("w1", "p1", "job1")[0]["step"] == 11
    recovered.log({"loss": 0.1}, step=12)
    recovered.finish()
    assert len(repository.events("w1", "p1", "job1")) == 2


def test_public_sdk_cloud_init_uses_bound_context_without_local_database(
    cloud, tmp_path, monkeypatch
):
    import trackio
    from trackio import cloud_client

    client, repository, _ = cloud
    binding = bind(client)
    context = tmp_path / "context.json"
    context.write_text(
        json.dumps(
            {
                "server_url": "http://127.0.0.1",
                "token": binding["token"],
                "run_id": "run1",
                "spool_dir": str(tmp_path / "spool"),
            }
        )
    )
    context.chmod(0o600)
    monkeypatch.setenv("TRACKIO_ENVLOOP_CONTEXT_FILE", str(context))
    monkeypatch.setattr(cloud_client.httpx2, "Client", lambda **kwargs: client)
    monkeypatch.setattr(
        trackio.SQLiteStorage,
        "validate_project_name",
        lambda *args: pytest.fail("SQLite path used"),
    )
    databases = set(tmp_path.rglob("*.sqlite"))
    run = trackio.init(
        "display-project", config={"learning_rate": 0.1, "secret_token": "do-not-send"}
    )
    trackio.config.update({"max_steps": 3}, allow_val_change=True)
    trackio.log({"train/loss": 0.2, "train/global_step": 2})
    trackio.finish()
    assert run.config == {"learning_rate": 0.1, "max_steps": 3}
    assert repository.events("w1", "p1", "job1")[0]["step"] == 2
    assert set(tmp_path.rglob("*.sqlite")) == databases


def test_public_sdk_outage_is_buffered_and_does_not_fail_training(
    cloud, tmp_path, monkeypatch
):
    from trackio.cloud_client import ManagedCloudRun

    client, repository, _ = cloud
    binding = bind(client)
    original = client.post
    monkeypatch.setattr(
        client,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")),
    )
    run = ManagedCloudRun(
        "http://testserver",
        binding["token"],
        "run1",
        tmp_path / "async",
        transport=client,
    )
    started = time.monotonic()
    run.log({"loss": 0.25}, step=1)
    assert time.monotonic() - started < 0.5
    assert len(list(run.directory.glob("*.json"))) == 1
    monkeypatch.setattr(client, "post", original)
    status = run.finish(timeout=3)
    assert status["committed"] and status["pending"] == 0
    assert repository.events("w1", "p1", "job1")[0]["step"] == 1
