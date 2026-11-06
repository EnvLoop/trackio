import time

from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, MetricsRepository, create_app
from trackio.cloud_client import CloudRun


def test_refresh_after_access_expiry_is_bounded_by_deadline_and_revocation(
    tmp_path, monkeypatch
):
    repository = MetricsRepository(f"sqlite:///{tmp_path / 'db.sqlite'}")
    config = CloudConfig("local", repository.url, "s" * 32, "k" * 32, token_ttl=60)
    now = time.time()
    with TestClient(create_app(config, repository)) as client:
        scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
        response = client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={**scope, "config": {}, "trial_id": "t", "renew_until": now + 300},
        )
        assert response.status_code == 200
        capability = response.json()
        assert capability["refresh_token"] != capability["token"]
        monkeypatch.setattr("trackio.cloud.time.time", lambda: now + 90)
        event = dict(
            batch_id="b",
            events=[
                dict(
                    event_id="e",
                    writer_id="wr",
                    trial_id="t",
                    step=7,
                    observed_at=now,
                    category="experiment",
                    metrics={"loss": 0.5},
                )
            ],
        )
        endpoint = "/v1/runs/r/events"
        assert (
            client.post(
                endpoint,
                json=event,
                headers={"Authorization": "Bearer " + capability["token"]},
            ).status_code
            == 401
        )
        fresh = client.post(
            "/v1/capabilities/refresh",
            headers={"Authorization": "Bearer " + capability["refresh_token"]},
        )
        assert fresh.status_code == 200
        token = fresh.json()["token"]
        assert (
            client.post(
                endpoint, json=event, headers={"Authorization": "Bearer " + token}
            ).status_code
            == 200
        )
        assert (
            client.get(
                "/v1/metrics",
                headers={"Authorization": "Bearer " + capability["refresh_token"]},
            ).status_code
            == 403
        )
        revoke = client.post(
            "/v1/runs/r/revoke",
            headers={"Authorization": "Bearer " + "s" * 32},
            json=scope,
        )
        assert revoke.status_code == 200
        assert (
            client.post(
                endpoint, json=event, headers={"Authorization": "Bearer " + token}
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/v1/capabilities/refresh",
                headers={"Authorization": "Bearer " + capability["refresh_token"]},
            ).status_code
            == 401
        )
        assert len(repository.events("w", "p", "j", trial_id="t")) == 1
    repository.close()


def test_refresh_stops_at_job_deadline_and_wrong_scope_cannot_revoke(
    tmp_path, monkeypatch
):
    repository = MetricsRepository(f"sqlite:///{tmp_path / 'db.sqlite'}")
    config = CloudConfig("local", repository.url, "s" * 32, "k" * 32, token_ttl=60)
    now = time.time()
    with TestClient(create_app(config, repository)) as client:
        scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
        bound = client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={**scope, "config": {}, "renew_until": now + 120},
        ).json()
        denied = client.post(
            "/v1/runs/r/revoke",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={**scope, "workspace_id": "another"},
        )
        assert denied.status_code == 403
        monkeypatch.setattr("trackio.cloud.time.time", lambda: now + 121)
        assert (
            client.post(
                "/v1/capabilities/refresh",
                headers={"Authorization": "Bearer " + bound["refresh_token"]},
            ).status_code
            == 401
        )
    repository.close()


def test_sdk_refreshes_an_expired_access_token_without_dropping_the_spooled_batch(
    tmp_path, monkeypatch
):
    repository = MetricsRepository(f"sqlite:///{tmp_path / 'db.sqlite'}")
    config = CloudConfig("local", repository.url, "s" * 32, "k" * 32, token_ttl=60)
    now = time.time()
    with TestClient(create_app(config, repository)) as client:
        scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
        bound = client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={**scope, "config": {}, "renew_until": now + 300},
        ).json()
        monkeypatch.setattr("trackio.cloud.time.time", lambda: now + 90)
        run = CloudRun(
            "http://testserver",
            bound["token"],
            "r",
            tmp_path / "spool",
            refresh_token=bound["refresh_token"],
            transport=client,
        )
        run.log({"train/loss": 0.25}, step=7)
        assert not list((tmp_path / "spool").glob("*.json"))
        assert run.token != bound["token"]
        assert repository.events("w", "p", "j")[0]["step"] == 7
        run.finish()
    repository.close()
