import json

import pytest
from starlette.testclient import TestClient

from trackio import cloud_metrics
from trackio.cloud import CloudConfig, MetricsRepository, create_app


@pytest.fixture(params=["sqlite", "postgres"])
def metrics(request, tmp_path):
    database, schema = (
        request.getfixturevalue("postgres_database")
        if request.param == "postgres"
        else (f"sqlite:///{tmp_path / 'metrics.sqlite'}", None)
    )
    repository = MetricsRepository(database, schema=schema)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    for offset in range(0, 2300, 200):
        repository.append(
            **scope,
            batch={
                "batch_id": str(offset),
                "events": [
                    dict(
                        event_id=f"e{step:04}",
                        writer_id="writer",
                        trial_id="trial",
                        step=step,
                        observed_at=step,
                        category="compute" if step < 2200 else "experiment",
                        metrics={"cpu": 1}
                        if step < 2200
                        else {"train/loss": step / 10, "train/entropy": 2},
                    )
                    for step in range(offset, min(offset + 200, 2300))
                ],
            },
        )
    config = CloudConfig(
        "local", repository.url, "s" * 32, "k" * 32, database_schema=schema
    )
    with TestClient(create_app(config, repository)) as client:
        yield client, repository
    repository.close()


def query(client, **updates):
    parameters = dict(
        workspace_id="w",
        project_id="p",
        job_id="j",
        category="experiment",
        start=0,
        end=2300,
        metric="train/loss",
    )
    parameters.update(updates)
    return client.get(
        "/v1/metrics",
        params=parameters,
        headers={"Authorization": "Bearer " + "s" * 32},
    )


def test_source_filters_before_sampling_and_retains_real_steps(metrics):
    client, _ = metrics
    response = query(client, max_points=5)
    assert response.status_code == 200, response.text
    page = response.json()
    assert 2 <= len(page["events"]) <= 5 and page["sampled"]
    assert page["events"][0]["step"] == 2200 and page["events"][-1]["step"] == 2299
    assert all(
        event["metrics"] == {"train/loss": event["step"] / 10}
        for event in page["events"]
    )
    assert page["next_cursor"] is None


def test_raw_pages_preserve_all_points_and_bind_cursor_to_scope_and_filter(metrics):
    client, _ = metrics
    cursor, steps = None, []
    while True:
        options = dict(mode="raw", limit=17)
        if cursor:
            options["cursor"] = cursor
        response = query(client, **options)
        assert response.status_code == 200, response.text
        page = response.json()
        assert not page["sampled"]
        steps.extend(event["step"] for event in page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            break
        assert (
            query(
                client, mode="raw", limit=17, cursor=cursor, trial_id="foreign"
            ).status_code
            == 422
        )
        assert (
            query(client, mode="raw", limit=17, cursor=cursor + "bad").status_code
            == 422
        )
        assert query(client, mode="raw", limit=18, cursor=cursor).status_code == 422
        assert (
            query(
                client, mode="raw", limit=17, cursor=cursor, metric="train/entropy"
            ).status_code
            == 422
        )
    assert steps == list(range(2200, 2300))


def test_raw_cursor_disambiguates_same_timestamp_and_event_id_between_runs(metrics):
    client, repository = metrics
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r2")
    repository.bind(**scope, config={})
    repository.append(
        **scope,
        batch={
            "batch_id": "tie",
            "events": [
                {
                    "event_id": "e2299",
                    "writer_id": "writer",
                    "trial_id": "trial",
                    "step": 9999,
                    "observed_at": 2299,
                    "category": "experiment",
                    "metrics": {"train/loss": 0.25},
                }
            ],
        },
    )
    first = query(client, mode="raw", limit=1, start=2299).json()
    second = query(
        client, mode="raw", limit=1, start=2299, cursor=first["next_cursor"]
    ).json()
    assert [
        (page["events"][0]["run_id"], page["events"][0]["event_id"])
        for page in (first, second)
    ] == [
        ("r", "e2299"),
        ("r2", "e2299"),
    ]
    assert second["next_cursor"] is None and not second["truncated"]


def test_byte_bounded_raw_pages_resume_from_last_returned_event(metrics, monkeypatch):
    client, _ = metrics
    monkeypatch.setattr(cloud_metrics, "MAX_QUERY_BYTES", 1200)
    cursor, steps = None, []
    while True:
        options = dict(mode="raw", limit=500)
        if cursor:
            options["cursor"] = cursor
        response = query(client, **options)
        assert response.status_code == 200, response.text
        page = response.json()
        assert len(json.dumps(page["events"], separators=(",", ":")).encode()) <= 1200
        assert not page["sampled"]
        steps.extend(event["step"] for event in page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert steps == list(range(2200, 2300))


def test_query_scope_and_limits_cannot_be_bypassed(metrics):
    client, _ = metrics
    for options in (
        {"max_points": 1},
        {"max_points": 2001},
        {"mode": "raw", "limit": 501},
    ):
        assert query(client, **options).status_code == 422
    response = client.get(
        "/v1/metrics",
        params={
            "workspace_id": "other",
            "project_id": "p",
            "job_id": "j",
            "category": "experiment",
            "start": 0,
            "end": 2300,
        },
        headers={"Authorization": "Bearer " + "s" * 32},
    )
    assert response.status_code == 200 and response.json()["events"] == []
    assert (
        client.get(
            "/v1/metrics", headers={"Authorization": "Bearer worker-token"}
        ).status_code
        == 403
    )


def test_legacy_query_is_unchanged_and_selected_query_uses_same_route(metrics):
    client, _ = metrics
    response = client.get(
        "/v1/metrics",
        params={"workspace_id": "w", "project_id": "p", "job_id": "j"},
        headers={"Authorization": "Bearer " + "s" * 32},
    )
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"events", "truncated"}
    assert len(body["events"]) == 2000 and body["truncated"]
    assert body["events"][0]["step"] == 0 and body["events"][-1]["step"] == 1999
    assert query(client, max_points=5).json()["events"][0]["step"] == 2200
    assert client.get("/v1/metrics/query").status_code == 404
    assert "metric_query_v1" in client.get("/ready").json()["capabilities"]
