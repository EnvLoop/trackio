from datetime import datetime

import pytest
from starlette.testclient import TestClient

from trackio import cloud_dashboard
from trackio.cloud import CloudConfig, MetricsRepository, create_app

SERVICE = {"Authorization": "Bearer " + "s" * 32}
SCOPE = dict(workspace_id="ws", project_id="project", job_id="job", run_id="run")
WINDOW = dict(**SCOPE, trial_id=None, start=10, end=20)


@pytest.fixture
def dashboard(workspace_database):
    database, schema = workspace_database
    repo = MetricsRepository(database, schema=schema)
    scopes = [
        SCOPE,
        {**SCOPE, "run_id": "other", "job_id": "other-job"},
        {**SCOPE, "run_id": "private", "project_id": "private-project"},
    ]
    for scope in scopes:
        repo.bind(**scope, config={"hidden": "private"})
        events = [
            dict(
                event_id=str(index),
                writer_id=writer,
                trial_id=trial,
                step=step,
                observed_at=observed,
                category=category,
                metrics={"metric": index},
            )
            for index, (writer, trial, step, observed, category) in enumerate(
                [
                    ("A", "t1", 3, 5, "experiment"),
                    ("A", "t1", None, 11.25, "experiment"),
                    ("B", "t1", 42, 12, "experiment"),
                    ("A", "t2", 42, 13, "experiment"),
                    ("A", "t1", 42, 14, "compute"),
                    ("A", "t1", 43, 15, "compute"),
                    ("A", "t1", 44, 16, "compute"),
                ]
            )
        ]
        repo.append(**scope, batch={"batch_id": "batch", "events": events})
    config = CloudConfig("local", repo.url, "s" * 32, "k" * 32)
    with TestClient(create_app(config, repo)) as client:
        yield client
    repo.close()


def query(
    client, action="get_embed_context", params=None, scopes=None, headers=SERVICE
):
    return client.post(
        "/v1/dashboard/query",
        headers=headers,
        json={
            "scopes": [WINDOW] if scopes is None else scopes,
            "action": action,
            "params": {} if params is None else params,
        },
    )


def native_params(runs):
    return {
        "project": "project",
        "runs": [{"run": r["name"], "run_id": r["id"]} for r in runs],
    }


def test_scoped_native_series(dashboard):
    result = query(dashboard)
    assert result.status_code == 200, result.text
    context = result.json()["data"]
    assert context["project"] == "project" and context["readonly"] is True
    runs = context["runs"]
    assert {(r["run_id"], r["trial_id"], r["writer_id"]) for r in runs} == {
        ("run", "t1", "A"),
        ("run", "t1", "B"),
        ("run", "t2", "A"),
    }
    assert len({r["id"] for r in runs}) == 3 and not context["truncated"]
    selected = next(r for r in runs if (r["trial_id"], r["writer_id"]) == ("t1", "A"))
    params = native_params([selected])
    (entry,) = query(
        dashboard, "get_logs_batch", {**params, "scalar_only": True}
    ).json()["data"]
    assert entry["run_id"] == selected["id"] and not entry["truncated"]
    (row,) = entry["logs"]
    assert row["step"] is None and row["metric"] == 1
    assert datetime.fromisoformat(row["timestamp"]).timestamp() == 11.25
    (system,) = query(dashboard, "get_system_logs_batch", params).json()["data"]
    assert [r["step"] for r in system["logs"]] == [42, 43, 44]
    scope = {**WINDOW, "trial_id": "t2"}
    assert query(dashboard, scopes=[scope]).json()["data"]["runs"] == [
        r for r in runs if r["trial_id"] == "t2"
    ]
    assert query(dashboard, "get_logs_batch", params, [scope]).status_code == 422
    assert not query(dashboard, scopes=[{**WINDOW, "run_id": "private"}]).json()[
        "data"
    ]["runs"]


def test_service_scope_and_action_validation(dashboard):
    for headers in ({}, {"Authorization": "Bearer worker"}):
        assert query(dashboard, headers=headers).status_code == 403
        assert (
            dashboard.get(
                "/v1/dashboard/assets/index.html", headers=headers
            ).status_code
            == 403
        )
    for scopes in (
        [],
        [SCOPE],
        [WINDOW] * 65,
        [WINDOW, {**WINDOW, "workspace_id": "other"}],
        [WINDOW, {**WINDOW, "project_id": "other"}],
        [{**WINDOW, "start": 30}],
    ):
        assert query(dashboard, scopes=scopes).status_code == 422
    for action, params in (
        ("delete_run", {}),
        ("get_all_projects", {}),
        ("get_embed_context", {"workspace_id": "other"}),
        ("get_logs_batch", {"project": "other", "runs": []}),
        (
            "get_logs_batch",
            {"project": "project", "runs": [{"run_id": "unknown", "run": "guess"}]},
        ),
    ):
        assert query(dashboard, action, params).status_code == 422


def test_truncation_is_visible_per_category(dashboard, monkeypatch):
    assert query(dashboard).status_code == 200
    adapter = cloud_dashboard
    monkeypatch.setattr(adapter, "MAX_LOGS", 2)
    runs = query(dashboard).json()["data"]["runs"]
    params = native_params(
        [next(r for r in runs if r["trial_id"] == "t1" and r["writer_id"] == "A")]
    )
    (entry,) = query(dashboard, "get_system_logs_batch", params).json()["data"]
    assert entry["truncated"] and len(entry["logs"]) == 2
    assert not query(dashboard, "get_logs_batch", params).json()["data"][0]["truncated"]
    monkeypatch.setattr(adapter, "MAX_SERIES", 1)
    assert query(dashboard).json()["data"]["truncated"]


def test_only_native_bundle_assets(dashboard, tmp_path, monkeypatch):
    assert query(dashboard).status_code == 200
    adapter = cloud_dashboard
    root = tmp_path / "dist"
    (root / "assets").mkdir(parents=True)
    html = '<script type="module" src="/assets/embed-abc.js"></script>'
    (root / "embed.html").write_text(html)
    (root / "assets" / "standalone.js").write_text("export {}")
    (root / "assets" / "embed-abc.js").write_text("export {}")
    secret = tmp_path / "private.js"
    secret.write_text("private")
    (root / "assets" / "link.js").symlink_to(secret)
    monkeypatch.setattr(adapter, "DIST", root)
    assert "dashboard_embed_v1" in dashboard.get("/ready").json()["capabilities"]
    route = "/v1/dashboard/assets/"
    assert dashboard.get(route + "index.html", headers=SERVICE).text == html
    assert (
        dashboard.get(route + "assets/embed-abc.js", headers=SERVICE).status_code == 200
    )
    for name in (
        "assets/link.js",
        "assets/standalone.js",
        "embed.html",
        ".env",
        "assets/fake.map",
        "%2e%2e%2fprivate.js",
        "assets%5c..%5cprivate.js",
    ):
        assert dashboard.get(route + name, headers=SERVICE).status_code == 404
    (root / "assets" / "embed-abc.js").unlink()
    assert "dashboard_embed_v1" not in dashboard.get("/ready").json()["capabilities"]
    (root / "embed.html").unlink()
    assert "dashboard_embed_v1" not in dashboard.get("/ready").json()["capabilities"]


def test_native_metadata_and_metric_values_can_share_a_name():
    row = cloud_dashboard.log_row(
        {
            "metrics": {"step": 9, "train/loss": 3, "~step": 2, "__proto__/value": 4},
            "step": None,
            "observed_at": 11.25,
        }
    )
    assert row["step"] is None and row["~step"] == 9 and row["~~step"] == 2
    assert row["train/loss"] == 3 and row["~__proto__/value"] == 4


def test_batch_byte_limit_includes_native_metadata_and_reports_truncation(
    dashboard, monkeypatch
):
    context = query(dashboard).json()["data"]
    params = native_params(context["runs"][:1])
    size = len(query(dashboard, "get_system_logs_batch", params).content)
    monkeypatch.setattr(cloud_dashboard, "MAX_BYTES", size - 1)
    response = query(dashboard, "get_system_logs_batch", params)
    assert response.status_code == 200
    assert len(response.content) <= size - 1
    (entry,) = response.json()["data"]
    assert entry["truncated"] and entry["logs"][-1]["step"] == 44


def test_observation_timestamp_retains_submicrosecond_precision():
    row = cloud_dashboard.log_row(
        {"metrics": {}, "step": 0, "observed_at": 12.123456789}
    )
    assert row["timestamp"] == "1970-01-01T00:00:12.123456789+00:00"


def test_postgres_streams_bounded_history_in_chunks(postgres_database, monkeypatch):
    from psycopg import ServerCursor

    database, schema = postgres_database
    repo = MetricsRepository(database, schema=schema)
    try:
        repo.bind(**SCOPE, config={})
        for offset in range(0, 5000, 200):
            repo.append(
                **SCOPE,
                batch={
                    "batch_id": str(offset),
                    "events": [
                        dict(
                            event_id=str(step),
                            step=step,
                            writer_id="writer",
                            trial_id="trial",
                            observed_at=step,
                            category="experiment",
                            metrics={"value": step},
                        )
                        for step in range(offset, offset + 200)
                    ],
                },
            )
        scopes = [{**WINDOW, "start": 0, "end": 5000}]
        request = {"action": "get_embed_context", "params": {}, "scopes": scopes}
        runs = cloud_dashboard.query(repo, request)["data"]["runs"]
        request.update(action="get_logs_batch", params=native_params(runs))
        sizes = []
        fetchmany = ServerCursor.fetchmany

        def count_fetch(cursor, size=0):
            sizes.append(size)
            return fetchmany(cursor, size)

        monkeypatch.setattr(ServerCursor, "fetchmany", count_fetch)
        (entry,) = cloud_dashboard.query(repo, request)["data"]
        assert len(entry["logs"]) == 5000 and not entry["truncated"]
        assert 1 < len(sizes) <= 100, (len(sizes), set(sizes))
        assert max(sizes) <= 256
    finally:
        repo.close()
