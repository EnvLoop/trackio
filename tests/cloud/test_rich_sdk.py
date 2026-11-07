import hashlib

from PIL import Image
from starlette.testclient import TestClient

import trackio
from trackio.cloud import CloudConfig, MetricsRepository, create_app
from trackio.cloud_client import CloudRun
from trackio.cloud_objects import FileObjects


def test_sdk_rich_payloads_use_managed_objects_and_replay_without_local_sqlite(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'server.sqlite'}"
    repository = MetricsRepository(url)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    objects = FileObjects(tmp_path / "objects")

    def forbidden(*args, **kwargs):
        raise AssertionError("managed SDK must not use upstream SQLite")

    monkeypatch.setattr(
        "trackio.sqlite_storage.SQLiteStorage.get_or_create_run",
        forbidden,
        raising=False,
    )
    with TestClient(create_app(config, repository, objects=objects)) as client:
        bound = client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + "s" * 32},
            json={**scope, "config": {}},
        ).json()
        run = CloudRun(
            "http://testserver",
            bound["token"],
            "r",
            tmp_path / "spool",
            transport=client,
        )
        monkeypatch.setattr(run, "_send", lambda: None)
        run.log(
            {
                "loss": 0.5,
                "preview": trackio.Image(Image.new("RGB", (2, 2))),
                "rows": trackio.Table(columns=["answer"], data=[[42]]),
                "trace": trackio.Trace(
                    messages=[{"role": "user", "content": "synthetic"}]
                ),
                "histogram": trackio.Histogram([1, 2]),
                "report": trackio.Markdown("# Result"),
            },
            step=7,
        )
        run.summary["accuracy"] = 0.9
        run.alert("Synthetic", text="Synthetic warning")
        assert list(run.directory.glob("*.blob"))
        recovered = CloudRun(
            "http://testserver", bound["token"], "r", run.directory, transport=client
        )
        recovered.flush()
        rows = repository.read_records(scope)
        history = next(row for row in rows if row["kind"] == "history")
        assert history["step"] == 7
        assert history["payload"]["rows"]["data"] == [{"answer": 42}]
        assert history["payload"]["trace"]["messages"][0]["content"] == "synthetic"
        digest = history["payload"]["preview"]["_object"]
        payload = repository.read_object(scope, digest, objects)
        assert hashlib.sha256(payload).hexdigest() == digest
        assert any(
            row["kind"] == "summary" and row["payload"] == {"accuracy": 0.9}
            for row in rows
        )
        assert any(row["kind"] == "alert" for row in rows)
        assert len(repository.events("w", "p", "j")) == 1
        recovered.flush()
        assert len(repository.read_records(scope)) == 3
        assert not list(run.directory.glob("*.blob"))
    repository.close()
