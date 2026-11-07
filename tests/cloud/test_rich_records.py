import hashlib
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, MetricsRepository, create_app
from trackio.cloud_objects import FileObjects


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_scoped_objects_and_records_require_committed_references_after_restart(
    tmp_path,
    backend,
):
    url = (
        Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
        if backend == "postgres"
        else f"sqlite:///{tmp_path / 'cloud.sqlite'}"
    )
    schema = "trackio_test_" + uuid4().hex[:12] if backend == "postgres" else None
    repository = MetricsRepository(url, schema=schema)
    config = CloudConfig("local", url, "s" * 32, "k" * 32)
    objects = FileObjects(tmp_path / "objects")
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    service = {"Authorization": "Bearer " + "s" * 32}
    payload = b"synthetic-image"
    digest = hashlib.sha256(payload).hexdigest()
    with TestClient(create_app(config, repository, objects=objects)) as client:
        binding = client.post(
            "/v1/runs", headers=service, json={**scope, "config": {}}
        ).json()
        worker = {"Authorization": "Bearer " + binding["token"]}
        record = dict(
            record_id="e",
            kind="history",
            step=7,
            observed_at=100,
            trial_id=None,
            writer_id="writer",
            payload={"image": {"_object": digest}},
        )
        assert (
            client.post("/v1/runs/r/records", headers=worker, json=record).status_code
            == 422
        )
        uploaded = client.put(
            f"/v1/runs/r/objects/{digest}",
            headers={**worker, "Content-Type": "image/png"},
            content=payload,
        )
        assert uploaded.status_code == 200, uploaded.text
        assert uploaded.json()["committed"] is True
        assert (
            client.post(
                "/v1/runs/other/records", headers=worker, json=record
            ).status_code
            == 403
        )
        assert (
            client.post("/v1/runs/r/records", headers=worker, json=record).status_code
            == 200
        )
        assert (
            client.post("/v1/runs/r/records", headers=worker, json=record).json()[
                "accepted"
            ]
            == 0
        )
        conflict = {**record, "payload": {"different": 1}}
        assert (
            client.post("/v1/runs/r/records", headers=worker, json=conflict).status_code
            == 409
        )
        assert (
            client.get("/v1/records", headers=worker, params=scope).status_code == 403
        )
    repository.close()
    repository = MetricsRepository(url, initialize=False, schema=schema)
    try:
        with TestClient(create_app(config, repository, objects=objects)) as client:
            rows = client.get("/v1/records", headers=service, params=scope).json()[
                "records"
            ]
            assert rows == [record]
            assert (
                client.get(
                    "/v1/records",
                    headers=service,
                    params={**scope, "workspace_id": "other"},
                ).json()["records"]
                == []
            )
            downloaded = client.get(
                f"/v1/runs/r/objects/{digest}", headers=service, params=scope
            )
            assert downloaded.content == payload
            assert downloaded.headers["X-Content-Type-Options"] == "nosniff"
            assert "attachment" in downloaded.headers["Content-Disposition"]
            assert (
                client.get(
                    f"/v1/runs/r/objects/{digest}",
                    headers=service,
                    params={**scope, "project_id": "other"},
                ).status_code
                == 403
            )
            client.post(
                "/v1/runs/r/revoke", headers=service, json=scope
            ).raise_for_status()
            assert (
                client.put(
                    f"/v1/runs/r/objects/{digest}", headers=worker, content=payload
                ).status_code
                == 401
            )
    finally:
        if schema is not None:
            with repository.engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        repository.close()


@pytest.mark.parametrize("size", [-1, 33 * 1024 * 1024])
def test_object_read_rejects_invalid_size_before_reading(tmp_path, size):
    objects = FileObjects(tmp_path / "objects")
    with pytest.raises(ValueError, match="INVALID_OBJECT_SIZE"):
        objects.get("a" * 64 + "/" + "b" * 64, "b" * 64, size)
