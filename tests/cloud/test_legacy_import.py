import importlib
import importlib.util
import json
import sqlite3
from pathlib import Path

import numpy as np
import pytest

import trackio
from trackio.cloud_objects import FileObjects
from trackio.cloud_snapshot import capture
from trackio.cloud_workspace import WorkspaceRepository
from trackio.sqlite_storage import _close_all_persistent_connections


def test_complete_offline_import_is_idempotent_scoped_and_source_independent(
    tmp_path, workspace_database
):
    source = tmp_path / "source"
    source.mkdir()
    with sqlite3.connect(source / "legacy.db") as database:
        database.execute(
            "CREATE TABLE configs (run_id TEXT, run_name TEXT, config TEXT)"
        )
        database.execute(
            "INSERT INTO configs VALUES ('legacy', 'display', '{\"lr\":0.1}')"
        )
        database.execute(
            "CREATE TABLE metrics (log_id TEXT, run_id TEXT, run_name TEXT, step INTEGER, timestamp TEXT, metrics TEXT)"
        )
        database.execute(
            "INSERT INTO metrics VALUES ('original-event', 'legacy', 'display', 7, '2026-11-06T00:00:00+00:00', '{\"loss\":0.5}')"
        )
        database.execute("CREATE TABLE custom_history (value BLOB)")
        database.execute("INSERT INTO custom_history VALUES (?)", (b"preserve-all",))
    for path, content in {
        "media/p/image.png": b"synthetic",
        ".trackio/logbook/index.md": b"# Notes",
        "inbox/writer/pending.jsonl": (
            json.dumps(
                {
                    "v": 1,
                    "kind": "metric",
                    "project": "legacy",
                    "run_id": "legacy",
                    "run": "display",
                    "log_id": "original-event",
                    "step": 7,
                    "timestamp": "2026-11-06T00:00:00+00:00",
                    "metrics": {"loss": 0.5},
                }
            )
            + "\n"
        ).encode(),
    }.items():
        target = source / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    snapshot = capture(source, tmp_path / "snapshot", writers_stopped=True)
    source.rename(tmp_path / "offline")
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    assert importlib.util.find_spec("trackio.cloud_import") is not None, (
        "Complete legacy import is missing"
    )
    module = importlib.import_module("trackio.cloud_import")
    objects = FileObjects(tmp_path / "objects")
    importer = module.LegacyImporter(repository, objects)
    bindings = {
        "databases": {"legacy.db": {"runs": {"legacy": scope}, "default_scope": scope}},
        "files": {path: scope for path in snapshot["files"]},
    }
    receipt = importer.import_snapshot(
        tmp_path / "snapshot", bindings, source_id="synthetic-source"
    )
    assert receipt["committed"] and receipt["snapshot_digest"] == snapshot["digest"]
    assert receipt["rows"] == 3 and receipt["files"] == 4
    assert (
        importer.import_snapshot(
            tmp_path / "snapshot", bindings, source_id="synthetic-source"
        )
        == receipt
    )
    assert len(repository.events("w", "p", "j")) == 1
    event = repository.events("w", "p", "j")[0]
    assert (
        event["step"] == 7
        and event["trial_id"] is None
        and event["metrics"] == {"loss": 0.5}
    )
    assert (
        importer.read_file(scope, "synthetic-source", "media/p/image.png")
        == b"synthetic"
    )
    with pytest.raises(PermissionError):
        importer.read_file(
            {**scope, "project_id": "other"}, "synthetic-source", "media/p/image.png"
        )
    assert importer.read_rows(scope, "synthetic-source", "legacy.db", "custom_history")[
        0
    ]["value"] == {"bytes": "cHJlc2VydmUtYWxs"}
    repository.close()
    repository = WorkspaceRepository(url, schema=schema, initialize=False)
    importer = module.LegacyImporter(repository, objects)
    assert (
        importer.read_file(scope, "synthetic-source", ".trackio/logbook/index.md")
        == b"# Notes"
    )
    assert (
        importer.receipt(scope, "synthetic-source")["snapshot_digest"]
        == snapshot["digest"]
    )
    repository.close()


def test_legacy_source_identity_is_workspace_scoped(tmp_path, workspace_database):
    module = importlib.import_module("trackio.cloud_import")
    source = tmp_path / "source"
    source.mkdir()
    (source / "payload.bin").write_bytes(b"synthetic")
    manifest = capture(source, tmp_path / "snapshot", writers_stopped=True)
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    importer = module.LegacyImporter(repository, FileObjects(tmp_path / "objects"))
    for workspace in ("one", "two"):
        scope = dict(
            workspace_id=workspace, project_id="p", job_id="j", run_id="r-" + workspace
        )
        repository.bind(**scope, config={})
        bindings = {
            "databases": {},
            "files": {name: scope for name in manifest["files"]},
        }
        assert importer.import_snapshot(
            tmp_path / "snapshot", bindings, source_id="same-name"
        )["committed"]
        assert importer.read_file(scope, "same-name", "payload.bin") == b"synthetic"
    repository.close()


@pytest.mark.parametrize("artifact_bytes", [b"actual-legacy-attachment", b""])
def test_real_legacy_sqlite_media_artifact_readback_after_source_closed(
    temp_dir, tmp_path, workspace_database, artifact_bytes
):
    module = importlib.import_module("trackio.cloud_import")
    weights = tmp_path / "weights.bin"
    weights.write_bytes(artifact_bytes)
    legacy = trackio.init(
        project="legacy-project",
        name="producer",
        auto_log_gpu=False,
        auto_log_cpu=False,
    )
    trackio.log(
        {"loss": 0.25, "image": trackio.Image(np.zeros((2, 2, 3), dtype=np.uint8))},
        step=9,
    )
    artifact = trackio.Artifact("weights", "model")
    artifact.add_file(weights)
    trackio.log_artifact(artifact, aliases=["best"])
    registry = trackio.Api().create_registry("models", description="Old registry")
    registry.create_collection("weights", "model")
    artifact.link("registry-models/weights", aliases=["staging"])
    trackio.finish()
    _close_all_persistent_connections()
    snapshot = capture(temp_dir, tmp_path / "real-snapshot", writers_stopped=True)
    Path(temp_dir).rename(tmp_path / "legacy-offline")
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    objects = FileObjects(tmp_path / "real-objects")
    bindings = {
        "databases": {
            name: {"runs": {legacy.id: scope}, "default_scope": scope}
            for name in snapshot["databases"]
        },
        "files": {name: scope for name in snapshot["files"]},
    }
    importer = module.LegacyImporter(repository, objects)
    importer.import_snapshot(
        tmp_path / "real-snapshot", bindings, source_id="real-legacy"
    )
    projected = repository.workspace.resolve(scope, "artifact", "weights", "best")
    collection = repository.workspace.resolve(
        scope, "registry", "models.weights", "staging"
    )
    assert collection["content"]["source"]["digest"] == projected["digest"]
    assert projected["version"] == artifact.version
    assert projected["content"]["legacy_manifest_digest"] == artifact.digest
    entry = projected["content"]["manifest"][0]
    assert repository.read_object(scope, entry["_object"], objects) == artifact_bytes
    rich = repository.read_records(scope, "history")
    assert rich[0]["payload"]["values"]["image"]["chunks"][0]["_object"]
    assert len(repository.events("w", "p", "j")) == 1
    repository.close()
    repository = WorkspaceRepository(url, schema=schema, initialize=False)
    importer = module.LegacyImporter(repository, objects)
    assert (
        importer.receipt(scope, "real-legacy")["snapshot_digest"] == snapshot["digest"]
    )
    assert repository.read_object(scope, entry["_object"], objects) == artifact_bytes
    repository.close()
