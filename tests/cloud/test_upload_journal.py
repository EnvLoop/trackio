import hashlib
import os
import time

import pytest
from sqlalchemy import select

from trackio.cloud_objects import FileObjects
from trackio.cloud_protocol import content_digest
from trackio.cloud_workspace import WorkspaceRepository


def test_revocation_after_physical_upload_leaves_collectable_scoped_intent(
    tmp_path, workspace_database
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}
    underlying = FileObjects(tmp_path / "objects")

    class RevokingStorage:
        def put(self, *args):
            underlying.put(*args)
            repository.revoke(scope)

        def get(self, *args):
            return underlying.get(*args)

    content = b"orphaned-upload"
    digest = hashlib.sha256(content).hexdigest()
    with pytest.raises(PermissionError):
        repository.put_object(
            claims, digest, content, "application/octet-stream", RevokingStorage()
        )
    with repository.engine.connect() as connection:
        assert connection.execute(select(repository.objects)).first() is None
        intent = (
            connection.execute(select(repository.workspace.uploads)).mappings().one()
        )
    key = intent["object_key"]
    assert underlying.get(key, digest, len(content)) == content
    assert repository.workspace.collect_orphans(scope, underlying)["removed"] == 0
    with pytest.raises(PermissionError):
        repository.workspace.collect_orphans(
            {**scope, "project_id": "other"}, underlying
        )
    with repository.engine.begin() as connection:
        connection.execute(
            repository.workspace.uploads.update().values(created_at=time.time() - 90000)
        )
    assert repository.workspace.collect_orphans(scope, underlying)["removed"] == 1
    with pytest.raises(FileNotFoundError):
        underlying.get(key, digest, len(content))
    repository.close()


def test_object_gc_preserves_committed_references_and_rejects_short_retention(
    tmp_path, workspace_database
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}
    objects = FileObjects(tmp_path / "objects")
    content = b"still-referenced"
    digest = hashlib.sha256(content).hexdigest()
    repository.put_object(claims, digest, content, "application/octet-stream", objects)
    repository.workspace.publish(
        claims,
        "artifact",
        "kept",
        {
            "artifact_type": "file",
            "manifest": [{"path": "file", "_object": digest, "size": len(content)}],
        },
        operation_id="publish",
    )
    with repository.engine.begin() as connection:
        connection.execute(
            repository.workspace.uploads.update().values(created_at=time.time() - 90000)
        )
    with pytest.raises(ValueError, match="INVALID_ORPHAN_RETENTION"):
        repository.workspace.collect_orphans(scope, objects, retention_seconds=0)
    assert repository.workspace.collect_orphans(scope, objects)["removed"] == 0
    assert repository.read_object(scope, digest, objects) == content
    repository.close()


def test_corrupted_stored_body_never_enters_catalog(tmp_path, workspace_database):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}

    class CorruptingStorage(FileObjects):
        def put(self, key, payload, digest, content_type):
            super().put(key, payload, digest, content_type)
            self._path(key).write_bytes(b"corrupt")

    objects = CorruptingStorage(tmp_path / "objects")
    payload = b"expected"
    with pytest.raises(ValueError, match="OBJECT_INTEGRITY_FAILURE"):
        repository.put_object(
            claims,
            hashlib.sha256(payload).hexdigest(),
            payload,
            "application/octet-stream",
            objects,
        )
    with repository.engine.connect() as connection:
        assert connection.execute(select(repository.objects)).first() is None
        assert (
            connection.execute(
                select(repository.workspace.uploads.c.status)
            ).scalar_one()
            == "pending"
        )
    repository.close()


def test_prejournal_physical_orphan_is_deleted_after_retention_only_in_scope(
    tmp_path, workspace_database
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    objects = FileObjects(tmp_path / "objects")
    payload = b"physical-orphan"
    digest = hashlib.sha256(payload).hexdigest()
    key = content_digest(scope) + "/" + digest
    foreign = content_digest({**scope, "project_id": "foreign"}) + "/" + digest
    for value in (key, foreign):
        objects.put(value, payload, digest, "application/octet-stream")
        os.utime(objects._path(value), (time.time() - 90000, time.time() - 90000))
    assert repository.workspace.collect_orphans(scope, objects)["removed"] == 1
    with pytest.raises(FileNotFoundError):
        objects.get(key, digest, len(payload))
    assert objects.get(foreign, digest, len(payload)) == payload
    repository.close()
