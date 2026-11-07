import hashlib
import importlib
import importlib.util

import pytest

from trackio.cloud import MetricsRepository
from trackio.cloud_objects import FileObjects


def workspace_store(repository):
    assert importlib.util.find_spec("trackio.cloud_workspace") is not None, (
        "Transactional workspace resources are missing"
    )
    return importlib.import_module("trackio.cloud_workspace").WorkspaceStore(
        repository, initialize=True
    )


def test_native_attachment_versions_alias_lineage_registry_and_logbook(
    tmp_path, workspace_database
):
    url, schema = workspace_database
    repository = MetricsRepository(url, schema=schema)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}
    storage = FileObjects(tmp_path / "objects")
    content = b"synthetic attachment"
    digest = hashlib.sha256(content).hexdigest()
    repository.put_object(claims, digest, content, "application/octet-stream", storage)
    store = workspace_store(repository)
    artifact = {
        "artifact_type": "model",
        "manifest": [{"path": "weights.bin", "_object": digest, "size": len(content)}],
    }
    first = store.publish(
        claims,
        "artifact",
        "experiment",
        artifact,
        operation_id="publish",
        aliases=["prod"],
    )
    assert first["version"] == "v0"
    assert (
        store.publish(
            claims,
            "artifact",
            "experiment",
            artifact,
            operation_id="publish",
            aliases=["prod"],
        )
        == first
    )
    assert store.resolve(scope, "artifact", "experiment", "prod") == first
    with pytest.raises(ValueError, match="OPERATION_CONTENT_CONFLICT"):
        store.publish(
            claims,
            "artifact",
            "experiment",
            artifact,
            operation_id="publish",
            aliases=["different"],
        )
    with pytest.raises(ValueError, match="FORMAL_CHECKPOINT"):
        store.publish(
            claims,
            "artifact",
            "formal",
            {**artifact, "formal_checkpoint": True},
            operation_id="formal",
        )
    book = store.publish(
        claims,
        "logbook",
        "notes",
        {
            "pages": {"index.md": "# Synthetic"},
            "attachments": [
                {"path": "data.bin", "_object": digest, "size": len(content)}
            ],
        },
        operation_id="notes",
    )
    assert store.resolve(scope, "logbook", "notes", "latest") == book
    linked = store.link(
        scope, scope, "experiment", "v0", "registry", "collection", operation_id="link"
    )
    assert linked["source"]["digest"] == first["digest"]
    assert store.lineage(scope, "experiment", "v0")[0]["target"] == scope
    assert (
        store.link(
            scope,
            scope,
            "experiment",
            "v0",
            "registry",
            "collection",
            operation_id="link",
        )
        == linked
    )
    with pytest.raises(PermissionError):
        store.link(
            {**scope, "project_id": "forged"},
            scope,
            "experiment",
            "v0",
            "registry",
            "forged",
            operation_id="forged",
        )
    with pytest.raises(PermissionError):
        store.publish(
            claims,
            "registry",
            "forged",
            {
                "source": {
                    "scope": {**scope, "project_id": "forged"},
                    "name": "experiment",
                    "version": "v0",
                    "digest": first["digest"],
                }
            },
            operation_id="forged-publish",
        )
    repository.close()
    repository = MetricsRepository(url, schema=schema, initialize=False)
    store = importlib.import_module("trackio.cloud_workspace").WorkspaceStore(
        repository
    )
    assert (
        store.resolve(scope, "artifact", "experiment", "prod")["digest"]
        == first["digest"]
    )
    with pytest.raises(PermissionError):
        store.resolve(
            {**scope, "project_id": "other"}, "artifact", "experiment", "prod"
        )
    repository.close()


def test_resource_reference_and_live_capability_are_checked_in_run_transaction(
    workspace_database,
):
    url, schema = workspace_database
    repository = MetricsRepository(url, schema=schema)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    repository.bind(**scope, config={})
    capability = repository.capability(scope, None, None)
    claims = {**scope, "trial_id": None, "capability_id": capability["capability_id"]}
    store = workspace_store(repository)
    artifact = {
        "artifact_type": "file",
        "manifest": [{"path": "file", "_object": "a" * 64, "size": 1}],
    }
    with pytest.raises(ValueError, match="OBJECT_NOT_COMMITTED"):
        store.publish(claims, "artifact", "missing", artifact, operation_id="missing")
    repository.revoke(scope)
    with pytest.raises(PermissionError):
        store.publish(claims, "logbook", "notes", {"pages": {}}, operation_id="revoked")
    repository.close()
