import hashlib
import json
import os

import pytest
from sqlalchemy import select

from trackio.cloud_objects import FileObjects
from trackio.cloud_protocol import canonical, content_digest
from trackio.cloud_workspace import WorkspaceRepository


def test_ws1_migration_delete_purge_preserves_bounded_historical_gc_authority(
    workspace_database, tmp_path
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    destination = {**scope, "project_id": "destination"}
    foreign_scope = {**scope, "workspace_id": "foreign"}
    repository.bind(**scope, config={})
    prefixes = []

    class AuditedObjects(FileObjects):
        def inventory(self, prefix, **kwargs):
            prefixes.append(prefix)
            return super().inventory(prefix, **kwargs)

    objects = AuditedObjects(tmp_path / "objects")
    payload = b"historical"
    digest = hashlib.sha256(payload).hexdigest()
    old_key = content_digest(scope) + "/" + digest
    foreign_key = content_digest(foreign_scope) + "/" + digest
    for key in (old_key, foreign_key):
        objects.put(key, payload, digest, "application/octet-stream")
        os.utime(objects._path(key), (1, 1))
    repository.workspace.mutate(
        scope, "move", {"destination": destination}, operation_id="move"
    )
    repository.put_import_object(destination, payload, objects)
    repository.workspace.mutate(
        destination, "rename", {"name": "purged-name"}, operation_id="rename"
    )
    with repository.engine.begin() as connection:
        repository.workspace.upload_locations.drop(connection)
        connection.execute(repository.workspace.schema.update().values(version=1))
    repository.close()
    upgraded = WorkspaceRepository(
        url,
        schema=schema,
        initialize=False,
        initialize_workspace=True,
        migrate_workspace_from=1,
    )
    try:
        upgraded.workspace.mutate(destination, "delete", {}, operation_id="delete")
        assert upgraded.workspace.purge(destination) == {"deleted": 1}
    finally:
        upgraded.close()
    reopened = WorkspaceRepository(url, schema=schema, initialize=False)
    try:
        with pytest.raises(PermissionError):
            reopened.workspace.collect_orphans(scope, objects, limit=1)
        with pytest.raises(PermissionError):
            reopened.workspace.collect_orphans(
                destination,
                objects,
                after=json.dumps(
                    {"prefix": content_digest(foreign_scope), "after": None}
                ),
                limit=1,
            )
        after = None
        for page in range(10):
            result = reopened.workspace.collect_orphans(
                destination, objects, after=after, limit=1
            )
            assert result["inventory_scanned"] <= 1
            after = result["next"]
            if after is None:
                break
        else:
            pytest.fail("historical inventory pagination did not terminate")
        with pytest.raises(FileNotFoundError):
            objects.get(old_key, digest, len(payload))
        assert objects.get(foreign_key, digest, len(payload)) == payload
        assert (
            objects.get(
                content_digest(destination) + "/" + digest, digest, len(payload)
            )
            == payload
        )
        assert content_digest(scope) in prefixes
        assert set(prefixes) <= {content_digest(scope), content_digest(destination)}
        with reopened.engine.connect() as connection:
            receipts = (
                connection.execute(select(reopened.workspace.operations.c.operation_id))
                .scalars()
                .all()
            )
        assert receipts == ["move"]
        assert reopened.workspace.purge(destination) == {"deleted": 1}
        assert (
            reopened.workspace.collect_orphans(destination, objects, limit=1)["removed"]
            == 0
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("corruption", ["digest", "foreign_scope"])
def test_purge_and_gc_reject_unverified_move_history_without_destroying_receipts(
    workspace_database, corruption
):
    url, schema = workspace_database
    repository = WorkspaceRepository(url, schema=schema, initialize_workspace=True)
    scope = dict(workspace_id="w", project_id="p", job_id="j", run_id="r")
    destination = {**scope, "project_id": "destination"}
    repository.bind(**scope, config={})
    repository.workspace.mutate(
        scope, "move", {"destination": destination}, operation_id="move"
    )
    repository.workspace.mutate(destination, "delete", {}, operation_id="delete")
    try:
        with repository.engine.begin() as connection:
            payload = json.loads(
                connection.execute(
                    select(repository.workspace.operations.c.payload).where(
                        repository.workspace.operations.c.operation_id == "move"
                    )
                ).scalar_one()
            )
            if corruption == "foreign_scope":
                payload["request"]["scope"]["workspace_id"] = "foreign"
                digest = content_digest(payload["request"])
            else:
                digest = "0" * 64
            connection.execute(
                repository.workspace.operations.update()
                .where(repository.workspace.operations.c.operation_id == "move")
                .values(payload=canonical(payload), digest=digest)
            )

        class ForbiddenInventory:
            def inventory(self, *args, **kwargs):
                raise AssertionError("unverified source reached physical inventory")

        with pytest.raises(PermissionError, match="OBJECT_GC_SCOPE_MISMATCH"):
            repository.workspace.purge(destination)
        with pytest.raises(PermissionError, match="OBJECT_GC_SCOPE_MISMATCH"):
            repository.workspace.collect_orphans(destination, ForbiddenInventory())
        with repository.engine.connect() as connection:
            receipts = (
                connection.execute(
                    select(repository.workspace.operations.c.operation_id)
                )
                .scalars()
                .all()
            )
        assert set(receipts) == {"move", "delete"}
    finally:
        repository.close()
