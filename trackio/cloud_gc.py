"""Durable invalidation before irreversible, scoped physical object deletion."""

import json
import re
import time
import uuid

from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB

from trackio.cloud_protocol import SCOPE_FIELDS, content_digest
from trackio.cloud_records import object_references


def validate_location(scope, row):
    origin = json.loads(row["scope"])
    if (
        any(origin[key] != scope[key] for key in ("workspace_id", "job_id", "run_id"))
        or not re.fullmatch(r"[a-f0-9]{64}", row["digest"])
        or row["object_key"] != content_digest(origin) + "/" + row["digest"]
    ):
        raise PermissionError("OBJECT_GC_SCOPE_MISMATCH")


def cleanup_history(store, connection, scope):
    origins = {content_digest(scope): scope}
    if store.engine.dialect.name == "postgresql":
        action = cast(store.operations.c.payload, JSONB)["request"][
            "action"
        ].as_string()
    else:
        action = func.json_extract(store.operations.c.payload, "$.request.action")
    moves = (
        connection.execute(
            select(store.operations)
            .where(store.operations.c.run_id == scope["run_id"], action == "move")
            .limit(1001)
        )
        .mappings()
        .all()
    )
    if len(moves) > 1000:
        raise ValueError("OBJECT_SCOPE_HISTORY_LIMIT")
    operation_ids = []
    for row in moves:
        request = json.loads(row["payload"])["request"]
        if (
            row["digest"] != content_digest(request)
            or set(request) != {"scope", "action", "values"}
            or set(request["values"]) != {"destination"}
        ):
            raise PermissionError("OBJECT_GC_SCOPE_MISMATCH")
        for origin in (request["scope"], request["values"]["destination"]):
            if set(origin) != set(SCOPE_FIELDS) or any(
                origin[key] != scope[key]
                for key in ("workspace_id", "job_id", "run_id")
            ):
                raise PermissionError("OBJECT_GC_SCOPE_MISMATCH")
            for value in origin.values():
                store.repository.validate_identity(value)
            origins[content_digest(origin)] = origin
        operation_ids.append(row["operation_id"])
    return origins, operation_ids


def inventory_page(store, connection, scope, storage, after, limit):
    origins, _ = cleanup_history(store, connection, scope)
    prefixes = sorted(origins)
    prefix, cursor = prefixes[0], None
    if after is not None:
        if not isinstance(after, str):
            raise ValueError("INVALID_OBJECT_CURSOR")
        if after.startswith("{"):
            value = json.loads(after)
            if set(value) != {"prefix", "after"}:
                raise ValueError("INVALID_OBJECT_CURSOR")
            prefix, cursor = value["prefix"], value["after"]
        else:
            prefix, cursor = after.split("/", 1)[0], after
        if prefix not in origins:
            raise PermissionError("OBJECT_GC_SCOPE_MISMATCH")
    result = storage.inventory(prefix, after=cursor, limit=limit)
    if result["next"] is not None:
        result["next"] = json.dumps(
            {"prefix": prefix, "after": result["next"]}, sort_keys=True
        )
    elif prefixes.index(prefix) + 1 < len(prefixes):
        result["next"] = json.dumps(
            {"prefix": prefixes[prefixes.index(prefix) + 1], "after": None},
            sort_keys=True,
        )
    return result, origins[prefix]


def collect(
    store,
    scope,
    storage,
    *,
    retention_seconds=86400,
    limit=100,
    after=None,
    deleted=False,
):
    if (
        type(retention_seconds) is not int
        or retention_seconds < 3600
        or type(limit) is not int
        or not 1 <= limit <= 1000
    ):
        raise ValueError("INVALID_ORPHAN_RETENTION")
    if store._connection.get() is not None:
        raise RuntimeError("GC_REQUIRES_INDEPENDENT_TRANSACTION")
    locations, objects = store.upload_locations, store.repository.objects
    cutoff = time.time() - retention_seconds
    inventory = {"objects": [], "next": None}
    with store.transaction() as connection:
        state = store._lock(connection, scope, allow_deleted=True)
        if deleted and not state["deleted"]:
            raise ValueError("RUN_MUST_BE_DELETED")
        referenced = set()
        if not deleted:
            for table in (
                store.repository.documents,
                store.resources,
                store.legacy_files,
            ):
                rows = (
                    connection.execute(
                        select(table.c.payload)
                        .where(table.c.run_id == scope["run_id"])
                        .limit(10001)
                    )
                    .scalars()
                    .all()
                )
                if len(rows) > 10000:
                    raise ValueError("OBJECT_REFERENCE_SCAN_LIMIT")
                for payload in rows:
                    referenced.update(object_references(json.loads(payload)))
        catalog = (
            connection.execute(
                select(objects).where(objects.c.run_id == scope["run_id"]).limit(10001)
            )
            .mappings()
            .all()
        )
        if len(catalog) > 10000:
            raise ValueError("OBJECT_REFERENCE_SCAN_LIMIT")
        protected = {
            row["object_key"] for row in catalog if row["digest"] in referenced
        }
        if deleted:
            for row in catalog:
                existing = connection.execute(
                    select(locations.c.object_key).where(
                        locations.c.run_id == scope["run_id"],
                        locations.c.object_key == row["object_key"],
                    )
                ).first()
                if existing is None:
                    origin = scope
                    if row["object_key"] != content_digest(scope) + "/" + row["digest"]:
                        raise PermissionError("OBJECT_GC_LOCATION_REQUIRED")
                    connection.execute(
                        locations.insert().values(
                            run_id=scope["run_id"],
                            object_key=row["object_key"],
                            digest=row["digest"],
                            scope=json.dumps(
                                origin, sort_keys=True, separators=(",", ":")
                            ),
                            generation=uuid.uuid4().hex,
                            status="complete",
                            created_at=0,
                        )
                    )
        if hasattr(storage, "inventory"):
            inventory, origin = inventory_page(
                store, connection, scope, storage, after, limit
            )
            for entry in inventory["objects"]:
                digest = entry["key"].rsplit("/", 1)[-1]
                if entry["modified_at"] > cutoff or entry["key"] in protected:
                    continue
                row = dict(
                    run_id=scope["run_id"],
                    object_key=entry["key"],
                    digest=digest,
                    scope=json.dumps(origin, sort_keys=True, separators=(",", ":")),
                    generation=uuid.uuid4().hex,
                    status="pending",
                    created_at=entry["modified_at"],
                )
                validate_location(scope, row)
                connection.execute(
                    store._insert(locations).values(**row).on_conflict_do_nothing()
                )
        conditions = [locations.c.run_id == scope["run_id"]]
        if not deleted:
            conditions.append(locations.c.created_at <= cutoff)
        else:
            conditions.append(locations.c.status != "collected")
        if protected:
            conditions.append(locations.c.object_key.not_in(protected))
        candidates = (
            connection.execute(
                select(locations)
                .where(*conditions)
                .order_by(locations.c.created_at, locations.c.object_key)
                .limit(limit)
            )
            .mappings()
            .all()
        )
        for row in candidates:
            validate_location(scope, row)
            connection.execute(
                objects.delete().where(
                    objects.c.run_id == scope["run_id"],
                    objects.c.object_key == row["object_key"],
                )
            )
            connection.execute(
                locations.update()
                .where(
                    locations.c.run_id == scope["run_id"],
                    locations.c.object_key == row["object_key"],
                )
                .values(status="collecting")
            )
            connection.execute(
                store.uploads.update()
                .where(
                    store.uploads.c.run_id == scope["run_id"],
                    store.uploads.c.object_key == row["object_key"],
                    store.uploads.c.generation == row["generation"],
                )
                .values(status="collecting")
            )
    removed = 0
    for row in candidates:
        with store.transaction() as connection:
            store._lock(connection, scope, allow_deleted=True)
            current = (
                connection.execute(
                    select(locations).where(
                        locations.c.run_id == scope["run_id"],
                        locations.c.object_key == row["object_key"],
                    )
                )
                .mappings()
                .one()
            )
            if (
                current["generation"] != row["generation"]
                or current["status"] != "collecting"
            ):
                continue
            storage.delete(row["object_key"])
            now = time.time()
            connection.execute(
                locations.update()
                .where(
                    locations.c.run_id == scope["run_id"],
                    locations.c.object_key == row["object_key"],
                )
                .values(status="collected", created_at=now)
            )
            connection.execute(
                store.uploads.update()
                .where(
                    store.uploads.c.run_id == scope["run_id"],
                    store.uploads.c.object_key == row["object_key"],
                    store.uploads.c.generation == row["generation"],
                )
                .values(status="collected", created_at=now)
            )
            removed += 1
    return {
        "removed": removed,
        "scanned": len(candidates),
        "retention_seconds": retention_seconds,
        "inventory_scanned": len(inventory["objects"]),
        "next": inventory["next"],
    }
