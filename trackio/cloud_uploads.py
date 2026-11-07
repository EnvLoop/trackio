"""Durable upload intents and a second capability fence before catalog commit."""

import hashlib
import re
import time
import uuid

from sqlalchemy import func, select

from trackio.cloud_objects import MAX_OBJECT_BYTES
from trackio.cloud_protocol import canonical, content_digest


def put(repository, scope, digest, payload, content_type, storage, *, claims=None):
    if storage is None:
        raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
    if (
        not isinstance(digest, str)
        or not re.fullmatch(r"[a-f0-9]{64}", digest)
        or len(payload) > MAX_OBJECT_BYTES
        or hashlib.sha256(payload).hexdigest() != digest
    ):
        raise ValueError("INVALID_OBJECT_CONTENT")
    if not isinstance(content_type, str) or not re.fullmatch(
        r"[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+", content_type
    ):
        raise ValueError("INVALID_CONTENT_TYPE")
    store, objects = repository.workspace, repository.objects
    key = content_digest(scope) + "/" + digest
    generation = uuid.uuid4().hex

    def guard(connection):
        if claims is not None:
            repository._rich_guard(connection, claims)
        store._lock(connection, scope)

    with repository.engine.begin() as connection:
        guard(connection)
        previous = (
            connection.execute(
                select(objects).where(
                    objects.c.run_id == scope["run_id"], objects.c.digest == digest
                )
            )
            .mappings()
            .first()
        )
        if previous is not None:
            storage.get(previous["object_key"], digest, previous["size"])
            return
        values = {
            "run_id": scope["run_id"],
            "digest": digest,
            "object_key": key,
            "scope": canonical(scope),
            "generation": generation,
            "status": "pending",
            "created_at": time.time(),
        }
        connection.execute(
            store._insert(store.uploads)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[store.uploads.c.run_id, store.uploads.c.digest],
                set_={
                    key: value
                    for key, value in values.items()
                    if key not in ("run_id", "digest")
                },
            )
        )
        connection.execute(
            store._insert(store.upload_locations)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[
                    store.upload_locations.c.run_id,
                    store.upload_locations.c.object_key,
                ],
                set_={
                    name: value
                    for name, value in values.items()
                    if name not in ("run_id", "object_key")
                },
            )
        )
    storage.put(key, payload, digest, content_type)
    with repository.engine.begin() as connection:
        guard(connection)
        intent = (
            connection.execute(
                select(store.uploads).where(
                    store.uploads.c.run_id == scope["run_id"],
                    store.uploads.c.digest == digest,
                )
            )
            .mappings()
            .one()
        )
        if intent["generation"] != generation or intent["status"] != "pending":
            raise RuntimeError("UPLOAD_LEASE_EXPIRED")
        location = (
            connection.execute(
                select(store.upload_locations).where(
                    store.upload_locations.c.run_id == scope["run_id"],
                    store.upload_locations.c.object_key == key,
                )
            )
            .mappings()
            .one()
        )
        if location["generation"] != generation or location["status"] != "pending":
            raise RuntimeError("UPLOAD_LEASE_EXPIRED")
        storage.get(key, digest, len(payload))
        used = (
            connection.execute(
                select(func.sum(objects.c.size)).where(
                    objects.c.run_id == scope["run_id"]
                )
            ).scalar_one()
            or 0
        )
        if used + len(payload) > 100 * 1024 * 1024:
            raise ValueError("RUN_OBJECT_QUOTA_EXCEEDED")
        connection.execute(
            store._insert(objects)
            .values(
                run_id=scope["run_id"],
                digest=digest,
                size=len(payload),
                content_type=content_type,
                object_key=key,
            )
            .on_conflict_do_nothing()
        )
        connection.execute(
            store.uploads.update()
            .where(
                store.uploads.c.run_id == scope["run_id"],
                store.uploads.c.digest == digest,
            )
            .values(status="complete")
        )
        connection.execute(
            store.upload_locations.update()
            .where(
                store.upload_locations.c.run_id == scope["run_id"],
                store.upload_locations.c.object_key == key,
                store.upload_locations.c.generation == generation,
            )
            .values(status="complete")
        )
