import hashlib
import json
import re
import time
from contextlib import nullcontext

from sqlalchemy import Column, Float, Integer, String, Table, Text, func, select

from trackio.cloud_objects import MAX_OBJECT_BYTES


def rich_tables(metadata):
    objects = Table(
        "trackio_cloud_objects",
        metadata,
        Column("run_id", String(256), primary_key=True),
        Column("digest", String(64), primary_key=True),
        Column("size", Integer, nullable=False),
        Column("content_type", String(128), nullable=False),
        Column("object_key", String(129), nullable=False),
    )
    records = Table(
        "trackio_cloud_records",
        metadata,
        Column("run_id", String(256), primary_key=True),
        Column("record_id", String(256), primary_key=True),
        Column("kind", String(32), nullable=False, index=True),
        Column("observed_at", Float, nullable=False, index=True),
        Column("payload", Text, nullable=False),
    )
    return objects, records


def object_references(value, depth=0):
    if depth > 24:
        raise ValueError("RECORD_NESTING_LIMIT")
    if isinstance(value, dict):
        if "_object" in value:
            digest = value["_object"]
            if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                raise ValueError("INVALID_OBJECT_REFERENCE")
            yield digest
        for child in value.values():
            yield from object_references(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            yield from object_references(child, depth + 1)


class RichRepository:
    def _rich_guard(self, connection, claims):
        updated = connection.execute(
            self.runs.update()
            .where(
                *[
                    self.runs.c[key] == claims[key]
                    for key in ("run_id", "workspace_id", "project_id", "job_id")
                ]
            )
            .values(updated_at=time.time())
        ).rowcount
        if updated != 1:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        if claims.get("capability_id") is not None:
            self._live_capability(connection, claims["capability_id"], claims["run_id"])
        else:
            self._live_legacy(connection, claims)

    def put_object(self, claims, digest, payload, content_type, storage):
        if storage is None:
            raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
        if not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("INVALID_OBJECT_DIGEST")
        if (
            len(payload) > MAX_OBJECT_BYTES
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise ValueError("INVALID_OBJECT_CONTENT")
        if not re.fullmatch(r"[a-zA-Z0-9.+-]+/[a-zA-Z0-9.+-]+", content_type):
            raise ValueError("INVALID_CONTENT_TYPE")
        scope = self.document_json(
            {
                key: claims[key]
                for key in ("workspace_id", "project_id", "job_id", "run_id")
            }
        )
        object_key = hashlib.sha256(scope.encode()).hexdigest() + "/" + digest
        with self.engine.begin() as connection:
            self._rich_guard(connection, claims)
            previous = (
                connection.execute(
                    select(self.objects).where(
                        self.objects.c.run_id == claims["run_id"],
                        self.objects.c.digest == digest,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if previous is not None:
                if previous["size"] != len(payload):
                    raise ValueError("OBJECT_CONTENT_CONFLICT")
                storage.get(previous["object_key"], digest, previous["size"])
                return
            used = (
                connection.execute(
                    select(func.sum(self.objects.c.size)).where(
                        self.objects.c.run_id == claims["run_id"]
                    )
                ).scalar_one()
                or 0
            )
            if used + len(payload) > 100 * 1024 * 1024:
                raise ValueError("RUN_OBJECT_QUOTA_EXCEEDED")
            storage.put(object_key, payload, digest, content_type)
            connection.execute(
                self.objects.insert().values(
                    run_id=claims["run_id"],
                    digest=digest,
                    size=len(payload),
                    content_type=content_type,
                    object_key=object_key,
                )
            )

    def append_record(self, claims, record):
        if not isinstance(record, dict) or set(record) != {
            "record_id",
            "kind",
            "observed_at",
            "trial_id",
            "writer_id",
            "step",
            "payload",
        }:
            raise ValueError("INVALID_RECORD")
        for key in ("record_id", "writer_id"):
            self.validate_identity(record[key])
        if record["kind"] not in (
            "history",
            "summary",
            "alert",
            "trace",
            "document",
            "artifact",
            "registry",
            "finish",
        ):
            raise ValueError("INVALID_RECORD_KIND")
        if record["trial_id"] != claims.get("trial_id"):
            raise PermissionError("TRIAL_SCOPE_MISMATCH")
        if record["step"] is not None and (
            type(record["step"]) is not int or record["step"] < 0
        ):
            raise ValueError("INVALID_STEP")
        if type(record["observed_at"]) not in (int, float) or not self.is_finite(
            record["observed_at"]
        ):
            raise ValueError("INVALID_OBSERVATION")
        if not isinstance(record["payload"], dict):
            raise ValueError("INVALID_RECORD_PAYLOAD")
        encoded = self.document_json(record)
        if len(encoded.encode()) > 1024 * 1024:
            raise ValueError("RECORD_TOO_LARGE")
        references = set(object_references(record["payload"]))
        with self.engine.begin() as connection:
            self._rich_guard(connection, claims)
            for digest in references:
                if (
                    connection.execute(
                        select(self.objects.c.digest).where(
                            self.objects.c.run_id == claims["run_id"],
                            self.objects.c.digest == digest,
                        )
                    ).first()
                    is None
                ):
                    raise ValueError("OBJECT_NOT_COMMITTED")
            previous = connection.execute(
                select(self.documents.c.payload).where(
                    self.documents.c.run_id == claims["run_id"],
                    self.documents.c.record_id == record["record_id"],
                )
            ).scalar_one_or_none()
            if previous is not None:
                if previous != encoded:
                    raise self.record_conflict("RECORD_CONTENT_CONFLICT")
                return 0
            connection.execute(
                self.documents.insert().values(
                    run_id=claims["run_id"],
                    record_id=record["record_id"],
                    kind=record["kind"],
                    observed_at=record["observed_at"],
                    payload=encoded,
                )
            )
            return 1

    def read_records(self, scope, kind=None, after=None, *, connection=None):
        conditions = [
            self.runs.c[key] == scope[key]
            for key in ("workspace_id", "project_id", "job_id", "run_id")
        ]
        if kind is not None:
            conditions.append(self.documents.c.kind == kind)
        if after is not None:
            conditions.append(self.documents.c.record_id > after)
        with (
            nullcontext(connection)
            if connection is not None
            else self.engine.connect() as connection
        ):
            rows = (
                connection.execute(
                    select(self.documents.c.payload)
                    .join(self.runs, self.runs.c.run_id == self.documents.c.run_id)
                    .where(*conditions)
                    .order_by(self.documents.c.record_id)
                    .limit(501)
                )
                .scalars()
                .all()
            )
        return [json.loads(row) for row in rows]

    def read_object(self, scope, digest, storage):
        if storage is None:
            raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
        with self.engine.connect() as connection:
            row = self._object_metadata(connection, scope, digest)
        return storage.get(row["object_key"], digest, row["size"])

    def _object_metadata(self, connection, scope, digest):
        row = (
            connection.execute(
                select(self.objects)
                .join(self.runs, self.runs.c.run_id == self.objects.c.run_id)
                .where(
                    *[
                        self.runs.c[key] == scope[key]
                        for key in (
                            "workspace_id",
                            "project_id",
                            "job_id",
                            "run_id",
                        )
                    ],
                    self.objects.c.digest == digest,
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        return row
