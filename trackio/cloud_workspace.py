"""Scoped experiment resources and transactional lifecycle, independent of HF."""

import hashlib
import importlib
import json
import re
import time
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar

from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    cast,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from trackio.cloud import Conflict, MetricsRepository, canonical, identifier
from trackio.cloud_protocol import content_digest
from trackio.cloud_records import object_references
from trackio.cloud_snapshot import relative_path

SCOPE_FIELDS = ("workspace_id", "project_id", "job_id", "run_id")
WORKSPACE_SCHEMA_VERSION = 2


def scoped(value):
    return {key: identifier(value[key]) for key in SCOPE_FIELDS}


class WorkspaceStore:
    def __init__(
        self,
        repository,
        *,
        initialize=False,
        owner_role=None,
        connection=None,
        migrate_from=None,
    ):
        self.repository = repository
        self.engine = repository.engine
        self._connection = ContextVar("workspace_connection", default=None)
        metadata = MetaData(schema=repository.runs.schema)
        self.schema = Table(
            "trackio_workspace_schema",
            metadata,
            Column("version", Integer, primary_key=True),
        )
        self.heads = Table(
            "trackio_workspace_heads",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("kind", String(32), primary_key=True),
            Column("name", String(256), primary_key=True),
            Column("next_version", Integer, nullable=False),
        )
        self.resources = Table(
            "trackio_workspace_resources",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("kind", String(32), primary_key=True),
            Column("name", String(256), primary_key=True),
            Column("version", String(64), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("payload", Text, nullable=False),
        )
        self.aliases = Table(
            "trackio_workspace_aliases",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("kind", String(32), primary_key=True),
            Column("name", String(256), primary_key=True),
            Column("alias", String(256), primary_key=True),
            Column("version", String(64), nullable=False),
        )
        self.operations = Table(
            "trackio_workspace_operations",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("operation_id", String(256), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("payload", Text, nullable=False),
        )
        self.links = Table(
            "trackio_workspace_links",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("operation_id", String(256), primary_key=True),
            Column("source_run", String(256), nullable=False),
            Column("payload", Text, nullable=False),
        )
        self.state = Table(
            "trackio_workspace_runs",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("name", String(256), nullable=False),
            Column("expires_at", Float),
            Column("deleted", Integer, nullable=False),
            Column("revision", Integer, nullable=False),
        )
        self.imports = Table(
            "trackio_workspace_imports",
            metadata,
            Column("source_id", String(256), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("bindings_digest", String(64), nullable=False),
            Column("payload", Text, nullable=False),
        )
        self.legacy_rows = Table(
            "trackio_workspace_legacy_rows",
            metadata,
            Column("source_id", String(256), primary_key=True),
            Column("database", String(512), primary_key=True),
            Column("table_name", String(256), primary_key=True),
            Column("row_key", String(64), primary_key=True),
            Column("run_id", String(256), nullable=False, index=True),
            Column("payload", Text, nullable=False),
        )
        self.legacy_files = Table(
            "trackio_workspace_legacy_files",
            metadata,
            Column("source_id", String(256), primary_key=True),
            Column("path", String(512), primary_key=True),
            Column("run_id", String(256), nullable=False, index=True),
            Column("payload", Text, nullable=False),
        )
        self.uploads = Table(
            "trackio_workspace_uploads",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("digest", String(64), primary_key=True),
            Column("object_key", String(129), nullable=False),
            Column("scope", Text, nullable=False),
            Column("generation", String(64), nullable=False),
            Column("status", String(32), nullable=False),
            Column("created_at", Float, nullable=False),
        )
        self.upload_locations = Table(
            "trackio_workspace_upload_locations",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("object_key", String(129), primary_key=True),
            Column("digest", String(64), nullable=False),
            Column("scope", Text, nullable=False),
            Column("generation", String(64), nullable=False),
            Column("status", String(32), nullable=False),
            Column("created_at", Float, nullable=False),
        )
        with (
            self.engine.begin() if connection is None else nullcontext(connection)
        ) as connection:
            if owner_role is not None:
                if (
                    not initialize
                    or self.engine.dialect.name != "postgresql"
                    or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", owner_role)
                ):
                    raise ValueError("INVALID_WORKSPACE_OWNER_ROLE")
                connection.execute(text(f'SET LOCAL ROLE "{owner_role}"'))
            if self.engine.dialect.name == "postgresql" and initialize:
                lock = int.from_bytes(
                    hashlib.sha256(
                        (repository.runs.schema or "public").encode()
                    ).digest()[:8],
                    "big",
                    signed=True,
                )
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock}
                )
            exists = inspect(connection).has_table(
                self.schema.name, schema=metadata.schema
            )
            if migrate_from is not None:
                if not initialize or migrate_from not in range(
                    1, WORKSPACE_SCHEMA_VERSION
                ):
                    raise ValueError("INVALID_WORKSPACE_MIGRATION_SOURCE")
                if not exists or connection.execute(
                    select(self.schema.c.version)
                ).scalars().all() != [migrate_from]:
                    raise ValueError("WORKSPACE_MIGRATION_SOURCE_MISMATCH")
            if exists:
                versions = (
                    connection.execute(select(self.schema.c.version)).scalars().all()
                )
                upgrading = versions == [1] and initialize
                if versions != [WORKSPACE_SCHEMA_VERSION] and not upgrading:
                    raise ValueError("INCOMPATIBLE_WORKSPACE_SCHEMA")
                if any(
                    not inspect(connection).has_table(
                        table.name, schema=metadata.schema
                    )
                    for table in metadata.tables.values()
                    if not upgrading or table is not self.upload_locations
                ):
                    raise ValueError("INCOMPLETE_WORKSPACE_SCHEMA")
                if upgrading:
                    self.upload_locations.create(connection)
                    columns = [column.name for column in self.upload_locations.columns]
                    connection.execute(
                        self.upload_locations.insert().from_select(
                            columns, select(*[self.uploads.c[name] for name in columns])
                        )
                    )
                    connection.execute(
                        self.schema.update().values(version=WORKSPACE_SCHEMA_VERSION)
                    )
            elif initialize:
                metadata.create_all(connection)
                connection.execute(
                    self.schema.insert().values(version=WORKSPACE_SCHEMA_VERSION)
                )
            else:
                raise ValueError("WORKSPACE_SCHEMA_NOT_INITIALIZED")

    def _insert(self, table):
        return (
            pg_insert(table)
            if self.engine.dialect.name == "postgresql"
            else sqlite_insert(table)
        )

    @contextmanager
    def transaction(self, claims=None):
        current = self._connection.get()
        with (
            self.engine.begin() if current is None else nullcontext(current)
        ) as connection:
            token = self._connection.set(connection)
            try:
                if claims is not None:
                    self._guard(connection, claims)
                yield connection
            finally:
                self._connection.reset(token)

    def _lock(self, connection, scope, *, allow_deleted=False):
        scope = scoped(scope)
        runs = self.repository.runs
        matched = connection.execute(
            runs.update()
            .where(*[runs.c[key] == scope[key] for key in SCOPE_FIELDS])
            .values(updated_at=runs.c.updated_at)
        ).rowcount
        if matched != 1:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        connection.execute(
            self._insert(self.state)
            .values(run_id=scope["run_id"], name=scope["run_id"], deleted=0, revision=0)
            .on_conflict_do_nothing()
        )
        state = (
            connection.execute(
                select(self.state).where(self.state.c.run_id == scope["run_id"])
            )
            .mappings()
            .one()
        )
        if state["deleted"] and not allow_deleted:
            raise PermissionError("RUN_DELETED")
        return dict(state)

    def _guard(self, connection, claims):
        self.repository._rich_guard(connection, claims)
        return self._lock(connection, claims)

    def _receipt(self, connection, scope, operation_id, request):
        identifier(operation_id)
        previous = (
            connection.execute(
                select(self.operations).where(
                    self.operations.c.run_id == scope["run_id"],
                    self.operations.c.operation_id == operation_id,
                )
            )
            .mappings()
            .first()
        )
        if previous is None:
            return None
        if previous["digest"] != content_digest(request):
            raise Conflict("OPERATION_CONTENT_CONFLICT")
        return json.loads(previous["payload"])["result"]

    def _commit_receipt(self, connection, scope, operation_id, request, result):
        connection.execute(
            self.operations.insert().values(
                run_id=scope["run_id"],
                operation_id=operation_id,
                digest=content_digest(request),
                payload=canonical(
                    {"request": request, "result": result, "timestamp": time.time()}
                ),
            )
        )
        return result

    def _identity(self, scope, kind, name):
        if kind not in (
            "artifact",
            "logbook",
            "registry",
            "registry_metadata",
            "collection",
        ):
            raise ValueError("INVALID_RESOURCE_KIND")
        if not isinstance(name, str) or not re.fullmatch(
            r"[A-Za-z0-9._-]{1,256}", name
        ):
            raise ValueError("INVALID_RESOURCE_NAME")
        return {"run_id": scope["run_id"], "kind": kind, "name": name}

    def _validate_content(self, connection, scope, kind, content, *, name=None):
        if (
            not isinstance(content, dict)
            or len(canonical(content).encode()) > 512 * 1024
        ):
            raise ValueError("INVALID_RESOURCE_CONTENT")
        if content.get("formal_checkpoint"):
            raise ValueError("FORMAL_CHECKPOINT_REQUIRES_ANS")
        if kind == "artifact":
            identifier(content["artifact_type"])
            entries = content["manifest"]
        elif kind == "logbook":
            pages = content["pages"]
            if not isinstance(pages, dict) or any(
                not isinstance(value, str) for value in pages.values()
            ):
                raise ValueError("INVALID_LOGBOOK_PAGES")
            for path in pages:
                relative_path(path)
            entries = content.get("attachments", [])
        elif kind in ("registry_metadata", "collection"):
            if set(content) - {"description", "artifact_type", "created_at"}:
                raise ValueError("INVALID_CATALOG_METADATA")
            if content.get("description") is not None and not isinstance(
                content["description"], str
            ):
                raise ValueError("INVALID_CATALOG_METADATA")
            if kind == "collection":
                identifier(content["artifact_type"])
                existing = (
                    connection.execute(
                        select(self.resources.c.payload).where(
                            self.resources.c.run_id == scope["run_id"],
                            self.resources.c.kind == "collection",
                            self.resources.c.name == name,
                        )
                    )
                    .scalars()
                    .first()
                )
                if (
                    existing is not None
                    and json.loads(existing)["content"]["artifact_type"]
                    != content["artifact_type"]
                ):
                    raise ValueError("REGISTRY_ARTIFACT_TYPE_CONFLICT")
            entries = []
        else:
            entries = []
            source = content["source"]
            source_scope = scoped(source["scope"])
            if source_scope["workspace_id"] != scope["workspace_id"]:
                raise PermissionError("CROSS_WORKSPACE_LINK_DENIED")
            resource = self._resolve(
                connection, source_scope, "artifact", source["name"], source["version"]
            )
            if resource["digest"] != source["digest"]:
                raise ValueError("SOURCE_CONTENT_CONFLICT")
            if (
                name is not None
                and "collection" in content
                and content["collection"] != name
            ):
                raise ValueError("REGISTRY_COLLECTION_MISMATCH")
            collection_name = content.get("collection", name)
            collection_exists = (
                connection.execute(
                    select(self.resources.c.payload).where(
                        self.resources.c.run_id == scope["run_id"],
                        self.resources.c.kind == "collection",
                        self.resources.c.name == collection_name,
                    )
                )
                .scalars()
                .first()
            )
            if collection_exists is not None:
                collection = self._resolve(
                    connection, scope, "collection", collection_name, "latest"
                )
                if (
                    collection["content"]["artifact_type"]
                    != resource["content"]["artifact_type"]
                ):
                    raise ValueError("REGISTRY_ARTIFACT_TYPE_CONFLICT")
        if not isinstance(entries, list) or len(entries) > 4096:
            raise ValueError("INVALID_RESOURCE_MANIFEST")
        paths = set()
        for entry in entries:
            path = str(relative_path(entry["path"]))
            if path in paths:
                raise ValueError("DUPLICATE_RESOURCE_PATH")
            paths.add(path)
            if "_object" not in entry:
                raise ValueError("OBJECT_REFERENCE_REQUIRED")
            if type(entry.get("size")) is not int or entry["size"] < 0:
                raise ValueError("INVALID_OBJECT_SIZE")
        objects = self.repository.objects
        for digest in object_references(content):
            row = connection.execute(
                select(objects.c.size).where(
                    objects.c.run_id == scope["run_id"], objects.c.digest == digest
                )
            ).first()
            if row is None:
                raise ValueError("OBJECT_NOT_COMMITTED")
            if any(
                entry.get("_object") == digest and entry["size"] != row[0]
                for entry in entries
            ):
                raise ValueError("OBJECT_SIZE_MISMATCH")

    def _publish(self, connection, scope, kind, name, content, aliases):
        identity = self._identity(scope, kind, name)
        self._validate_content(connection, scope, kind, content, name=name)
        if (
            not isinstance(aliases, list)
            or len(aliases) > 64
            or any(
                not isinstance(alias, str)
                or not re.fullmatch(r"[A-Za-z0-9._-]{1,256}", alias)
                or re.fullmatch(r"v[0-9]+", alias)
                for alias in aliases
            )
        ):
            raise ValueError("INVALID_RESOURCE_ALIAS")
        connection.execute(
            self._insert(self.heads)
            .values(**identity, next_version=0)
            .on_conflict_do_nothing()
        )
        conditions = [self.resources.c[key] == value for key, value in identity.items()]
        digest = content_digest(content)
        previous = (
            connection.execute(
                select(self.resources).where(
                    *conditions, self.resources.c.digest == digest
                )
            )
            .mappings()
            .first()
        )
        if previous is None:
            number = connection.execute(
                select(self.heads.c.next_version)
                .where(*[self.heads.c[key] == value for key, value in identity.items()])
                .with_for_update()
            ).scalar_one()
            version = "v" + str(number)
            result = {
                "scope": scoped(scope),
                "kind": kind,
                "name": name,
                "version": version,
                "digest": digest,
                "content": content,
            }
            connection.execute(
                self.resources.insert().values(
                    **identity,
                    version=version,
                    digest=digest,
                    payload=canonical(result),
                )
            )
            connection.execute(
                self.heads.update()
                .where(*[self.heads.c[key] == value for key, value in identity.items()])
                .values(next_version=number + 1)
            )
        else:
            version, result = previous["version"], json.loads(previous["payload"])
        for alias in ["latest", *aliases]:
            connection.execute(
                self._insert(self.aliases)
                .values(**identity, alias=alias, version=version)
                .on_conflict_do_update(
                    index_elements=[
                        self.aliases.c[key] for key in (*identity, "alias")
                    ],
                    set_={"version": version},
                )
            )
        result["aliases"] = list(dict.fromkeys(["latest", *aliases]))
        return result

    def publish(self, claims, kind, name, content, *, operation_id, aliases=None):
        scope = scoped(claims)
        request = {
            "scope": scope,
            "kind": kind,
            "name": name,
            "content": content,
            "aliases": aliases or [],
        }
        with self.transaction() as connection:
            self._guard(connection, claims)
            if kind == "registry" and scoped(content["source"]["scope"]) != scope:
                raise PermissionError("CROSS_RUN_LINK_REQUIRES_SERVICE_AUTH")
            previous = self._receipt(connection, scope, operation_id, request)
            if previous is not None:
                return previous
            result = self._publish(
                connection, scope, kind, name, content, aliases or []
            )
            return self._commit_receipt(
                connection, scope, operation_id, request, result
            )

    def _resolve(self, connection, scope, kind, name, version):
        identity = self._identity(scope, kind, name)
        if not re.fullmatch(r"v[0-9]+", identifier(version)):
            version = connection.execute(
                select(self.aliases.c.version).where(
                    *[self.aliases.c[key] == value for key, value in identity.items()],
                    self.aliases.c.alias == version,
                )
            ).scalar_one_or_none()
        payload = connection.execute(
            select(self.resources.c.payload).where(
                *[self.resources.c[key] == value for key, value in identity.items()],
                self.resources.c.version == version,
            )
        ).scalar_one_or_none()
        if payload is None:
            raise ValueError("RESOURCE_NOT_FOUND")
        result = json.loads(payload)
        result["scope"] = scoped(scope)
        if kind == "registry":
            source = result["content"]["source"]["scope"]
            current = (
                connection.execute(
                    select(self.repository.runs).where(
                        *[
                            self.repository.runs.c[key] == source[key]
                            for key in ("workspace_id", "job_id", "run_id")
                        ]
                    )
                )
                .mappings()
                .first()
            )
            if current is not None and all(
                source[key] == scope[key]
                for key in ("workspace_id", "job_id", "run_id")
            ):
                result["resolved_source_scope"] = scoped(current)
        result["aliases"] = (
            connection.execute(
                select(self.aliases.c.alias)
                .where(
                    *[self.aliases.c[key] == value for key, value in identity.items()],
                    self.aliases.c.version == version,
                )
                .order_by(self.aliases.c.alias)
            )
            .scalars()
            .all()
        )
        return result

    def resolve(self, scope, kind, name, version="latest"):
        with self.transaction() as connection:
            self._lock(connection, scope)
            return self._resolve(connection, scope, kind, name, version)

    def link(
        self, source, target, name, version, target_kind, target_name, *, operation_id
    ):
        source, target = scoped(source), scoped(target)
        if source["workspace_id"] != target["workspace_id"]:
            raise PermissionError("CROSS_WORKSPACE_LINK_DENIED")
        if target_kind not in ("registry", "artifact"):
            raise ValueError("INVALID_LINK_KIND")
        request = dict(
            source=source,
            target=target,
            name=name,
            version=version,
            kind=target_kind,
            target_name=target_name,
        )
        with self.transaction() as connection:
            for scope in sorted((source, target), key=lambda scope: scope["run_id"]):
                self._lock(connection, scope)
            previous = self._receipt(connection, target, operation_id, request)
            if previous is not None:
                return previous
            resource = self._resolve(connection, source, "artifact", name, version)
            result = {
                "source": {**resource, "scope": source},
                "target": target,
                "kind": target_kind,
                "name": target_name,
            }
            self._publish(
                connection,
                target,
                "registry",
                target_name,
                {
                    "source": {
                        "scope": source,
                        "name": name,
                        "version": resource["version"],
                        "digest": resource["digest"],
                    }
                },
                [],
            )
            connection.execute(
                self.links.insert().values(
                    run_id=target["run_id"],
                    operation_id=operation_id,
                    source_run=source["run_id"],
                    payload=canonical(result),
                )
            )
            return self._commit_receipt(
                connection, target, operation_id, request, result
            )

    def lineage(self, scope, name, version, *, visible_scopes=None):
        with self.transaction() as connection:
            self._lock(connection, scope)
            resource = self._resolve(connection, scope, "artifact", name, version)
            links = (
                connection.execute(
                    select(self.links.c.payload)
                    .where(self.links.c.source_run == scope["run_id"])
                    .limit(1001)
                )
                .scalars()
                .all()
            )
            if len(links) > 1000:
                raise ValueError("LINEAGE_QUERY_LIMIT")
            results = []
            for payload in links:
                link = json.loads(payload)
                target = link["target"]
                current = (
                    connection.execute(
                        select(self.repository.runs).where(
                            *[
                                self.repository.runs.c[key] == target[key]
                                for key in ("workspace_id", "job_id", "run_id")
                            ]
                        )
                    )
                    .mappings()
                    .first()
                )
                if current is None or current["workspace_id"] != scope["workspace_id"]:
                    continue
                target = scoped(current)
                if (
                    link["source"]["name"] == name
                    and link["source"]["digest"] == resource["digest"]
                    and (visible_scopes is None or target in visible_scopes)
                ):
                    link["resolved_target_scope"] = target
                    results.append(link)
            return results

    def list_runs(self, scope, *, filters=None, after=None, limit=100):
        required = {key: identifier(scope[key]) for key in SCOPE_FIELDS[:3]}
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("INVALID_QUERY_LIMIT")
        filters = filters or {}
        if not isinstance(filters, dict) or set(filters) - {"name", "config"}:
            raise ValueError("INVALID_RUN_FILTER")
        config_filter = filters.get("config", {})
        if not isinstance(config_filter, dict) or len(config_filter) > 32:
            raise ValueError("INVALID_RUN_FILTER")
        runs = self.repository.runs
        conditions = [runs.c[key] == value for key, value in required.items()]
        conditions.append(func.coalesce(self.state.c.deleted, 0) == 0)
        if scope.get("run_id") is not None:
            conditions.append(runs.c.run_id == identifier(scope["run_id"]))
        if after is not None:
            conditions.append(runs.c.run_id > identifier(after))
        if "name" in filters:
            conditions.append(
                func.coalesce(self.state.c.name, runs.c.run_id)
                == identifier(filters["name"])
            )
        statement = (
            select(
                runs, self.state.c.name, self.state.c.expires_at, self.state.c.revision
            )
            .outerjoin(self.state, self.state.c.run_id == runs.c.run_id)
            .where(*conditions)
            .order_by(runs.c.run_id)
            .limit(5001)
        )
        results = []
        scanned = 0
        with self.engine.connect() as connection:
            for row in connection.execute(statement).mappings():
                scanned += 1
                if scanned > 5000:
                    raise ValueError("RUN_FILTER_SCAN_LIMIT")
                config = json.loads(row["config"])
                update = connection.execute(
                    select(self.repository.configs.c.payload).where(
                        self.repository.configs.c.run_id == row["run_id"]
                    )
                ).scalar_one_or_none()
                if update:
                    config.update(json.loads(update))
                if any(
                    config.get(key) != value or key not in config
                    for key, value in config_filter.items()
                ):
                    continue
                results.append(
                    {
                        **{key: row[key] for key in SCOPE_FIELDS},
                        "name": row["name"] or row["run_id"],
                        "config": config,
                        "expires_at": row["expires_at"],
                        "revision": row["revision"] or 0,
                    }
                )
                if len(results) > limit:
                    break
        return {
            "runs": results[:limit],
            "next": results[limit - 1]["run_id"] if len(results) > limit else None,
        }

    def mutate(self, scope, action, values, *, operation_id):
        scope = scoped(scope)
        if action not in ("rename", "move", "delete", "retention") or not isinstance(
            values, dict
        ):
            raise ValueError("INVALID_RUN_OPERATION")
        request = {"scope": scope, "action": action, "values": values}
        with self.transaction() as connection:
            try:
                state = self._lock(connection, scope, allow_deleted=True)
            except PermissionError:
                if action != "move" or set(values) != {"destination"}:
                    raise
                destination = scoped(values["destination"])
                if any(
                    destination[key] != scope[key]
                    for key in ("workspace_id", "job_id", "run_id")
                ):
                    raise PermissionError("MOVE_IDENTITY_CHANGE_DENIED")
                self._lock(connection, destination, allow_deleted=True)
                previous = self._receipt(connection, scope, operation_id, request)
                if previous is None or previous["scope"] != destination:
                    raise PermissionError("RUN_SCOPE_MISMATCH")
                return previous
            previous = self._receipt(connection, scope, operation_id, request)
            if previous is not None:
                return previous
            if state["deleted"]:
                raise PermissionError("RUN_DELETED")
            changes = {"revision": state["revision"] + 1}
            result_scope = scope
            if action == "rename":
                if set(values) != {"name"}:
                    raise ValueError("INVALID_RUN_OPERATION")
                changes["name"] = identifier(values["name"])
            elif action == "retention":
                if set(values) != {"expires_at"} or (
                    values["expires_at"] is not None
                    and (
                        type(values["expires_at"]) not in (int, float)
                        or not self.repository.is_finite(values["expires_at"])
                    )
                ):
                    raise ValueError("INVALID_RETENTION")
                changes["expires_at"] = values["expires_at"]
            else:
                if action == "move":
                    if set(values) != {"destination"}:
                        raise ValueError("INVALID_RUN_OPERATION")
                    destination = scoped(values["destination"])
                    if any(
                        destination[key] != scope[key]
                        for key in ("workspace_id", "job_id", "run_id")
                    ):
                        raise PermissionError("MOVE_IDENTITY_CHANGE_DENIED")
                    connection.execute(
                        self.repository.runs.update()
                        .where(self.repository.runs.c.run_id == scope["run_id"])
                        .values(project_id=destination["project_id"])
                    )
                    result_scope = destination
                else:
                    if values:
                        raise ValueError("INVALID_RUN_OPERATION")
                    changes["deleted"] = 1
                connection.execute(
                    self.repository.capabilities.update()
                    .where(self.repository.capabilities.c.run_id == scope["run_id"])
                    .values(revoked=1)
                )
            connection.execute(
                self.state.update()
                .where(self.state.c.run_id == scope["run_id"])
                .values(**changes)
            )
            result = {**state, **changes, "scope": result_scope, "committed": True}
            return self._commit_receipt(
                connection, scope, operation_id, request, result
            )

    def purge(self, scope):
        scope = scoped(scope)
        now = time.time()
        with self.transaction() as connection:
            state = self._lock(connection, scope, allow_deleted=True)
            expired = state["expires_at"] is not None and state["expires_at"] <= now
            if not state["deleted"] and not expired:
                return {"deleted": 0}
            connection.execute(
                self.state.update()
                .where(self.state.c.run_id == scope["run_id"])
                .values(deleted=1, revision=self.state.c.revision + 1)
            )
            connection.execute(
                self.repository.capabilities.update()
                .where(self.repository.capabilities.c.run_id == scope["run_id"])
                .values(revoked=1)
            )
            for table in (
                self.repository.records,
                self.repository.batches,
                self.repository.documents,
                self.repository.configs,
                self.resources,
                self.heads,
                self.aliases,
                self.links,
                self.legacy_rows,
                self.legacy_files,
                self.operations,
            ):
                connection.execute(
                    table.delete().where(table.c.run_id == scope["run_id"])
                )
            return {"deleted": 1}

    def collect_objects(self, scope, storage):
        collector = importlib.import_module("trackio.cloud_gc")
        return collector.collect(self, scoped(scope), storage, deleted=True, limit=1000)

    def collect_orphans(
        self, scope, storage, *, retention_seconds=86400, limit=100, after=None
    ):
        collector = importlib.import_module("trackio.cloud_gc")
        return collector.collect(
            self,
            scoped(scope),
            storage,
            retention_seconds=retention_seconds,
            limit=limit,
            after=after,
        )

    def export(self, scope, *, max_records=10000, max_bytes=16 * 1024 * 1024):
        scope = scoped(scope)
        if (
            type(max_records) is not int
            or not 1 <= max_records <= 10000
            or type(max_bytes) is not int
            or not 1 <= max_bytes <= 64 * 1024 * 1024
        ):
            raise ValueError("INVALID_EXPORT_LIMIT")
        with self.transaction() as connection:
            state = self._lock(connection, scope)
            config = connection.execute(
                select(self.repository.runs.c.config).where(
                    self.repository.runs.c.run_id == scope["run_id"]
                )
            ).scalar_one()
            result = {
                "format_version": 1,
                "scope": scope,
                "run": state,
                "initial_config": json.loads(config),
            }
            count = 0
            tables = {
                "events": self.repository.records,
                "records": self.repository.documents,
                "config_updates": self.repository.configs,
                "resources": self.resources,
                "aliases": self.aliases,
                "links": self.links,
                "objects": self.repository.objects,
                "operations": self.operations,
                "legacy_rows": self.legacy_rows,
                "legacy_files": self.legacy_files,
            }
            for name, table in tables.items():
                statement = (
                    select(table)
                    .where(table.c.run_id == scope["run_id"])
                    .order_by(*table.primary_key.columns)
                    .limit(max_records - count + 1)
                )
                entries = [
                    dict(row) for row in connection.execute(statement).mappings()
                ]
                count += len(entries)
                if count > max_records:
                    raise ValueError("EXPORT_RECORD_LIMIT")
                for entry in entries:
                    entry.pop("object_key", None)
                result[name] = entries
                if len(canonical(result).encode()) > max_bytes:
                    raise ValueError("EXPORT_SIZE_LIMIT")
            result["digest"] = content_digest(result)
            return result

    def compare(
        self,
        scopes,
        *,
        metrics=None,
        start=None,
        end=None,
        min_step=None,
        max_step=None,
        max_points=2000,
    ):
        if (
            not isinstance(scopes, list)
            or not 1 <= len(scopes) <= 16
            or type(max_points) is not int
            or not 1 <= max_points <= 10000
        ):
            raise ValueError("INVALID_COMPARE_LIMIT")
        if metrics is not None and (
            not isinstance(metrics, list)
            or len(metrics) > 64
            or any(not isinstance(metric, str) for metric in metrics)
        ):
            raise ValueError("INVALID_METRIC_FILTER")
        for value in (start, end):
            if value is not None and not self.repository.is_finite(value):
                raise ValueError("INVALID_TIME_RANGE")
        for value in (min_step, max_step):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("INVALID_STEP_RANGE")
        if (start is not None and end is not None and start > end) or (
            min_step is not None and max_step is not None and min_step > max_step
        ):
            raise ValueError("INVALID_COMPARE_RANGE")
        scopes = [scoped(scope) for scope in scopes]
        if len({scope["workspace_id"] for scope in scopes}) != 1:
            raise PermissionError("CROSS_WORKSPACE_COMPARE_DENIED")
        with self.transaction() as connection:
            for scope in sorted(scopes, key=lambda value: value["run_id"]):
                self._lock(connection, scope)
            results = []
            total = 0
            for scope in scopes:
                conditions = [self.repository.records.c.run_id == scope["run_id"]]
                if start is not None:
                    conditions.append(self.repository.records.c.observed_at >= start)
                if end is not None:
                    conditions.append(self.repository.records.c.observed_at <= end)
                if self.engine.dialect.name == "postgresql":
                    step = cast(self.repository.records.c.payload, JSONB)[
                        "step"
                    ].as_integer()
                else:
                    step = func.json_extract(
                        self.repository.records.c.payload, "$.step"
                    )
                if min_step is not None:
                    conditions.append(step >= min_step)
                if max_step is not None:
                    conditions.append(step <= max_step)
                entries = (
                    connection.execute(
                        select(self.repository.records.c.payload)
                        .where(*conditions)
                        .order_by(
                            self.repository.records.c.observed_at,
                            self.repository.records.c.event_id,
                        )
                        .limit(max_points - total + 1)
                    )
                    .scalars()
                    .all()
                )
                total += len(entries)
                if total > max_points:
                    raise ValueError("COMPARE_POINT_LIMIT")
                points = [json.loads(payload) for payload in entries]
                points = [
                    point
                    for point in points
                    if (
                        min_step is None
                        or point.get("step") is not None
                        and point["step"] >= min_step
                    )
                    and (
                        max_step is None
                        or point.get("step") is not None
                        and point["step"] <= max_step
                    )
                ]
                if metrics is not None:
                    for point in points:
                        point["metrics"] = {
                            key: value
                            for key, value in point["metrics"].items()
                            if key in metrics
                        }
                results.append({"scope": scope, "points": points})
            return {
                "runs": results,
                "truncated": False,
                "axis": "observed_at",
                "step_field": "step",
                "trial_field": "trial_id",
            }


class WorkspaceRepository(MetricsRepository):
    def __init__(
        self, *args, initialize_workspace=False, migrate_workspace_from=None, **kwargs
    ):
        if migrate_workspace_from is not None and (
            kwargs.get("initialize", True) or kwargs.get("migrate_from") is not None
        ):
            raise ValueError("WORKSPACE_MIGRATION_REQUIRES_EXISTING_CORE")

        def extension(repository, connection):
            self.workspace = WorkspaceStore(
                repository,
                initialize=initialize_workspace,
                connection=connection,
                migrate_from=migrate_workspace_from,
            )

        super().__init__(
            *args,
            _schema_extension=extension,
            _schema_extension_ddl=migrate_workspace_from is not None,
            **kwargs,
        )

    def _workspace_fence(self, connection, run_id):
        deleted = connection.execute(
            select(self.workspace.state.c.deleted).where(
                self.workspace.state.c.run_id == run_id
            )
        ).scalar_one_or_none()
        if deleted:
            raise PermissionError("RUN_DELETED")

    def _live_capability(self, connection, capability_id, run_id):
        super()._live_capability(connection, capability_id, run_id)
        self._workspace_fence(connection, run_id)

    def _live_legacy(self, connection, claims):
        super()._live_legacy(connection, claims)
        self._workspace_fence(connection, claims["run_id"])

    def read_records(self, scope, kind=None, after=None):
        with self.engine.begin() as connection:
            self.workspace._lock(connection, scope)
            return super().read_records(scope, kind, after)

    def read_object(self, scope, digest, storage, *, claims=None):
        with self.engine.begin() as connection:
            if claims is not None:
                self._rich_guard(connection, claims)
            self.workspace._lock(connection, scope)
            return super().read_object(scope, digest, storage)

    def put_object(self, claims, digest, payload, content_type, storage):
        return self._put_object(
            scoped(claims), digest, payload, content_type, storage, claims=claims
        )

    def put_import_object(self, scope, payload, storage):
        digest = hashlib.sha256(payload).hexdigest()
        self._put_object(
            scoped(scope), digest, payload, "application/octet-stream", storage
        )
        return digest

    def _put_object(
        self, scope, digest, payload, content_type, storage, *, claims=None
    ):
        journal = importlib.import_module("trackio.cloud_uploads")
        return journal.put(
            self, scope, digest, payload, content_type, storage, claims=claims
        )
