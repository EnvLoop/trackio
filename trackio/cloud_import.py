"""Explicit offline ETL with source identities, content verification, and readback."""

import base64
import hashlib
import json
from contextlib import closing
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select

from trackio.cloud import Conflict, canonical, finite, identifier
from trackio.cloud_objects import MAX_OBJECT_BYTES
from trackio.cloud_snapshot import _open_database, _table_rows, verify
from trackio.cloud_workspace import content_digest, scoped


def encoded_row(row):
    return {
        key: {"bytes": base64.b64encode(value).decode()}
        if isinstance(value, bytes)
        else value
        for key, value in row.items()
    }


class LegacyImporter:
    def __init__(self, repository, objects):
        if objects is None:
            raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
        self.repository, self.objects = repository, objects
        self.store = repository.workspace

    def _row_scope(self, binding, row):
        run_id = row.get("run_id") or row.get("producer_run_id")
        if run_id is not None:
            if run_id not in binding.get("runs", {}):
                raise ValueError("LEGACY_RUN_BINDING_REQUIRED")
            return scoped(binding["runs"][run_id])
        name = row.get("run_name") or row.get("producer_run_name")
        if name is not None and name in binding.get("names", {}):
            return scoped(binding["names"][name])
        if "default_scope" not in binding:
            raise ValueError("LEGACY_DEFAULT_SCOPE_REQUIRED")
        return scoped(binding["default_scope"])

    def _object(self, connection, scope, content):
        digest = hashlib.sha256(content).hexdigest()
        key = content_digest(scope) + "/" + digest
        table = self.repository.objects
        previous = (
            connection.execute(
                select(table).where(
                    table.c.run_id == scope["run_id"], table.c.digest == digest
                )
            )
            .mappings()
            .first()
        )
        if previous is not None:
            self.objects.get(previous["object_key"], digest, previous["size"])
            return {"_object": digest, "size": len(content)}
        used = (
            connection.execute(
                select(func.sum(table.c.size)).where(table.c.run_id == scope["run_id"])
            ).scalar_one()
            or 0
        )
        if used + len(content) > 100 * 1024 * 1024:
            raise ValueError("RUN_OBJECT_QUOTA_EXCEEDED")
        self.objects.put(key, content, digest, "application/octet-stream")
        self.objects.get(key, digest, len(content))
        connection.execute(
            table.insert().values(
                run_id=scope["run_id"],
                digest=digest,
                size=len(content),
                content_type="application/octet-stream",
                object_key=key,
            )
        )
        return {"_object": digest, "size": len(content)}

    def _file(self, connection, snapshot, source_id, name, info, scope):
        chunks = []
        with (Path(snapshot) / "files" / name).open("rb") as stream:
            while content := stream.read(MAX_OBJECT_BYTES):
                chunks.append(self._object(connection, scope, content))
            if not chunks:
                chunks.append(self._object(connection, scope, b""))
        manifest = {"digest": info["digest"], "size": info["size"], "chunks": chunks}
        connection.execute(
            self.store.legacy_files.insert().values(
                source_id=source_id,
                path=name,
                run_id=scope["run_id"],
                payload=canonical(manifest),
            )
        )
        return manifest

    def _timestamp(self, row):
        timestamp = row.get("timestamp") or row.get("created_at")
        if finite(timestamp):
            return timestamp
        if isinstance(timestamp, str):
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("LEGACY_TIMESTAMP_TIMEZONE_REQUIRED")
            return parsed.timestamp()
        raise ValueError("LEGACY_TIMESTAMP_REQUIRED")

    def _insert_event(self, connection, scope, event):
        table = self.repository.records
        encoded = canonical(event)
        previous = connection.execute(
            select(table.c.payload).where(
                table.c.run_id == scope["run_id"], table.c.event_id == event["event_id"]
            )
        ).scalar_one_or_none()
        if previous is not None:
            if previous != encoded:
                raise Conflict("LEGACY_EVENT_CONTENT_CONFLICT")
            return 0
        connection.execute(
            table.insert().values(
                run_id=scope["run_id"],
                event_id=event["event_id"],
                observed_at=event["observed_at"],
                trial_id=None,
                payload=encoded,
            )
        )
        return 1

    def _document(
        self, connection, scope, kind, record_id, payload, timestamp, step=None
    ):
        record = {
            "record_id": record_id,
            "kind": kind,
            "payload": payload,
            "observed_at": timestamp,
            "step": step,
            "trial_id": None,
            "writer_id": "legacy-import",
        }
        encoded = canonical(record)
        table = self.repository.documents
        previous = connection.execute(
            select(table.c.payload).where(
                table.c.run_id == scope["run_id"], table.c.record_id == record_id
            )
        ).scalar_one_or_none()
        if previous is not None:
            if previous != encoded:
                raise Conflict("LEGACY_RECORD_CONTENT_CONFLICT")
            return
        connection.execute(
            table.insert().values(
                run_id=scope["run_id"],
                record_id=record_id,
                kind=kind,
                observed_at=timestamp,
                payload=encoded,
            )
        )

    def _rich(self, value, scope, files):
        if isinstance(value, list):
            return [self._rich(child, scope, files) for child in value]
        if not isinstance(value, dict):
            return value
        if "file_path" in value:
            path = "media/" + str(value["file_path"]).replace("\\", "/")
            if path not in files or files[path]["scope"] != scope:
                raise ValueError("LEGACY_MEDIA_BINDING_REQUIRED")
            reference = files[path]["manifest"]
            return {
                **{
                    key: self._rich(child, scope, files)
                    for key, child in value.items()
                    if key != "file_path"
                },
                "_content_digest": reference["digest"],
                "size": reference["size"],
                "chunks": reference["chunks"],
            }
        return {key: self._rich(child, scope, files) for key, child in value.items()}

    def _project(
        self, connection, scope, source_id, snapshot_digest, table, row, row_key, files
    ):
        if table == "configs":
            config = json.loads(row["config"])
            if not isinstance(config, dict):
                raise ValueError("INVALID_LEGACY_CONFIG")
            target = self.repository.configs
            previous = connection.execute(
                select(target.c.payload).where(target.c.run_id == scope["run_id"])
            ).scalar_one_or_none()
            merged = json.loads(previous) if previous is not None else {}
            if any(
                key in merged and merged[key] != value for key, value in config.items()
            ):
                raise Conflict("LEGACY_CONFIG_CONTENT_CONFLICT")
            connection.execute(
                self.store._insert(target)
                .values(run_id=scope["run_id"], payload=canonical({**merged, **config}))
                .on_conflict_do_update(
                    index_elements=[target.c.run_id],
                    set_={"payload": canonical({**merged, **config})},
                )
            )
            return 0
        if table not in ("metrics", "system_metrics", "alerts", "traces"):
            return 0
        source_event = str(
            row.get("log_id") or row.get("alert_id") or (table + ":" + row_key)
        )
        event_id = "legacy-" + content_digest(
            {"source_id": source_id, "event": source_event}
        )
        timestamp = self._timestamp(row)
        step = row.get("step")
        if step is not None and (type(step) is not int or step < 0):
            raise ValueError("INVALID_LEGACY_STEP")
        provenance = {
            "source_id": source_id,
            "snapshot_digest": snapshot_digest,
            "legacy_event_id": source_event,
        }
        if table in ("alerts", "traces"):
            self._document(
                connection,
                scope,
                "alert" if table == "alerts" else "trace",
                event_id,
                {"legacy": encoded_row(row), "source": provenance},
                timestamp,
                step,
            )
            return 0
        values = (
            json.loads(row["metrics"])
            if isinstance(row["metrics"], (str, bytes))
            else row["metrics"]
        )
        if not isinstance(values, dict):
            raise ValueError("INVALID_LEGACY_METRICS")
        scalars = {key: value for key, value in values.items() if finite(value)}
        rich = {key: value for key, value in values.items() if key not in scalars}
        accepted = 0
        if scalars:
            event = {
                "event_id": event_id,
                "writer_id": "legacy-import",
                "trial_id": None,
                "step": step,
                "observed_at": timestamp,
                "category": "compute" if table == "system_metrics" else "experiment",
                "metrics": scalars,
                "source": provenance,
            }
            accepted = self._insert_event(connection, scope, event)
        if rich:
            self._document(
                connection,
                scope,
                "history",
                event_id,
                {"values": self._rich(rich, scope, files), "source": provenance},
                timestamp,
                step,
            )
        return accepted

    def _artifacts(
        self, connection, database, database_info, binding, source_id, files
    ):
        tables = database_info["tables"]
        if "artifacts" not in tables or "artifact_versions" not in tables:
            return
        if (
            tables["artifact_versions"]["rows"] > 10000
            or tables["artifacts"]["rows"] > 10000
        ):
            raise ValueError("LEGACY_ARTIFACT_LIMIT")
        artifacts = {row["id"]: row for row in _table_rows(database, "artifacts")}
        versions = {}
        for row in _table_rows(database, "artifact_versions"):
            scope = self._row_scope(binding, row)
            artifact = artifacts[row["artifact_id"]]
            entries = []
            for entry in json.loads(row["manifest"]):
                if "ref" in entry:
                    entries.append(dict(entry))
                    continue
                candidates = [
                    file["manifest"]
                    for file in files.values()
                    if file["scope"] == scope
                    and file["manifest"]["digest"] == entry["digest"]
                ]
                if (
                    not candidates
                    or len(candidates[0]["chunks"]) != 1
                    or candidates[0]["size"] != entry["size"]
                ):
                    raise ValueError("LEGACY_ARTIFACT_FILE_BINDING_REQUIRED")
                entries.append(
                    {
                        "path": entry["path"],
                        "_object": candidates[0]["chunks"][0]["_object"],
                        "size": entry["size"],
                    }
                )
            content = {
                "artifact_type": artifact["type"],
                "description": artifact.get("description"),
                "metadata": json.loads(row["metadata"]) if row.get("metadata") else {},
                "manifest": entries,
                "legacy_manifest_digest": row["manifest_digest"],
                "source_id": source_id,
            }
            version = "v" + str(row["version"])
            identity = {
                "run_id": scope["run_id"],
                "kind": "artifact",
                "name": artifact["name"],
            }
            result = {
                "scope": scope,
                "kind": "artifact",
                "name": artifact["name"],
                "version": version,
                "digest": content_digest(content),
                "content": content,
            }
            previous = connection.execute(
                select(self.store.resources.c.digest).where(
                    *[
                        self.store.resources.c[key] == value
                        for key, value in identity.items()
                    ],
                    self.store.resources.c.version == version,
                )
            ).scalar_one_or_none()
            if previous is not None and previous != result["digest"]:
                raise Conflict("LEGACY_ARTIFACT_CONTENT_CONFLICT")
            if previous is None:
                connection.execute(
                    self.store.resources.insert().values(
                        **identity,
                        version=version,
                        digest=result["digest"],
                        payload=canonical(result),
                    )
                )
            connection.execute(
                self.store._insert(self.store.heads)
                .values(**identity, next_version=row["version"] + 1)
                .on_conflict_do_nothing()
            )
            connection.execute(
                self.store.heads.update()
                .where(
                    *[
                        self.store.heads.c[key] == value
                        for key, value in identity.items()
                    ],
                    self.store.heads.c.next_version <= row["version"],
                )
                .values(next_version=row["version"] + 1)
            )
            versions[row["id"]] = result
        if "artifact_aliases" in tables:
            for alias in _table_rows(database, "artifact_aliases"):
                if alias["artifact_version_id"] not in versions:
                    raise ValueError("LEGACY_ALIAS_VERSION_MISSING")
                result = versions[alias["artifact_version_id"]]
                identity = {
                    "run_id": result["scope"]["run_id"],
                    "kind": "artifact",
                    "name": result["name"],
                    "alias": alias["alias"],
                }
                connection.execute(
                    self.store._insert(self.store.aliases)
                    .values(**identity, version=result["version"])
                    .on_conflict_do_update(
                        index_elements=[self.store.aliases.c[key] for key in identity],
                        set_={"version": result["version"]},
                    )
                )
        if "run_artifact_links" in tables:
            for row in _table_rows(database, "run_artifact_links"):
                target = self._row_scope(binding, row)
                resource = versions[row["artifact_version_id"]]
                result = {
                    "source": resource,
                    "target": target,
                    "kind": row["direction"],
                    "name": resource["name"],
                    "source_id": source_id,
                }
                connection.execute(
                    self.store.links.insert().values(
                        run_id=target["run_id"],
                        operation_id="legacy-"
                        + content_digest(
                            {"source_id": source_id, "link": encoded_row(row)}
                        ),
                        source_run=resource["scope"]["run_id"],
                        payload=canonical(result),
                    )
                )

    def _registry(self, connection, database_name, database, info, bindings, source_id):
        if "collections" not in info["tables"]:
            return
        binding = bindings["databases"][database_name]
        scope = self._row_scope(binding, {})
        registry = binding.get("registry_name")
        if registry is None:
            stem = Path(database_name).stem
            if not stem.startswith("registry-"):
                raise ValueError("LEGACY_REGISTRY_NAME_BINDING_REQUIRED")
            registry = stem.removeprefix("registry-")
        self.store._publish(
            connection, scope, "registry_metadata", registry, {"description": None}, []
        )
        collections = {row["id"]: row for row in _table_rows(database, "collections")}
        for collection in collections.values():
            self.store._publish(
                connection,
                scope,
                "collection",
                registry + "." + collection["name"],
                {
                    "artifact_type": collection["type"],
                    "description": collection.get("description"),
                    "created_at": collection["created_at"],
                },
                [],
            )
        aliases = {}
        if "collection_aliases" in info["tables"]:
            for row in _table_rows(database, "collection_aliases"):
                aliases.setdefault(row["link_id"], []).append(row["alias"])
        if "collection_links" in info["tables"]:
            for row in _table_rows(database, "collection_links"):
                source_bindings = [
                    value
                    for name, value in bindings["databases"].items()
                    if value.get("legacy_project", Path(name).stem)
                    == row["source_project"]
                ]
                if len(source_bindings) != 1:
                    raise ValueError("LEGACY_REGISTRY_SOURCE_BINDING_REQUIRED")
                source_binding = source_bindings[0]
                source_scopes = {
                    canonical(scoped(value)): scoped(value)
                    for value in source_binding.get("runs", {}).values()
                }
                if "default_scope" in source_binding:
                    value = scoped(source_binding["default_scope"])
                    source_scopes[canonical(value)] = value
                matches = []
                for candidate in source_scopes.values():
                    identity = self.store._identity(
                        candidate, "artifact", row["source_artifact"]
                    )
                    exists = connection.execute(
                        select(self.store.resources.c.payload).where(
                            *[
                                self.store.resources.c[key] == value
                                for key, value in identity.items()
                            ],
                            self.store.resources.c.version
                            == "v" + str(row["source_version"]),
                        )
                    ).scalar_one_or_none()
                    if exists is not None:
                        matches.append(json.loads(exists))
                if len(matches) != 1:
                    raise ValueError("LEGACY_REGISTRY_SOURCE_VERSION_REQUIRED")
                source = matches[0]
                collection = collections[row["collection_id"]]
                name = registry + "." + collection["name"]
                content = {
                    "source": {
                        "scope": source["scope"],
                        "name": source["name"],
                        "version": source["version"],
                        "digest": source["digest"],
                    },
                    "collection": name,
                }
                self.store._validate_content(connection, scope, "registry", content)
                result = {
                    "scope": scope,
                    "kind": "registry",
                    "name": name,
                    "version": "v" + str(row["collection_version"]),
                    "digest": content_digest(content),
                    "content": content,
                }
                identity = self.store._identity(scope, "registry", name)
                previous = connection.execute(
                    select(self.store.resources.c.digest).where(
                        *[
                            self.store.resources.c[key] == value
                            for key, value in identity.items()
                        ],
                        self.store.resources.c.version == result["version"],
                    )
                ).scalar_one_or_none()
                if previous is not None and previous != result["digest"]:
                    raise Conflict("LEGACY_REGISTRY_CONTENT_CONFLICT")
                if previous is None:
                    connection.execute(
                        self.store.resources.insert().values(
                            **identity,
                            version=result["version"],
                            digest=result["digest"],
                            payload=canonical(result),
                        )
                    )
                connection.execute(
                    self.store._insert(self.store.heads)
                    .values(**identity, next_version=0)
                    .on_conflict_do_nothing()
                )
                connection.execute(
                    self.store.heads.update()
                    .where(
                        *[
                            self.store.heads.c[key] == value
                            for key, value in identity.items()
                        ],
                        self.store.heads.c.next_version <= row["collection_version"],
                    )
                    .values(next_version=row["collection_version"] + 1)
                )
                for alias in aliases.get(row["id"], []):
                    connection.execute(
                        self.store._insert(self.store.aliases)
                        .values(**identity, alias=alias, version=result["version"])
                        .on_conflict_do_update(
                            index_elements=[
                                self.store.aliases.c[key]
                                for key in (*identity, "alias")
                            ],
                            set_={"version": result["version"]},
                        )
                    )
        if "registry_events" in info["tables"]:
            for row in _table_rows(database, "registry_events"):
                request = {
                    "name": registry,
                    "action": "legacy_registry_event",
                    "source_id": source_id,
                    "original_event": encoded_row(row),
                }
                self.store._commit_receipt(
                    connection,
                    scope,
                    "legacy-registry-" + content_digest(request),
                    request,
                    {"imported": True},
                )

    def import_snapshot(self, snapshot, bindings, *, source_id):
        identifier(source_id)
        manifest = verify(snapshot)
        if set(bindings.get("databases", {})) != set(manifest["databases"]) or set(
            bindings.get("files", {})
        ) != set(manifest["files"]):
            raise ValueError("COMPLETE_LEGACY_BINDINGS_REQUIRED")
        scopes = [scoped(scope) for scope in bindings["files"].values()]
        for binding in bindings["databases"].values():
            scopes.extend(scoped(scope) for scope in binding.get("runs", {}).values())
            scopes.extend(scoped(scope) for scope in binding.get("names", {}).values())
            if "default_scope" in binding:
                scopes.append(scoped(binding["default_scope"]))
        unique = {canonical(scope): scope for scope in scopes}
        if (
            not unique
            or len(unique) > 1000
            or len({scope["workspace_id"] for scope in scopes}) != 1
        ):
            raise ValueError("INVALID_LEGACY_SCOPE_BINDINGS")
        external_source_id = source_id
        source_id = content_digest(
            {"workspace_id": scopes[0]["workspace_id"], "source_id": source_id}
        )
        for name in manifest["files"]:
            scope = scoped(bindings["files"][name])
            with (Path(snapshot) / "files" / name).open("rb") as stream:
                if manifest["files"][name]["size"] == 0:
                    self.repository.put_import_object(scope, b"", self.objects)
                while content := stream.read(MAX_OBJECT_BYTES):
                    self.repository.put_import_object(scope, content, self.objects)
        with self.repository.engine.begin() as connection:
            for scope in sorted(unique.values(), key=lambda scope: scope["run_id"]):
                self.store._lock(connection, scope)
            previous = (
                connection.execute(
                    select(self.store.imports).where(
                        self.store.imports.c.source_id == source_id
                    )
                )
                .mappings()
                .first()
            )
            if previous is not None:
                if previous["digest"] != manifest["digest"] or previous[
                    "bindings_digest"
                ] != content_digest(bindings):
                    raise Conflict("LEGACY_SOURCE_CONTENT_CONFLICT")
                return json.loads(previous["payload"])
            files = {}
            for name, info in manifest["files"].items():
                scope = scoped(bindings["files"][name])
                files[name] = {
                    "scope": scope,
                    "manifest": self._file(
                        connection, snapshot, source_id, name, info, scope
                    ),
                }
            count, events = 0, 0
            for database_name, database_info in manifest["databases"].items():
                binding = bindings["databases"][database_name]
                with closing(
                    _open_database(Path(snapshot) / "files" / database_name)
                ) as database:
                    for table in database_info["tables"]:
                        for index, row in enumerate(_table_rows(database, table)):
                            scope = self._row_scope(binding, row)
                            self.store._lock(connection, scope)
                            row_key = str(index)
                            connection.execute(
                                self.store.legacy_rows.insert().values(
                                    source_id=source_id,
                                    database=database_name,
                                    table_name=table,
                                    row_key=row_key,
                                    run_id=scope["run_id"],
                                    payload=canonical(encoded_row(row)),
                                )
                            )
                            events += self._project(
                                connection,
                                scope,
                                source_id,
                                manifest["digest"],
                                table,
                                row,
                                database_name + ":" + row_key,
                                files,
                            )
                            count += 1
                    self._artifacts(
                        connection, database, database_info, binding, source_id, files
                    )
            for database_name, database_info in manifest["databases"].items():
                with closing(
                    _open_database(Path(snapshot) / "files" / database_name)
                ) as database:
                    self._registry(
                        connection,
                        database_name,
                        database,
                        database_info,
                        bindings,
                        source_id,
                    )
            for name in files:
                if name.startswith("inbox/") and name.endswith(".jsonl"):
                    scope = files[name]["scope"]
                    with (Path(snapshot) / "files" / name).open() as stream:
                        for index, line in enumerate(stream):
                            row = json.loads(line)
                            if row.get("v") != 1 or row.get("kind") not in (
                                "metric",
                                "system_metric",
                                "alert",
                            ):
                                raise ValueError("UNKNOWN_LEGACY_FRAGMENT_FORMAT")
                            table = {
                                "metric": "metrics",
                                "system_metric": "system_metrics",
                                "alert": "alerts",
                            }[row["kind"]]
                            events += self._project(
                                connection,
                                scope,
                                source_id,
                                manifest["digest"],
                                table,
                                row,
                                name + ":" + str(index),
                                files,
                            )
            books = {}
            for name, file in files.items():
                if ".trackio/logbook/" in name and name.endswith(".md"):
                    key = canonical(file["scope"])
                    books.setdefault(key, {"scope": file["scope"], "pages": {}})[
                        "pages"
                    ][name.split(".trackio/logbook/", 1)[1]] = (
                        Path(snapshot) / "files" / name
                    ).read_text()
            for book in books.values():
                self.store._publish(
                    connection,
                    book["scope"],
                    "logbook",
                    "legacy-" + content_digest(source_id)[:16],
                    {"pages": book["pages"], "attachments": [], "source_id": source_id},
                    [],
                )
            verify(snapshot)
            receipt = {
                "committed": True,
                "format_version": 1,
                "source_id": external_source_id,
                "snapshot_digest": manifest["digest"],
                "bindings_digest": content_digest(bindings),
                "rows": count,
                "files": len(files),
                "events": events,
                "scopes": sorted(unique),
                "complete_raw_snapshot": True,
            }
            connection.execute(
                self.store.imports.insert().values(
                    source_id=source_id,
                    digest=manifest["digest"],
                    bindings_digest=content_digest(bindings),
                    payload=canonical(receipt),
                )
            )
            return receipt

    def read_file(self, scope, source_id, path):
        source_id = content_digest(
            {"workspace_id": scope["workspace_id"], "source_id": source_id}
        )
        with self.store.transaction() as connection:
            self.store._lock(connection, scope)
            payload = connection.execute(
                select(self.store.legacy_files.c.payload).where(
                    self.store.legacy_files.c.run_id == scope["run_id"],
                    self.store.legacy_files.c.source_id == source_id,
                    self.store.legacy_files.c.path == path,
                )
            ).scalar_one_or_none()
            if payload is None:
                raise PermissionError("LEGACY_FILE_SCOPE_MISMATCH")
            manifest = json.loads(payload)
            chunks = []
            for chunk in manifest["chunks"]:
                row = (
                    connection.execute(
                        select(self.repository.objects).where(
                            self.repository.objects.c.run_id == scope["run_id"],
                            self.repository.objects.c.digest == chunk["_object"],
                        )
                    )
                    .mappings()
                    .one()
                )
                chunks.append(
                    self.objects.get(row["object_key"], chunk["_object"], chunk["size"])
                )
            content = b"".join(chunks)
            if (
                len(content) != manifest["size"]
                or hashlib.sha256(content).hexdigest() != manifest["digest"]
            ):
                raise ValueError("LEGACY_FILE_INTEGRITY_FAILURE")
            return content

    def read_rows(self, scope, source_id, database, table, *, after=None, limit=500):
        source_id = content_digest(
            {"workspace_id": scope["workspace_id"], "source_id": source_id}
        )
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("INVALID_QUERY_LIMIT")
        with self.store.transaction() as connection:
            self.store._lock(connection, scope)
            rows = self.store.legacy_rows
            conditions = [
                rows.c.run_id == scope["run_id"],
                rows.c.source_id == source_id,
                rows.c.database == database,
                rows.c.table_name == table,
            ]
            if after is not None:
                conditions.append(rows.c.row_key > str(after))
            return [
                json.loads(payload)
                for payload in connection.execute(
                    select(rows.c.payload)
                    .where(*conditions)
                    .order_by(rows.c.row_key)
                    .limit(limit)
                ).scalars()
            ]

    def receipt(self, scope, source_id):
        source_id = content_digest(
            {"workspace_id": scope["workspace_id"], "source_id": source_id}
        )
        with self.store.transaction() as connection:
            self.store._lock(connection, scope)
            payload = connection.execute(
                select(self.store.imports.c.payload).where(
                    self.store.imports.c.source_id == source_id
                )
            ).scalar_one_or_none()
            if (
                payload is None
                or canonical(scoped(scope)) not in json.loads(payload)["scopes"]
            ):
                raise PermissionError("LEGACY_IMPORT_SCOPE_MISMATCH")
            result = json.loads(payload)
            result.pop("scopes")
            result.pop("events", None)
            for key, table in (
                ("rows", self.store.legacy_rows),
                ("files", self.store.legacy_files),
            ):
                result[key] = connection.execute(
                    select(func.count())
                    .select_from(table)
                    .where(
                        table.c.run_id == scope["run_id"],
                        table.c.source_id == source_id,
                    )
                ).scalar_one()
            result["scope"] = scoped(scope)
            return result
