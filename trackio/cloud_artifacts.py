"""Native experiment attachments over the acknowledged, bounded cloud spool."""

import hashlib
import importlib
import json
import mimetypes
import os
import time
import uuid
from pathlib import Path

import httpx2

from trackio.artifact import Artifact
from trackio.cloud_protocol import content_digest
from trackio.cloud_snapshot import relative_path


class CloudArtifact(Artifact):
    def __init__(self, run, name, artifact_type, *, operation_id=None, result=None):
        super().__init__(name, artifact_type)
        self.cloud_run = run
        self.operation_id = operation_id
        self.result = None
        if result is not None:
            self._accept(result)

    def _accept(self, result):
        if (
            result["scope"]["run_id"] != self.cloud_run.run_id
            or result["kind"] != "artifact"
            or result["name"] != self.name
            or content_digest(result["content"]) != result["digest"]
        ):
            raise RuntimeError("INVALID_ARTIFACT_ACK")
        content = result["content"]
        manifest = [
            {
                "path": entry["path"],
                "digest": entry.get("_object", entry.get("digest")),
                "size": entry["size"],
                **({"ref": entry["ref"]} if "ref" in entry else {}),
            }
            for entry in content["manifest"]
        ]
        self._hydrate_from_db(
            project=result["scope"]["project_id"],
            version=int(result["version"][1:]),
            aliases=[],
            manifest=manifest,
            manifest_digest=result["digest"],
            size_bytes=sum(entry["size"] for entry in manifest),
            description=content.get("description"),
            metadata=content.get("metadata", {}),
        )
        self.result = result
        self._aliases = tuple(result.get("aliases", []))
        if getattr(self, "origin", None) is not None:
            self.origin._hydrate_from_db(
                project=self.project,
                version=self._version,
                aliases=list(self.aliases),
                manifest=self.manifest,
                manifest_digest=self.digest,
                size_bytes=self.size,
                description=self.description,
                metadata=self.metadata,
            )

    def wait(self, timeout=None):
        deadline = time.monotonic() + (10 if timeout is None else max(0, timeout))
        while self.result is None:
            self.cloud_run.flush()
            response = self.cloud_run._workspace_query(
                "operation", operation_id=self.operation_id
            )
            if (
                response.get("committed") is True
                and response.get("operation_id") == self.operation_id
            ):
                self._accept(response["result"])
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("ARTIFACT_NOT_COMMITTED")
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        return self

    def download(self, root=None):
        self.wait()
        if any("ref" in entry for entry in self.manifest):
            raise ValueError("EXTERNAL_REFERENCES_REQUIRE_EXPLICIT_ASSET_RESOLUTION")
        root = (
            Path(root)
            if root is not None
            else Path.cwd()
            / ".trackio"
            / "artifact-downloads"
            / self.cloud_run.run_id
            / self.name
            / self.version
        )
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if root.is_symlink():
            raise ValueError("ARTIFACT_SYMLINK_REJECTED")
        for entry in self.manifest:
            relative = relative_path(entry["path"])
            target = root.joinpath(*relative.parts)
            parent = root
            for part in relative.parts[:-1]:
                parent = parent / part
                if parent.is_symlink():
                    raise ValueError("ARTIFACT_SYMLINK_REJECTED")
                parent.mkdir(exist_ok=True, mode=0o700)
            response = self.cloud_run._request(
                "get",
                self.cloud_run.server_url + "/v1/workspace/objects/" + entry["digest"],
            )
            response.raise_for_status()
            content = response.content
            if (
                len(content) != entry["size"]
                or hashlib.sha256(content).hexdigest() != entry["digest"]
            ):
                raise RuntimeError("ARTIFACT_CONTENT_INTEGRITY_FAILURE")
            temporary = target.with_name(uuid.uuid4().hex + ".pending")
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        return str(root)

    def link(self, target_path, aliases=None, bucket_id=None):
        if bucket_id is not None:
            raise ValueError("MANAGED_HF_DESTINATIONS_DISABLED")
        self.wait()
        return self.cloud_run.link_artifact(self, target_path, aliases=aliases)

    def unlink(self):
        raise ValueError("USE_SCOPED_REGISTRY_UNLINK")


class WorkspaceRunMixin:
    def _workspace_query(self, action, **values):
        response = self._request(
            "post",
            self.server_url + "/v1/workspace/query",
            json={"action": action, **values},
        )
        response.raise_for_status()
        return response.json()

    def _queue_workspace(self, operation, uploads):
        operation_id = operation["operation_id"]
        if len(json.dumps(operation).encode()) > 512 * 1024:
            raise BufferError("RESOURCE_OPERATION_TOO_LARGE")
        with self.lock:
            if self.closed:
                raise RuntimeError("RUN_CLOSED")
            pending = {
                "record": {
                    "kind": "workspace",
                    "record_id": operation_id,
                    "payload": operation,
                },
                "objects": list(uploads.values()),
            }
            self._write(
                self.directory / (str(time.time_ns()) + "-" + operation_id + ".json"),
                pending,
            )

    def _flush_record(self, pending):
        if pending["record"]["kind"] != "workspace":
            return super()._flush_record(pending)
        for entry in pending["objects"]:
            content = (self.directory / (entry["digest"] + ".blob")).read_bytes()
            if (
                len(content) != entry["size"]
                or hashlib.sha256(content).hexdigest() != entry["digest"]
            ):
                raise RuntimeError("METRICS_OBJECT_INTEGRITY_FAILURE")
            response = self._request(
                "put",
                self.endpoint + "/objects/" + entry["digest"],
                content=content,
                headers={"Content-Type": entry["content_type"]},
            )
            response.raise_for_status()
            if (
                response.json().get("committed") is not True
                or response.json().get("digest") != entry["digest"]
            ):
                raise RuntimeError("INVALID_OBJECT_ACK")
        operation = pending["record"]["payload"]
        response = self._request(
            "post", self.server_url + "/v1/workspace/publish", json=operation
        )
        response.raise_for_status()
        result = response.json()
        if (
            result.get("committed") is not True
            or result.get("operation_id") != operation["operation_id"]
            or (
                operation.get("action", "publish") == "publish"
                and result.get("result", {}).get("digest")
                != content_digest(operation["content"])
            )
        ):
            raise RuntimeError("INVALID_RESOURCE_ACK")

    def log_artifact(
        self, artifact_or_path, name=None, type=None, aliases=None, overwrite=False
    ):
        if overwrite:
            raise ValueError("SCOPED_ARTIFACT_RETENTION_REQUIRES_EXPLICIT_OPERATION")
        if isinstance(artifact_or_path, Artifact):
            if name is not None or type is not None:
                raise ValueError("ARTIFACT_NAME_AND_TYPE_ALREADY_SET")
            artifact = artifact_or_path
        else:
            path = Path(artifact_or_path)
            artifact = Artifact(
                name or ("run-" + self.run_id + "-" + path.name), type or "unspecified"
            )
            if path.is_dir():
                artifact.add_dir(path)
            else:
                artifact.add_file(path)
        uploads, entries = {}, []
        with self.lock:
            try:
                if artifact._pending_references:
                    raise ValueError(
                        "CLOUD_REFERENCE_IMPORT_REQUIRES_EXPLICIT_ASSET_BINDING"
                    )
                for path, logical in artifact._pending_files:
                    relative_path(logical)
                    if path.stat().st_size > min(self.max_bytes, 32 * 1024 * 1024):
                        raise BufferError("METRICS_OBJECT_TOO_LARGE")
                    content = path.read_bytes()
                    staged = self._stage_bytes(
                        content,
                        mimetypes.guess_type(logical)[0] or "application/octet-stream",
                        uploads,
                    )
                    entries.append({"path": logical, **staged, "size": len(content)})
                operation_id = uuid.uuid4().hex
                content = {
                    "artifact_type": artifact.type,
                    "description": artifact.description,
                    "metadata": artifact.metadata,
                    "manifest": entries,
                }
                self._queue_workspace(
                    {
                        "operation_id": operation_id,
                        "kind": "artifact",
                        "name": artifact.name,
                        "content": content,
                        "aliases": aliases or [],
                    },
                    uploads,
                )
            except Exception:
                self._collect_spool_objects()
                raise
        result = CloudArtifact(
            self, artifact.name, artifact.type, operation_id=operation_id
        )
        result.origin = artifact
        artifact._cloud_artifact = result
        self._send()
        try:
            result.wait(timeout=0)
        except (httpx2.HTTPError, OSError, TimeoutError):
            pass
        return result

    def use_artifact(self, artifact_or_name, type=None):
        expected_digest = None
        if isinstance(artifact_or_name, CloudArtifact):
            artifact = artifact_or_name.wait()
            name, version = artifact.name, artifact.version
        else:
            name, separator, version = str(artifact_or_name).partition(":")
            if "/" in name:
                if not name.startswith("registry-"):
                    raise ValueError("CROSS_RUN_ARTIFACT_REQUIRES_EXPLICIT_SCOPES")
                linked = self._workspace_query(
                    "resource",
                    kind="registry",
                    name=name.removeprefix("registry-").replace("/", "."),
                    version=version if separator else "latest",
                )
                source = linked["content"]["source"]
                if (
                    linked.get("resolved_source_scope", source["scope"])
                    != linked["scope"]
                ):
                    raise PermissionError("CROSS_RUN_ARTIFACT_REQUIRES_EXPLICIT_SCOPES")
                name, version, separator = source["name"], source["version"], ":"
                expected_digest = source["digest"]
            version = version if separator else "latest"
        result = self._workspace_query(
            "resource", kind="artifact", name=name, version=version
        )
        if expected_digest is not None and result["digest"] != expected_digest:
            raise ValueError("REGISTRY_SOURCE_DIGEST_MISMATCH")
        if type is not None and result["content"]["artifact_type"] != type:
            raise ValueError("ARTIFACT_TYPE_MISMATCH")
        self._queue_workspace(
            {
                "action": "use",
                "operation_id": uuid.uuid4().hex,
                "name": name,
                "version": result["version"],
            },
            {},
        )
        self._send()
        return CloudArtifact(
            self, name, result["content"]["artifact_type"], result=result
        )

    def link_artifact(self, artifact, target_path, aliases=None):
        artifact = getattr(artifact, "_cloud_artifact", artifact)
        artifact = artifact.wait()
        if artifact.cloud_run is not self:
            raise ValueError("CROSS_RUN_ARTIFACT_REQUIRES_EXPLICIT_SCOPES")
        if target_path.startswith("registry-"):
            registry_name, separator, collection_name = target_path.removeprefix(
                "registry-"
            ).partition("/")
            if not separator:
                raise ValueError("INVALID_REGISTRY_TARGET")
            api = importlib.import_module("trackio.cloud_api").CloudApi(run=self)
            api.registry(registry_name).create_collection(
                collection_name, artifact.type
            )
        name = target_path.removeprefix("registry-").replace("/", ".")
        operation_id = uuid.uuid4().hex
        source = {
            "scope": artifact.result["scope"],
            "name": artifact.name,
            "version": artifact.version,
            "digest": artifact.digest,
        }
        self._queue_workspace(
            {
                "operation_id": operation_id,
                "kind": "registry",
                "name": name,
                "content": {
                    "source": source,
                    **(
                        {"collection": name}
                        if target_path.startswith("registry-")
                        else {}
                    ),
                },
                "aliases": aliases or [],
            },
            {},
        )
        self._send()
        return importlib.import_module("trackio.cloud_registry").CloudRegistryArtifact(
            self, artifact, name, operation_id, source
        )

    def log_logbook(self, name, pages, attachments=None, aliases=None):
        uploads, entries = {}, []
        with self.lock:
            try:
                for logical, path in (attachments or {}).items():
                    relative_path(logical)
                    path = Path(path)
                    if path.stat().st_size > min(self.max_bytes, 32 * 1024 * 1024):
                        raise BufferError("METRICS_OBJECT_TOO_LARGE")
                    content = path.read_bytes()
                    entries.append(
                        {
                            "path": logical,
                            **self._stage_bytes(
                                content, "application/octet-stream", uploads
                            ),
                            "size": len(content),
                        }
                    )
                operation_id = uuid.uuid4().hex
                self._queue_workspace(
                    {
                        "operation_id": operation_id,
                        "kind": "logbook",
                        "name": name,
                        "content": {"pages": pages, "attachments": entries},
                        "aliases": aliases or [],
                    },
                    uploads,
                )
            except Exception:
                self._collect_spool_objects()
                raise
        self._send()
        return operation_id

    def get_logbook(self, name, version="latest"):
        return self._workspace_query(
            "resource", kind="logbook", name=name, version=version
        )
