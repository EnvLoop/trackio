"""Native registry handles backed by the scoped cloud resource catalog."""

import json
import time
import uuid
from datetime import datetime, timezone

from trackio.cloud_artifacts import CloudArtifact
from trackio.registry import Collection


class CloudRegistryArtifact(CloudArtifact):
    def __init__(self, run, artifact, name, operation_id, source):
        self.source_artifact = artifact
        self.registry_name = name
        self.registry_operation = operation_id
        self.source = source
        super().__init__(run, artifact.name, artifact.type, result=artifact.result)
        self.wait()

    @property
    def is_link(self):
        return True

    def wait(self, timeout=None):
        deadline = time.monotonic() + (10 if timeout is None else max(0, timeout))
        while True:
            self.cloud_run.flush()
            receipt = self.cloud_run._workspace_query(
                "operation", operation_id=self.registry_operation
            )
            if receipt.get("committed"):
                self.registry_result = receipt["result"]
                self._version = int(self.registry_result["version"][1:])
                return self
            if time.monotonic() >= deadline:
                raise TimeoutError("REGISTRY_LINK_NOT_COMMITTED")
            time.sleep(0.05)

    def __getitem__(self, name):
        return {
            "source": self.source,
            "operation_id": self.registry_operation,
            "name": self.registry_name,
        }[name]

    def download(self, root=None):
        return self.source_artifact.download(root)

    def link(self, target_path, aliases=None, bucket_id=None):
        return self.source_artifact.link(
            target_path, aliases=aliases, bucket_id=bucket_id
        )

    def unlink(self):
        operation = {
            "action": "unlink",
            "kind": "registry",
            "name": self.registry_name,
            "version": self.version,
            "operation_id": uuid.uuid4().hex,
        }
        self.cloud_run._queue_workspace(operation, {})
        self.cloud_run.flush()
        receipt = self.cloud_run._workspace_query(
            "operation", operation_id=operation["operation_id"]
        )
        if not receipt.get("committed"):
            raise RuntimeError("REGISTRY_UNLINK_NOT_COMMITTED")


class CloudRegistry:
    def __init__(self, api, name):
        if (
            not isinstance(name, str)
            or not name
            or any(
                character
                not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for character in name
            )
        ):
            raise ValueError("INVALID_REGISTRY_NAME")
        self.api, self.name = api, name
        self.bucket_id = None

    @property
    def description(self):
        return self.api.query("resource", kind="registry_metadata", name=self.name)[
            "content"
        ].get("description")

    def create_collection(self, name, artifact_type, description=None):
        key = self.name + "." + name
        existing = self.api.query("resources", kind="collection", prefix=key)[
            "resources"
        ]
        exact = [value for value in existing if value["name"] == key]
        if exact and any(
            value["content"]["artifact_type"] != artifact_type for value in exact
        ):
            raise ValueError("REGISTRY_ARTIFACT_TYPE_CONFLICT")
        if not exact or description is not None:
            self.api.publish(
                "collection",
                key,
                {
                    "artifact_type": artifact_type,
                    "description": description,
                    "created_at": exact[0]["content"]["created_at"]
                    if exact
                    else datetime.now(timezone.utc).isoformat(),
                },
            )
        return self.collection(name)

    def collection(self, name):
        key = self.name + "." + name
        metadata = self.api.query("resources", kind="collection", prefix=key)[
            "resources"
        ]
        metadata = [
            value
            for value in metadata
            if value["name"] == key and "latest" in value["aliases"]
        ]
        if not metadata:
            return None
        metadata = metadata[0]["content"]
        links = self.api.query("resources", kind="registry", prefix=key)["resources"]
        links = [value for value in links if value["name"] == key]
        values = [
            {
                "collection_version": int(value["version"][1:]),
                "source_project": value["content"]["source"]["scope"]["project_id"],
                "source_artifact": value["content"]["source"]["name"],
                "source_version": int(value["content"]["source"]["version"][1:]),
                "aliases": value["aliases"],
            }
            for value in links
        ]
        values.sort(key=lambda value: value["collection_version"], reverse=True)
        return Collection(
            name=name,
            type=metadata["artifact_type"],
            description=metadata.get("description"),
            created_at=metadata["created_at"],
            num_links=len(values),
            latest_version=values[0]["collection_version"] if values else None,
            links=values,
        )

    def collections(self):
        values = self.api.query("resources", kind="collection", prefix=self.name + ".")[
            "resources"
        ]
        return [
            self.collection(value["name"][len(self.name) + 1 :])
            for value in values
            if "latest" in value["aliases"]
        ]

    def events(self):
        snapshot = self.api.query("export")
        events = []
        for value in snapshot["operations"]:
            operation = json.loads(value["payload"])
            request = operation["request"]
            name = request.get("name", "")
            if name == self.name or name.startswith(self.name + "."):
                events.append(
                    {
                        "id": value["operation_id"],
                        "ts": operation["timestamp"],
                        "kind": request.get("action", "publish"),
                        "payload": {"request": request, "result": operation["result"]},
                    }
                )
        return sorted(events, key=lambda value: (value["ts"], value["id"]))
