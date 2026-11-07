"""Structured cloud API. No upstream SQLite, SQL passthrough, or HF sync."""

import importlib
import json
import uuid
from urllib.parse import urlsplit

import httpx2


class CloudApiRun:
    def __init__(self, api, value):
        self.api, self.value = api, value
        self.id, self.name = value["run_id"], value["name"]
        self.project, self.config = value["project_id"], value["config"]
        self.scope = {
            key: value[key]
            for key in ("workspace_id", "project_id", "job_id", "run_id")
        }

    def rename(self, new_name):
        result = self.api.mutate(self.scope, "rename", {"name": new_name})
        self.name = result["name"]
        return self

    def move(self, new_project):
        self.api.mutate(
            self.scope,
            "move",
            {"destination": {**self.scope, "project_id": new_project}},
        )
        self.scope["project_id"], self.project = new_project, new_project
        return True

    def delete(self):
        self.api.mutate(self.scope, "delete", {})
        return True

    def export(self, path=None, *, format="json"):
        snapshot = importlib.import_module("trackio.cloud_export").verify(
            self.api.query("export", scope=self.scope)
        )
        if path is None:
            if format != "json":
                raise ValueError("EXPORT_PATH_REQUIRED")
            return snapshot
        return importlib.import_module("trackio.cloud_export").write(
            snapshot, path, format=format
        )

    def retention(self, expires_at):
        return self.api.mutate(self.scope, "retention", {"expires_at": expires_at})

    def alerts(self, level=None, since=None):
        result = self.export()
        alerts = [
            json.loads(row["payload"])
            for row in result["records"]
            if row["kind"] == "alert"
        ]
        if level is not None:
            alerts = [
                alert for alert in alerts if alert["payload"].get("level") == level
            ]
        if since is not None:
            alerts = [alert for alert in alerts if alert["observed_at"] >= float(since)]
        return alerts


class CloudApi:
    def __init__(
        self, *, run=None, server_url=None, token=None, scope=None, transport=None
    ):
        self.run = run
        if run is not None:
            self.server_url, self.token, self.transport = (
                run.server_url,
                run.token,
                run.transport,
            )
            self.owns_transport = False
        else:
            address = urlsplit(server_url or "")
            if (
                not address.hostname
                or address.username
                or address.password
                or address.query
                or address.fragment
                or address.scheme not in ("http", "https")
                or (
                    address.scheme == "http"
                    and address.hostname not in ("127.0.0.1", "localhost", "::1")
                )
            ):
                raise ValueError("PRIVATE_CLOUD_ADDRESS_REQUIRED")
            if not token or scope is None:
                raise ValueError("EXPLICIT_CLOUD_SCOPE_AND_TOKEN_REQUIRED")
            self.server_url, self.token = server_url.rstrip("/"), token
            self.transport = transport or httpx2.Client(
                timeout=10, follow_redirects=False, trust_env=False
            )
            self.owns_transport = transport is None
        self.scope = scope

    def _request(self, path, value):
        if self.run is not None:
            response = self.run._request("post", self.server_url + path, json=value)
        else:
            response = self.transport.post(
                self.server_url + path,
                json=value,
                headers={"Authorization": "Bearer " + self.token},
            )
        if response.status_code in (401, 403):
            raise PermissionError("CLOUD_SCOPE_OR_OPERATION_DENIED")
        response.raise_for_status()
        return response.json()

    def query(self, action, **values):
        return self._request(
            "/v1/workspace/query",
            {
                "action": action,
                **({"scope": self.scope} if self.scope else {}),
                **values,
            },
        )

    def runs(self, project, *, filters=None, limit=100):
        if self.scope is not None and self.scope["project_id"] != project:
            raise PermissionError("PROJECT_SCOPE_MISMATCH")
        after = None
        while True:
            response = self.query("list", filters=filters, after=after, limit=limit)
            for value in response["runs"]:
                if value["project_id"] != project:
                    raise PermissionError("PROJECT_SCOPE_MISMATCH")
                yield CloudApiRun(self, value)
            after = response["next"]
            if after is None:
                break

    def mutate(self, scope, action, values, *, operation_id=None):
        request = {"action": action, "scope": scope, "values": values}
        return self._request(
            "/v1/workspace/mutate",
            {**request, "operation_id": operation_id or uuid.uuid4().hex},
        )

    def compare(self, scopes, **filters):
        return self.query("compare", scopes=scopes, **filters)

    def logbook(self, name, version="latest"):
        return self.query("resource", kind="logbook", name=name, version=version)

    def legacy_rows(self, source_id, database, table, **pagination):
        return self.query(
            "legacy_rows",
            source_id=source_id,
            database=database,
            table=table,
            **pagination,
        )

    def legacy_receipt(self, source_id):
        return self.query("legacy_receipt", source_id=source_id)

    def publish(self, kind, name, content, *, aliases=None):
        operation = {
            "action": "publish",
            "operation_id": uuid.uuid4().hex,
            "kind": kind,
            "name": name,
            "content": content,
            "aliases": aliases or [],
        }
        if self.run is not None:
            self.run._queue_workspace(operation, {})
            self.run.flush()
            receipt = self.query("operation", operation_id=operation["operation_id"])
            if not receipt.get("committed"):
                raise RuntimeError("RESOURCE_NOT_COMMITTED")
            return receipt["result"]
        return self._request(
            "/v1/workspace/publish", {"scope": self.scope, **operation}
        )["result"]

    def create_registry(self, name, description=None, bucket_id=None):
        if bucket_id is not None:
            raise ValueError("MANAGED_HF_DESTINATIONS_DISABLED")
        registry = importlib.import_module("trackio.cloud_registry").CloudRegistry(
            self, name
        )
        self.publish("registry_metadata", name, {"description": description})
        return registry

    def registry(self, name, bucket_id=None):
        if bucket_id is not None:
            raise ValueError("MANAGED_HF_DESTINATIONS_DISABLED")
        registry = importlib.import_module("trackio.cloud_registry").CloudRegistry(
            self, name
        )
        self.query("resource", kind="registry_metadata", name=name)
        return registry

    def registries(self):
        return [
            importlib.import_module("trackio.cloud_registry").CloudRegistry(
                self, value["name"]
            )
            for value in self.query("resources", kind="registry_metadata")["resources"]
            if "latest" in value["aliases"]
        ]

    def alerts(self, project, run=None, level=None, since=None):
        return [
            alert
            for value in self.runs(project)
            if run is None or run in (value.id, value.name)
            for alert in value.alerts(level, since)
        ]

    def close(self):
        if self.owns_transport:
            self.transport.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
