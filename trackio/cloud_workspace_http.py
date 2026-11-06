"""Structured, scope checked resource and lifecycle HTTP operations."""

import hmac
import importlib
import json

from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from trackio.cloud import MAX_BODY, verify_token
from trackio.cloud_resource_ops import change, resources
from trackio.cloud_workspace import SCOPE_FIELDS, scoped


def install_routes(app, config, repository, objects):
    store = repository.workspace

    def authorization(request):
        raw = request.headers.get("authorization", "")
        if hmac.compare_digest(raw, "Bearer " + config.service_token):
            return None
        claims = verify_token(config.signing_key, raw.removeprefix("Bearer "))
        if claims.get("capability_id") is None:
            repository.authorize_legacy(claims)
        else:
            capability = repository.authorize_capability(claims["capability_id"])
            if any(
                claims.get(key) != capability.get(key)
                for key in (*SCOPE_FIELDS, "trial_id")
            ):
                raise PermissionError("CAPABILITY_SCOPE_MISMATCH")
        return claims

    def effective_scope(data, claims, *, collection=False):
        provided = data.get("scope", {})
        if claims is not None:
            if any(
                key not in SCOPE_FIELDS or value != claims[key]
                for key, value in provided.items()
            ):
                raise PermissionError("RUN_SCOPE_MISMATCH")
            return scoped(claims)
        if collection:
            return {key: provided[key] for key in SCOPE_FIELDS if key in provided}
        return scoped(provided)

    async def read_body(request):
        content = bytearray()
        async for chunk in request.stream():
            content.extend(chunk)
            if len(content) > MAX_BODY:
                raise ValueError("REQUEST_TOO_LARGE")
        value = json.loads(content)
        if not isinstance(value, dict):
            raise ValueError("INVALID_REQUEST")
        return value

    async def query(request):
        claims = authorization(request)
        data = await read_body(request)
        action = data["action"]
        if action == "list":
            result = store.list_runs(
                effective_scope(data, claims, collection=True),
                filters=data.get("filters"),
                after=data.get("after"),
                limit=data.get("limit", 100),
            )
        elif action == "compare":
            scopes = [scoped(scope) for scope in data["scopes"]]
            if claims is not None and any(scope != scoped(claims) for scope in scopes):
                raise PermissionError("RUN_SCOPE_MISMATCH")
            result = store.compare(
                scopes,
                metrics=data.get("metrics"),
                start=data.get("start"),
                end=data.get("end"),
                max_points=data.get("max_points", 2000),
                min_step=data.get("min_step"),
                max_step=data.get("max_step"),
            )
        elif action == "export":
            result = store.export(
                effective_scope(data, claims),
                max_records=data.get("max_records", 10000),
                max_bytes=data.get("max_bytes", 16 * 1024 * 1024),
            )
        elif action == "resources":
            result = {
                "resources": resources(
                    store,
                    effective_scope(data, claims),
                    kind=data["kind"],
                    prefix=data.get("prefix", ""),
                    limit=data.get("limit", 1000),
                )
            }
        elif action in ("legacy_rows", "legacy_receipt"):
            importer = importlib.import_module("trackio.cloud_import").LegacyImporter(
                repository, objects
            )
            scope = effective_scope(data, claims)
            if action == "legacy_rows":
                result = {
                    "rows": importer.read_rows(
                        scope,
                        data["source_id"],
                        data["database"],
                        data["table"],
                        after=data.get("after"),
                        limit=data.get("limit", 500),
                    )
                }
            else:
                result = importer.receipt(scope, data["source_id"])
        elif action == "resource":
            result = store.resolve(
                effective_scope(data, claims),
                data["kind"],
                data["name"],
                data.get("version", "latest"),
            )
        elif action == "lineage":
            result = {
                "links": store.lineage(
                    effective_scope(data, claims),
                    data["name"],
                    data.get("version", "latest"),
                )
            }
        elif action == "operation":
            scope = effective_scope(data, claims)
            with store.engine.begin() as connection:
                store._lock(connection, scope)
                row = (
                    connection.execute(
                        store.operations.select().where(
                            store.operations.c.run_id == scope["run_id"],
                            store.operations.c.operation_id == data["operation_id"],
                        )
                    )
                    .mappings()
                    .first()
                )
                result = {
                    "committed": row is not None,
                    "operation_id": data["operation_id"],
                    "result": json.loads(row["payload"])["result"] if row else None,
                }
        else:
            raise ValueError("INVALID_QUERY_ACTION")
        return JSONResponse(result)

    async def publish(request):
        claims = authorization(request)
        data = await read_body(request)
        scope = effective_scope(data, claims)
        result = change(store, scope, {"action": "publish", **data}, claims=claims)
        return JSONResponse(
            {"committed": True, "operation_id": data["operation_id"], "result": result}
        )

    async def mutate(request):
        if authorization(request) is not None:
            raise PermissionError("SERVICE_AUTH_REQUIRED")
        data = await read_body(request)
        action = data["action"]
        if action == "link":
            result = store.link(
                data["source"],
                data["target"],
                data["name"],
                data.get("version", "latest"),
                data["target_kind"],
                data["target_name"],
                operation_id=data["operation_id"],
            )
        elif action == "purge":
            result = store.purge(scoped(data["scope"]))
        elif action == "collect_objects":
            if objects is None:
                raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
            result = store.collect_objects(scoped(data["scope"]), objects)
        elif action == "collect_orphans":
            if objects is None:
                raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
            result = store.collect_orphans(
                scoped(data["scope"]),
                objects,
                retention_seconds=data.get("retention_seconds", 86400),
                limit=data.get("limit", 100),
                after=data.get("after"),
            )
        else:
            result = store.mutate(
                scoped(data["scope"]),
                action,
                data.get("values", {}),
                operation_id=data["operation_id"],
            )
        return JSONResponse(result)

    async def legacy_file(request):
        claims = authorization(request)
        if claims is not None:
            raise PermissionError("SERVICE_AUTH_REQUIRED")
        data = await read_body(request)
        scope = effective_scope(data, claims)
        if objects is None:
            raise ValueError("OBJECT_STORAGE_NOT_CONFIGURED")
        importer = importlib.import_module("trackio.cloud_import").LegacyImporter(
            repository, objects
        )
        content = importer.read_file(scope, data["source_id"], data["path"])
        return Response(
            content,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": "attachment",
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "sandbox; default-src 'none'",
            },
        )

    async def download(request):
        claims = authorization(request)
        scope = effective_scope({"scope": dict(request.query_params)}, claims)
        payload = repository.read_object(
            scope, request.path_params["digest"], objects, claims=claims
        )
        return Response(
            payload,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": 'attachment; filename="'
                + request.path_params["digest"]
                + '"',
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "sandbox; default-src 'none'",
            },
        )

    app.routes.extend(
        [
            Route("/v1/workspace/query", query, methods=["POST"]),
            Route("/v1/workspace/legacy-file", legacy_file, methods=["POST"]),
            Route("/v1/workspace/publish", publish, methods=["POST"]),
            Route("/v1/workspace/mutate", mutate, methods=["POST"]),
            Route("/v1/workspace/objects/{digest}", download, methods=["GET"]),
        ]
    )
