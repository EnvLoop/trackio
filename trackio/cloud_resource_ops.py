"""Scoped resource catalog operations, fenced by the owning run lock."""

import json
import time

from sqlalchemy import select

from trackio.cloud_protocol import canonical


def resources(store, scope, *, kind, prefix="", limit=1000):
    if (
        kind
        not in ("artifact", "registry", "registry_metadata", "collection", "logbook")
        or not isinstance(prefix, str)
        or type(limit) is not int
        or not 1 <= limit <= 1000
    ):
        raise ValueError("INVALID_RESOURCE_QUERY")
    with store.transaction() as connection:
        store._lock(connection, scope)
        rows = (
            connection.execute(
                select(store.resources.c.payload)
                .where(
                    store.resources.c.run_id == scope["run_id"],
                    store.resources.c.kind == kind,
                )
                .order_by(store.resources.c.name, store.resources.c.version)
                .limit(10001)
            )
            .scalars()
            .all()
        )
        if len(rows) > 10000:
            raise ValueError("RESOURCE_SCAN_LIMIT")
        result = [
            json.loads(value)
            for value in rows
            if json.loads(value)["name"].startswith(prefix)
        ]
        if len(result) > limit:
            raise ValueError("RESOURCE_QUERY_LIMIT")
        for resource in result:
            resource["scope"] = dict(scope)
            resource["aliases"] = (
                connection.execute(
                    select(store.aliases.c.alias)
                    .where(
                        store.aliases.c.run_id == scope["run_id"],
                        store.aliases.c.kind == kind,
                        store.aliases.c.name == resource["name"],
                        store.aliases.c.version == resource["version"],
                    )
                    .order_by(store.aliases.c.alias)
                )
                .scalars()
                .all()
            )
        return result


def change(store, scope, data, *, claims=None):
    action, operation_id = data["action"], data["operation_id"]
    request = {"scope": scope, **data}
    with store.transaction() as connection:
        if claims is None:
            store._lock(connection, scope)
        else:
            store._guard(connection, claims)
        previous = store._receipt(connection, scope, operation_id, request)
        if previous is not None:
            return previous
        if action == "publish":
            kind, name, content = data["kind"], data["name"], data["content"]
            if kind == "registry" and content["source"]["scope"] != scope:
                raise PermissionError("CROSS_RUN_LINK_REQUIRES_SERVICE_AUTH")
            result = store._publish(
                connection, scope, kind, name, content, data.get("aliases", [])
            )
        elif action == "use":
            result = store._resolve(
                connection,
                scope,
                "artifact",
                data["name"],
                data.get("version", "latest"),
            )
            link = {
                "source": result,
                "target": scope,
                "kind": "use",
                "name": data["name"],
                "timestamp": time.time(),
            }
            connection.execute(
                store.links.insert().values(
                    run_id=scope["run_id"],
                    operation_id=operation_id,
                    source_run=scope["run_id"],
                    payload=canonical(link),
                )
            )
        elif action in ("unlink", "aliases"):
            kind = data["kind"]
            if action == "unlink" and kind != "registry":
                raise ValueError("UNLINK_REQUIRES_REGISTRY_LINK")
            result = store._resolve(
                connection, scope, kind, data["name"], data["version"]
            )
            if action == "unlink":
                connection.execute(
                    store.resources.delete().where(
                        store.resources.c.run_id == scope["run_id"],
                        store.resources.c.kind == kind,
                        store.resources.c.name == data["name"],
                        store.resources.c.version == result["version"],
                    )
                )
                connection.execute(
                    store.aliases.delete().where(
                        store.aliases.c.run_id == scope["run_id"],
                        store.aliases.c.kind == kind,
                        store.aliases.c.name == data["name"],
                        store.aliases.c.version == result["version"],
                    )
                )
                remaining = (
                    connection.execute(
                        select(store.resources.c.version).where(
                            store.resources.c.run_id == scope["run_id"],
                            store.resources.c.kind == kind,
                            store.resources.c.name == data["name"],
                        )
                    )
                    .scalars()
                    .all()
                )
                if remaining:
                    latest = max(remaining, key=lambda value: int(value[1:]))
                    connection.execute(
                        store._insert(store.aliases)
                        .values(
                            run_id=scope["run_id"],
                            kind=kind,
                            name=data["name"],
                            alias="latest",
                            version=latest,
                        )
                        .on_conflict_do_update(
                            index_elements=[
                                store.aliases.c.run_id,
                                store.aliases.c.kind,
                                store.aliases.c.name,
                                store.aliases.c.alias,
                            ],
                            set_={"version": latest},
                        )
                    )
                result = {"unlinked": True, "version": result["version"]}
            else:
                store._publish(
                    connection,
                    scope,
                    kind,
                    data["name"],
                    result["content"],
                    data["aliases"],
                )
                result = {"aliases": data["aliases"], "version": result["version"]}
        else:
            raise ValueError("INVALID_RESOURCE_ACTION")
        return store._commit_receipt(connection, scope, operation_id, request, result)
