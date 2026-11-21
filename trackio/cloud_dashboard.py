"""Read-only bridge from platform resource scopes to native Dashboard series."""

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from math import floor
from pathlib import Path

from sqlalchemy import JSON, and_, cast, or_, select, type_coerce
from starlette.responses import FileResponse, Response

DIST = Path(__file__).parent / "frontend" / "dist"
MAX_SCOPES = 64
MAX_SERIES = 256
MAX_LOGS = 10000
MAX_BYTES = 512 * 1024
IDENTITY = ("workspace_id", "project_id", "job_id", "run_id")
CATEGORIES = {"get_logs_batch": "experiment", "get_system_logs_batch": "compute"}


ASSET_REF = re.compile(
    r'(?:src|href)="/(assets/[A-Za-z0-9_-][A-Za-z0-9_.-]*\.(?:js|css))"'
)


def _packaged_file(path):
    return path.resolve().is_relative_to(DIST.resolve()) and path.is_file()


def _asset_names():
    entry = DIST / "embed.html"
    if not _packaged_file(entry):
        return set()
    return set(ASSET_REF.findall(entry.read_text(encoding="utf-8")))


def available():
    names = _asset_names()
    return bool(names) and all(_packaged_file(DIST / name) for name in names)


def asset(path):
    if path == "index.html" and available():
        source = DIST / "embed.html"
        media_type = "text/html"
    elif path in _asset_names() and _packaged_file(DIST / path):
        source = DIST / path
        media_type = "text/javascript" if path.endswith(".js") else "text/css"
    else:
        return Response(status_code=404)
    return FileResponse(
        source, media_type=media_type, headers={"X-Content-Type-Options": "nosniff"}
    )


def _size(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def _scope_query(repository, scopes):
    if not isinstance(scopes, list) or not 1 <= len(scopes) <= MAX_SCOPES:
        raise ValueError("INVALID_SCOPES")
    records, runs = repository.records, repository.runs
    filters = []
    for scope in scopes:
        if not isinstance(scope, dict) or scope.keys() != {
            *IDENTITY,
            "trial_id",
            "start",
            "end",
        }:
            raise ValueError("INVALID_SCOPE")
        clauses = [
            runs.c[key] == repository.validate_identity(scope[key]) for key in IDENTITY
        ]
        if any(scope[key] != scopes[0][key] for key in ("workspace_id", "project_id")):
            raise ValueError("MIXED_PROJECTS")
        if scope["trial_id"] is not None:
            clauses.append(
                records.c.trial_id == repository.validate_identity(scope["trial_id"])
            )
        if (
            not all(repository.is_finite(scope[key]) for key in ("start", "end"))
            or scope["start"] > scope["end"]
        ):
            raise ValueError("INVALID_TIME_RANGE")
        filters.append(
            and_(*clauses, records.c.observed_at.between(scope["start"], scope["end"]))
        )
    payload = (cast if repository.engine.dialect.name == "postgresql" else type_coerce)(
        records.c.payload, JSON
    )
    query = (
        select()
        .select_from(records.join(runs, records.c.run_id == runs.c.run_id))
        .where(or_(*filters))
    )
    if hasattr(repository, "workspace"):
        state = repository.workspace.state
        query = query.where(
            ~select(state.c.run_id)
            .where(state.c.run_id == records.c.run_id, state.c.deleted == 1)
            .exists()
        )
    return query, payload


def _series(connection, statement, columns, project):
    runs = {}
    context = {"project": project, "readonly": True, "runs": [], "truncated": False}
    size = _size({"data": context})
    rows = connection.execute(
        statement.with_only_columns(*columns)
        .distinct()
        .order_by(*columns)
        .limit(MAX_SERIES + 1)
    )
    for run_id, trial_id, writer_id in rows:
        identity = [run_id, trial_id, writer_id]
        series_id = hashlib.sha256(
            json.dumps(identity, ensure_ascii=True).encode()
        ).hexdigest()
        name = " / ".join(part for part in identity if part is not None)
        metadata = dict(
            id=series_id,
            name=name,
            run_id=run_id,
            trial_id=trial_id,
            writer_id=writer_id,
        )
        size += _size(metadata) + 1
        if len(runs) == MAX_SERIES or size > MAX_BYTES:
            context["truncated"] = True
            break
        runs[series_id] = metadata
        context["runs"].append(metadata)
    return runs, context


def _selection(params, action, project, runs):
    keys = {"project", "runs"}
    if action == "get_logs_batch" and "scalar_only" in params:
        if params["scalar_only"] is not True:
            raise ValueError("SCALAR_ONLY_REQUIRED")
        keys.add("scalar_only")
    if params.keys() != keys or params["project"] != project:
        raise ValueError("INVALID_PARAMETERS")
    if not isinstance(params["runs"], list) or not 1 <= len(params["runs"]) <= 64:
        raise ValueError("INVALID_RUN_SELECTION")
    selected = {}
    for item in params["runs"]:
        if not isinstance(item, dict) or item.keys() != {"run_id", "run"}:
            raise ValueError("INVALID_RUN_SELECTION")
        key = item["run_id"]
        if key not in runs or key in selected or item["run"] != runs[key]["name"]:
            raise ValueError("INVALID_RUN_SELECTION")
        selected[key] = runs[key]
    return selected


NATIVE_FIELDS = {
    "project",
    "run",
    "run_id",
    "series_key",
    "data_type",
    "timestamp",
    "step",
    "time",
    "metrics",
    "x_axis",
}
RESERVED_GROUPS = {
    "charts",
    "__proto__",
    "constructor",
    "hasOwnProperty",
    "toString",
    "toLocaleString",
    "valueOf",
    "isPrototypeOf",
    "propertyIsEnumerable",
    "__defineGetter__",
    "__defineSetter__",
    "__lookupGetter__",
    "__lookupSetter__",
}


def _metric_field(name):
    if name in NATIVE_FIELDS:
        return "~" + name
    return "/".join(
        "~" + part if part.startswith("~") or part in RESERVED_GROUPS else part
        for part in name.split("/")
    )


def log_row(event):
    row = {_metric_field(key): value for key, value in event["metrics"].items()}
    row["step"] = event["step"]
    observed = Decimal(str(event["observed_at"]))
    seconds = floor(observed)
    try:
        utc = datetime.fromtimestamp(seconds, timezone.utc).isoformat(
            timespec="seconds"
        )
    except (OverflowError, OSError) as exc:
        raise ValueError("OBSERVATION_OUTSIDE_CALENDAR_RANGE") from exc
    with localcontext() as ctx:
        ctx.prec = max(ctx.prec, -observed.as_tuple().exponent + 1)
        fraction = format(observed - seconds, "f").partition(".")[2].rstrip("0")
    row["timestamp"] = utc.replace(
        "+00:00", ("." + fraction if fraction else "") + "+00:00"
    )
    return row


def _batch(connection, statement, records, payload, series_columns, selected, category):
    entries = []
    by_identity = {}
    filters = []
    for key, run in selected.items():
        identity = (run["run_id"], run["trial_id"], run["writer_id"])
        filters.append(
            and_(*(col == value for col, value in zip(series_columns, identity)))
        )
        entry = dict(run=run["name"], run_id=key, logs=[], truncated=False)
        by_identity[identity] = entry
        entries.append(entry)
    query = (
        statement.with_only_columns(records.c.run_id, records.c.payload)
        .where(payload["category"].as_string() == category, or_(*filters))
        .order_by(
            records.c.observed_at.desc(), records.c.run_id, records.c.event_id.desc()
        )
        .limit(MAX_LOGS + 1)
    )
    size = _size({"data": entries})
    with connection.execution_options(yield_per=128).execute(query) as rows:
        for index, (run_id, raw) in enumerate(rows):
            event = json.loads(raw)
            row = log_row(event)
            size += _size(row) + 1
            if index == MAX_LOGS or size > MAX_BYTES:
                for entry in entries:
                    entry["truncated"] = True
                break
            by_identity[run_id, event["trial_id"], event["writer_id"]]["logs"].append(
                row
            )
    for entry in entries:
        entry["logs"].reverse()
    return entries


def query(repository, request):
    if not isinstance(request, dict) or request.keys() != {
        "action",
        "params",
        "scopes",
    }:
        raise ValueError("INVALID_DASHBOARD_QUERY")
    action, params, scopes = request["action"], request["params"], request["scopes"]
    if action not in {"get_embed_context", *CATEGORIES} or not isinstance(params, dict):
        raise ValueError("INVALID_DASHBOARD_ACTION")
    statement, payload = _scope_query(repository, scopes)
    records = repository.records
    series_columns = (
        records.c.run_id,
        records.c.trial_id,
        payload["writer_id"].as_string(),
    )
    project = scopes[0]["project_id"]
    with repository.read_connection() as connection:
        runs, context = _series(connection, statement, series_columns, project)
        if action == "get_embed_context":
            if params:
                raise ValueError("INVALID_PARAMETERS")
            return {"data": context}
        selected = _selection(params, action, project, runs)
        data = _batch(
            connection,
            statement,
            records,
            payload,
            series_columns,
            selected,
            CATEGORIES[action],
        )
        return {"data": data}
