"""Scoped scalar selection, bounded chart samples and signed history pages."""

import base64
import hashlib
import hmac
import json
import math

from sqlalchemy import JSON, Integer, case, cast, func, or_, select, tuple_, type_coerce

MAX_QUERY_BYTES = 512 * 1024


def query_options(parameters):
    allowed = {
        "workspace_id",
        "project_id",
        "job_id",
        "trial_id",
        "start",
        "end",
        "category",
        "metric",
        "mode",
        "max_points",
        "limit",
        "cursor",
    }
    if set(parameters) - allowed or any(
        len(parameters.getlist(key)) != 1 for key in parameters if key != "metric"
    ):
        raise ValueError("INVALID_METRICS_QUERY")
    category = parameters.get("category")
    mode = parameters.get("mode", "sample")
    metrics = sorted(set(parameters.getlist("metric")))
    if category not in {"experiment", "compute"} or mode not in {"sample", "raw"}:
        raise ValueError("INVALID_METRICS_QUERY")
    if len(metrics) > 32 or any(
        not key or len(key) > 256 or any(ord(char) < 32 for char in key)
        for key in metrics
    ):
        raise ValueError("INVALID_METRICS_QUERY")
    start, end = float(parameters["start"]), float(parameters["end"])
    if not all(math.isfinite(value) for value in (start, end)) or start > end:
        raise ValueError("INVALID_TIME_RANGE")
    maximum, limit = (
        int(parameters.get("max_points", "500")),
        int(parameters.get("limit", "100")),
    )
    if not 2 <= maximum <= 2000 or not 1 <= limit <= 500:
        raise ValueError("INVALID_METRICS_QUERY")
    if mode == "sample" and ("cursor" in parameters or "limit" in parameters):
        raise ValueError("INVALID_METRICS_QUERY")
    if mode == "raw" and "max_points" in parameters:
        raise ValueError("INVALID_METRICS_QUERY")
    return dict(
        category=category,
        mode=mode,
        metrics=metrics,
        start=start,
        end=end,
        max_points=maximum,
        limit=limit,
        cursor=parameters.get("cursor"),
    )


def cursor_value(secret, fingerprint, position=None, token=None):
    if token is None:
        payload = json.dumps([fingerprint, position], separators=(",", ":")).encode()
        encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
        signature = hmac.new(
            secret.encode(), encoded.encode(), hashlib.sha256
        ).hexdigest()
        return encoded + "." + signature
    try:
        if len(token) > 4096:
            raise ValueError
        encoded, signature = token.split(".")
        expected = hmac.new(
            secret.encode(), encoded.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError
        bound, position = json.loads(
            base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        )
        if bound != fingerprint or not isinstance(position, list) or len(position) != 3:
            raise ValueError
        if type(position[0]) not in (int, float) or not math.isfinite(position[0]):
            raise ValueError
        if any(
            not isinstance(value, str) or not 1 <= len(value) <= 256
            for value in position[1:]
        ):
            raise ValueError
        return position
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("INVALID_METRICS_CURSOR") from None


def query_metrics(repository, secret, scope, options):
    records, runs = repository.records, repository.runs
    payload = (
        cast(records.c.payload, JSON)
        if repository.engine.dialect.name == "postgresql"
        else type_coerce(records.c.payload, JSON)
    )
    statement = (
        select(
            records.c.payload,
            records.c.run_id,
            records.c.event_id,
            records.c.observed_at,
        )
        .join(runs, records.c.run_id == runs.c.run_id)
        .where(
            runs.c.workspace_id == scope["workspace_id"],
            runs.c.project_id == scope["project_id"],
            runs.c.job_id == scope["job_id"],
            records.c.observed_at >= options["start"],
            records.c.observed_at <= options["end"],
            payload["category"].as_string() == options["category"],
        )
    )
    if scope.get("trial_id"):
        statement = statement.where(records.c.trial_id == scope["trial_id"])
    if options["metrics"]:
        statement = statement.where(
            or_(
                *(
                    payload["metrics"][key].as_float().is_not(None)
                    for key in options["metrics"]
                )
            )
        )
    if hasattr(repository, "workspace"):
        statement = statement.where(
            ~select(repository.workspace.state.c.run_id)
            .where(
                repository.workspace.state.c.run_id == records.c.run_id,
                repository.workspace.state.c.deleted == 1,
            )
            .exists()
        )
    fingerprint = hashlib.sha256(
        json.dumps(
            [scope, {key: value for key, value in options.items() if key != "cursor"}],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    ordering = (records.c.observed_at, records.c.event_id, records.c.run_id)
    if options["mode"] == "raw":
        if options["cursor"]:
            position = cursor_value(secret, fingerprint, token=options["cursor"])
            statement = statement.where(tuple_(*ordering) > tuple_(*position))
        statement = statement.order_by(*ordering).limit(options["limit"] + 1)
    else:
        ranked = statement.add_columns(
            func.row_number().over(order_by=ordering).label("position"),
            func.count().over().label("total"),
        ).subquery()
        stride = case(
            (ranked.c.total <= options["max_points"], 1),
            else_=cast(
                (ranked.c.total + options["max_points"] - 3)
                / (options["max_points"] - 1),
                Integer,
            ),
        )
        statement = (
            select(ranked)
            .where(
                or_(
                    (ranked.c.position - 1) % stride == 0,
                    ranked.c.position == ranked.c.total,
                )
            )
            .order_by(ranked.c.position)
        )

    def event(row):
        value = {**json.loads(row["payload"]), "run_id": row["run_id"]}
        if options["metrics"]:
            value["metrics"] = {
                key: number
                for key, number in value["metrics"].items()
                if key in options["metrics"]
            }
        return value

    events, sizes, positions = [], [], []
    total, used, truncated = 0, 2, False
    with repository.read_connection() as connection:
        with connection.execution_options(yield_per=1).execute(statement) as result:
            for row in result.mappings():
                if options["mode"] == "raw" and len(events) == options["limit"]:
                    truncated = True
                    break
                value = event(row)
                size = len(json.dumps(value, separators=(",", ":")).encode()) + 1
                if options["mode"] == "raw" and used + size > MAX_QUERY_BYTES:
                    if not events:
                        raise ValueError("METRICS_EVENT_TOO_LARGE")
                    truncated = True
                    break
                events.append(value)
                sizes.append(size)
                positions.append([row["observed_at"], row["event_id"], row["run_id"]])
                used += size
                if options["mode"] == "sample":
                    total = row["total"]
                    while used > MAX_QUERY_BYTES:
                        if len(events) <= 2:
                            raise ValueError("METRICS_EVENT_TOO_LARGE")
                        selected = sorted(
                            set(range(0, len(events), 2)) | {len(events) - 1}
                        )
                        events, sizes, positions = (
                            [values[index] for index in selected]
                            for values in (events, sizes, positions)
                        )
                        used = sum(sizes) + 2
    sampled = options["mode"] == "sample" and total > len(events)
    next_cursor = None
    if truncated and positions:
        next_cursor = cursor_value(secret, fingerprint, positions[-1])
    return dict(
        events=events,
        truncated=truncated or sampled,
        sampled=sampled,
        next_cursor=next_cursor,
    )
