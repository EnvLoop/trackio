"""Content-verified structured JSON and Parquet exports, without SQLite."""

import importlib
import json
import os
import uuid
from pathlib import Path

from trackio.cloud_protocol import canonical, content_digest


def verify(snapshot):
    if not isinstance(snapshot, dict) or snapshot.get("digest") != content_digest(
        {key: value for key, value in snapshot.items() if key != "digest"}
    ):
        raise ValueError("EXPORT_CONTENT_INTEGRITY_FAILURE")
    return snapshot


def write(snapshot, path, *, format="json"):
    verify(snapshot)
    path = Path(path)
    if path.exists() or path.is_symlink():
        raise FileExistsError(path)
    temporary = path.with_name("." + uuid.uuid4().hex + ".pending")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            if format == "json":
                stream.write(canonical(snapshot).encode())
            elif format == "parquet":
                arrow = importlib.import_module("pyarrow")
                parquet = importlib.import_module("pyarrow.parquet")
                points = [json.loads(value["payload"]) for value in snapshot["events"]]
                table = arrow.table(
                    {
                        "event_id": arrow.array(
                            [point["event_id"] for point in points], type=arrow.string()
                        ),
                        "trial_id": arrow.array(
                            [point.get("trial_id") for point in points],
                            type=arrow.string(),
                        ),
                        "step": arrow.array(
                            [point.get("step") for point in points], type=arrow.int64()
                        ),
                        "observed_at": arrow.array(
                            [point["observed_at"] for point in points],
                            type=arrow.float64(),
                        ),
                        "payload": arrow.array(
                            [canonical(point) for point in points], type=arrow.string()
                        ),
                    }
                )
                table = table.replace_schema_metadata(
                    {b"trackio.snapshot": canonical(snapshot).encode()}
                )
                parquet.write_table(table, stream)
            else:
                raise ValueError("INVALID_EXPORT_FORMAT")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return str(path)


def read(path, *, format="json", max_bytes=64 * 1024 * 1024):
    path = Path(path)
    if path.stat().st_size > max_bytes:
        raise ValueError("EXPORT_SIZE_LIMIT")
    if format == "json":
        return verify(json.loads(path.read_bytes()))
    if format != "parquet":
        raise ValueError("INVALID_EXPORT_FORMAT")
    parquet = importlib.import_module("pyarrow.parquet")
    table = parquet.read_table(path)
    snapshot = verify(json.loads(table.schema.metadata[b"trackio.snapshot"]))
    expected = [json.loads(value["payload"]) for value in snapshot["events"]]
    if [json.loads(value) for value in table["payload"].to_pylist()] != expected:
        raise ValueError("EXPORT_CONTENT_INTEGRITY_FAILURE")
    return snapshot
