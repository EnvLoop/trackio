import hashlib
import json
import math
import mimetypes
import os
import time
import uuid

import httpx2
import numpy as np

from trackio.histogram import Histogram
from trackio.markdown import Markdown
from trackio.media.media import TrackioMedia
from trackio.table import Table
from trackio.trace import Trace


class CloudSummary(dict):
    def __init__(self, run):
        super().__init__()
        self.run = run

    def __setitem__(self, key, value):
        self.update({key: value})

    def update(self, values=None, **kwargs):
        values = {**(values or {}), **kwargs}
        self.run.queue_record("summary", values)
        super().update(values)

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self.get(key)


class RichCloudRun:
    def _spool_size(self):
        return sum(
            path.stat().st_size for path in self.directory.iterdir() if path.is_file()
        )

    def _stage_bytes(self, payload, content_type, uploads):
        if len(payload) > 32 * 1024 * 1024:
            raise BufferError("METRICS_OBJECT_TOO_LARGE")
        digest = hashlib.sha256(payload).hexdigest()
        path = self.directory / (digest + ".blob")
        if not path.exists():
            if self._spool_size() + len(payload) > self.max_bytes:
                raise BufferError("METRICS_SPOOL_FULL")
            temporary = self.directory / (uuid.uuid4().hex + ".pending")
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        uploads[digest] = {
            "digest": digest,
            "content_type": content_type,
            "size": len(payload),
        }
        return {"_object": digest}

    def _serialize_rich(self, value, uploads, depth=0):
        if depth > 24:
            raise ValueError("METRICS_NESTING_LIMIT")
        if isinstance(value, TrackioMedia):
            extension = value._file_extension()
            if not extension.isalnum() or len(extension) > 12:
                raise ValueError("INVALID_MEDIA_EXTENSION")
            path = self.directory / (uuid.uuid4().hex + "." + extension)
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
            try:
                value._save_media(path)
                path.chmod(0o600)
                if path.stat().st_size > min(32 * 1024 * 1024, self.max_bytes):
                    raise BufferError("METRICS_OBJECT_TOO_LARGE")
                content = self._stage_bytes(
                    path.read_bytes(),
                    mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                    uploads,
                )
                return {"_type": value.TYPE, "caption": value.caption, **content}
            finally:
                path.unlink(missing_ok=True)
        if isinstance(value, Table):
            value = {"_type": value.TYPE, "data": value.data}
        elif isinstance(value, Trace):
            value = {
                "_type": value.TYPE,
                "messages": value.messages,
                "metadata": value.metadata,
                "spans": value.spans,
            }
        elif isinstance(value, (Histogram, Markdown)):
            value = value._to_dict()
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, dict):
            if any(
                not isinstance(key, str) or not key or len(key) > 256 for key in value
            ):
                raise ValueError("INVALID_RECORD_KEY")
            return {
                key: self._serialize_rich(child, uploads, depth + 1)
                for key, child in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [self._serialize_rich(child, uploads, depth + 1) for child in value]
        if value is None or type(value) in (str, bool, int):
            return value
        if type(value) is float and math.isfinite(value):
            return value
        raise ValueError("UNSUPPORTED_METRICS_VALUE")

    def queue_record(self, kind, payload, step=None):
        try:
            with self.lock:
                if self.closed:
                    return
                uploads = {}
                value = self._serialize_rich(payload, uploads)
                record = dict(
                    record_id=uuid.uuid4().hex,
                    kind=kind,
                    observed_at=time.time(),
                    trial_id=self.trial_id,
                    writer_id=self.state["writer_id"],
                    step=step,
                    payload=value,
                )
                pending = {"record": record, "objects": list(uploads.values())}
                if len(json.dumps(pending).encode()) > 1024 * 1024:
                    raise BufferError("METRICS_RECORD_TOO_LARGE")
                if (
                    self._spool_size() + len(json.dumps(pending).encode())
                    > self.max_bytes
                ):
                    raise BufferError("METRICS_SPOOL_FULL")
                self._write(
                    self.directory
                    / (str(time.time_ns()) + "-" + record["record_id"] + ".json"),
                    pending,
                )
            self._send()
        except (OSError, BufferError, httpx2.HTTPError, RuntimeError):
            self._record_gap()

    def _flush_record(self, pending):
        for entry in pending["objects"]:
            digest = entry["digest"]
            payload = (self.directory / (digest + ".blob")).read_bytes()
            if (
                len(payload) != entry["size"]
                or hashlib.sha256(payload).hexdigest() != digest
            ):
                raise RuntimeError("METRICS_OBJECT_INTEGRITY_FAILURE")
            response = self._request(
                "put",
                self.endpoint + "/objects/" + digest,
                content=payload,
                headers={"Content-Type": entry["content_type"]},
            )
            response.raise_for_status()
            if (
                response.json().get("committed") is not True
                or response.json().get("digest") != digest
            ):
                raise RuntimeError("INVALID_OBJECT_ACK")
        record = pending["record"]
        response = self._request("post", self.endpoint + "/records", json=record)
        response.raise_for_status()
        if (
            response.json().get("committed") is not True
            or response.json().get("record_id") != record["record_id"]
        ):
            raise RuntimeError("INVALID_RECORD_ACK")

    def _collect_spool_objects(self):
        live = {
            entry["digest"]
            for path in self.directory.glob("*.json")
            for entry in json.loads(path.read_text()).get("objects", [])
        }
        for path in self.directory.glob("*.blob"):
            if path.stem not in live:
                path.unlink(missing_ok=True)

    def alert(self, title, text=None, level="WARN", webhook_url=None):
        if webhook_url is not None:
            raise ValueError("MANAGED_WEBHOOK_DESTINATIONS_DISABLED")
        self.queue_record("alert", {"title": title, "text": text, "level": str(level)})
