"""Scalar SDK transport with bounded, durable, acknowledged local spooling."""

import json
import math
import os
import re
import stat
import threading
import time
import uuid
import warnings
from collections import deque
from itertools import islice
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx2

import trackio.apple_gpu as apple_metrics
import trackio.cpu as cpu_metrics
import trackio.gpu as gpu_metrics
from trackio.cloud_artifacts import WorkspaceRunMixin
from trackio.cloud_rich_client import CloudSummary, RichCloudRun

MAX_BATCH_EVENTS = 256
MAX_BATCH_BYTES = 1024 * 1024
BATCH_JOURNAL_RESERVE = 16 * 1024
DELIVERY_STATUS_RESERVE = 192
EVENT_FILE_NAME = re.compile(r"[0-9]+-[0-9a-f]{32}\.json")

CONFIG_FIELDS = frozenset(
    {
        "learning_rate",
        "lr",
        "batch_size",
        "per_device_train_batch_size",
        "gradient_accumulation_steps",
        "max_steps",
        "num_train_epochs",
        "seed",
        "model_type",
        "model",
        "recipe_revision",
        "dataset_revision",
        "recipe_digest",
        "dataset_digest",
        "model/num_parameters",
    }
)


class CloudConfig(dict):
    def __init__(self, run):
        super().__init__()
        self.run = run

    @staticmethod
    def selected(values):
        return {
            key: value
            for key, value in values.items()
            if key in CONFIG_FIELDS
            and isinstance(value, (str, int, float, bool, type(None)))
            and (not isinstance(value, float) or math.isfinite(value))
            and (not isinstance(value, str) or len(value) <= 4096)
        }

    def __setitem__(self, key, value):
        self.update({key: value})

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self.get(key)

    def __ior__(self, values):
        self.update(values)
        return self

    def update(self, values=None, *, allow_val_change=False, **kwargs):
        selected = self.selected({**(values or {}), **kwargs})
        if not selected:
            return
        try:
            with self.run.lock:
                if self.run.closed:
                    return
                super().update(selected)
                self.run._write(
                    self.run.directory / "config.pending", self.selected(self)
                )
        except (OSError, BufferError):
            self.run._record_gap()
            return
        self.run._send()


class CloudRun(WorkspaceRunMixin, RichCloudRun):
    def __init__(
        self,
        server_url,
        token,
        run_id,
        spool_dir,
        *,
        trial_id=None,
        refresh_token=None,
        categories=None,
        transport=None,
        max_bytes=16 * 1024 * 1024,
    ):
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.refresh_token = refresh_token
        self.run_id = run_id
        self._space_id = None
        self.closed = False
        self.summary = CloudSummary(self)
        self.endpoint = self.server_url + "/v1/runs/" + quote(run_id, safe="")
        self.trial_id = trial_id
        if categories is not None and (
            not isinstance(categories, (list, tuple, set, frozenset))
            or not categories
            or set(categories) - {"experiment", "compute"}
        ):
            raise ValueError("INVALID_METRICS_CATEGORIES")
        self.categories = frozenset(categories or ("experiment", "compute"))
        self.name = run_id
        self.max_bytes = max_bytes
        self.directory = Path(spool_dir)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = self.directory.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise ValueError("PRIVATE_SPOOL_DIRECTORY_REQUIRED")
        self.transport = transport if transport is not None else self._new_transport()
        self.owns_transport = transport is None
        self.lock = threading.RLock()
        self.flush_lock = threading.Lock()
        self.config = CloudConfig(self)
        self.event_batch_path = self.directory / "event-batch.pending"
        self.state_path = self.directory / "state"
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            if self.state["run_id"] != run_id or self.state.get("trial_id") != trial_id:
                raise ValueError("SPOOL_IDENTITY_MISMATCH")
        else:
            self.state = {
                "run_id": run_id,
                "trial_id": trial_id,
                "writer_id": uuid.uuid4().hex,
                "step": -1,
            }
            self._write(self.state_path, self.state)

    @staticmethod
    def _new_transport():
        return httpx2.Client(timeout=10, follow_redirects=False, trust_env=False)

    def _sync_directory(self):
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _write(self, path, payload):
        encoded = json.dumps(payload, allow_nan=False, separators=(",", ":"))
        previous_size = path.stat().st_size if path.exists() else 0
        limit = self.max_bytes
        if path == self.event_batch_path:
            limit -= min(DELIVERY_STATUS_RESERVE, self.max_bytes // 4)
        elif path.name not in {"state", "delivery-status"}:
            limit -= min(BATCH_JOURNAL_RESERVE, self.max_bytes // 4)
        if self._spool_size() - previous_size + len(encoded.encode()) > limit:
            raise BufferError("METRICS_SPOOL_FULL")
        temporary = path.with_suffix("." + uuid.uuid4().hex + ".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            self._sync_directory()
        finally:
            temporary.unlink(missing_ok=True)

    def log(self, metrics, step=None):
        if not isinstance(metrics, dict) or not metrics:
            raise ValueError("METRICS_DICTIONARY_REQUIRED")
        scalar = {
            key: value for key, value in metrics.items() if type(value) in (int, float)
        }
        rich = {key: value for key, value in metrics.items() if key not in scalar}
        if scalar:
            self._log(scalar, step, "experiment")
        if rich and "experiment" in self.categories:
            self.queue_record(
                "history",
                rich,
                step if step is not None else scalar.get("train/global_step"),
            )

    def log_system(self, metrics):
        if "compute" not in self.categories:
            return
        if not isinstance(metrics, dict):
            raise ValueError("METRICS_DICTIONARY_REQUIRED")
        items = list(metrics.items())
        for offset in range(0, len(items), 128):
            self._log(dict(items[offset : offset + 128]), None, "compute")

    def _log(self, metrics, step, category):
        if category not in self.categories:
            return
        with self.lock:
            if not isinstance(metrics, dict) or not metrics or len(metrics) > 128:
                raise ValueError("SCALAR_METRICS_REQUIRED")
            values = {}
            for key, value in metrics.items():
                if (
                    not isinstance(key, str)
                    or not key
                    or len(key) > 256
                    or type(value) not in (int, float)
                    or not math.isfinite(value)
                ):
                    raise ValueError("FINITE_SCALAR_METRICS_REQUIRED")
                values[key] = value
            if step is None and category == "experiment":
                step = values.get("train/global_step")
            if step is not None and (type(step) is not int or step < 0):
                raise ValueError("NONNEGATIVE_INTEGER_STEP_REQUIRED")
            event_id = uuid.uuid4().hex
            event = {
                "event_id": event_id,
                "writer_id": self.state["writer_id"],
                "trial_id": self.trial_id,
                "step": step,
                "observed_at": time.time(),
                "category": category,
                "metrics": values,
            }
            payload = {"batch_id": event_id, "events": [event]}
            size = len(json.dumps(payload).encode())
            if self._spool_size() + size > self.max_bytes:
                raise BufferError("METRICS_SPOOL_FULL")
            self._write(
                self.directory / (str(time.time_ns()) + "-" + event_id + ".json"),
                payload,
            )
            if step is not None:
                self.state["step"] = max(self.state["step"], step)
                self._write(self.state_path, self.state)
        self._send()

    def _send(self):
        self.flush()

    def _record_gap(self):
        raise OSError("METRICS_SPOOL_UNAVAILABLE")

    def flush(self):
        self._flush()

    def _flush(self, stopped=None):
        def cancelled():
            return stopped is not None and stopped.is_set()

        with self.flush_lock:
            config_path = self.directory / "config.pending"
            with self.lock:
                if cancelled():
                    return
                self._collect_spool_objects()
                config = config_path.read_bytes() if config_path.exists() else None
            if config is not None:
                response = self._request(
                    "put", self.endpoint + "/config", json=json.loads(config)
                )
                self._check_ack(response)
                with self.lock:
                    if cancelled():
                        return
                    if config_path.exists() and config_path.read_bytes() == config:
                        config_path.unlink()

            with self.lock:
                if cancelled():
                    return
                journal = self._read_batch_journal()
                if journal is not None:
                    events = (
                        None
                        if journal["acknowledged"]
                        else self._read_batch_events(journal)
                    )
            if journal is not None:
                self._flush_event_batch(journal, events, cancelled)

            with self.lock:
                if cancelled():
                    return
                paths = deque(sorted(self.directory.glob("*.json")))
            while paths:
                with self.lock:
                    if cancelled():
                        return
                    path = paths.popleft()
                    payload = json.loads(path.read_text())
                    if "record" not in payload:
                        journal, events = self._prepare_event_batch(
                            path, payload, paths
                        )
                if "record" in payload:
                    self._flush_record(payload)
                    with self.lock:
                        if cancelled():
                            return
                        path.unlink(missing_ok=True)
                        self._collect_spool_objects()
                else:
                    self._flush_event_batch(journal, events, cancelled)

    @staticmethod
    def _event_bytes(events):
        def normalized(value):
            return int(value) if type(value) is float and value.is_integer() else value

        size = 0
        for event in events:
            canonical_event = {
                **event,
                "observed_at": normalized(event["observed_at"]),
                "metrics": {
                    key: normalized(value) for key, value in event["metrics"].items()
                },
            }
            size += 1 + max(
                len(json.dumps(event, separators=(",", ":"))),
                len(json.dumps(canonical_event, separators=(",", ":"))),
            )
        return size

    def _prepare_event_batch(self, path, payload, paths):
        first_events = payload["events"]
        events = list(first_events)
        batch_paths = [path.name]
        batch_id = "sdk-" + uuid.uuid4().hex
        size = (
            len(batch_id)
            + len('{"batch_id":"","events":[]}')
            + self._event_bytes(events)
        )
        for next_path in islice(paths, MAX_BATCH_EVENTS - 1):
            next_payload = json.loads(next_path.read_text())
            if "record" in next_payload:
                break
            next_events = next_payload["events"]
            next_size = size + self._event_bytes(next_events)
            if (
                len(events) + len(next_events) > MAX_BATCH_EVENTS
                or next_size > MAX_BATCH_BYTES
            ):
                break
            size = next_size
            batch_paths.append(next_path.name)
            events.extend(next_events)

        journal = {"batch_id": batch_id, "paths": batch_paths, "acknowledged": False}
        if len(batch_paths) > 1:
            try:
                self._write(self.event_batch_path, journal)
            except BufferError:
                batch_paths = [path.name]
            else:
                for _ in range(len(batch_paths) - 1):
                    paths.popleft()
                return journal, events

        journal.update(batch_id=payload["batch_id"], paths=batch_paths)
        self._write(self.event_batch_path, journal)
        return journal, first_events

    def _read_batch_journal(self):
        if not self.event_batch_path.exists():
            return None
        journal = json.loads(self.event_batch_path.read_text())
        if (
            not isinstance(journal, dict)
            or not isinstance(journal.get("batch_id"), str)
            or not isinstance(journal.get("paths"), list)
            or not 1 <= len(journal["paths"]) <= MAX_BATCH_EVENTS
            or any(
                not isinstance(name, str) or not EVENT_FILE_NAME.fullmatch(name)
                for name in journal["paths"]
            )
            or type(journal.get("acknowledged")) is not bool
        ):
            raise ValueError("INVALID_BATCH_JOURNAL")
        return journal

    def _read_batch_events(self, journal):
        return [
            event
            for name in journal["paths"]
            for event in json.loads((self.directory / name).read_text())["events"]
        ]

    @staticmethod
    def _check_ack(response, key=None, identity=None):
        response.raise_for_status()
        try:
            ack = response.json()
        except ValueError:
            raise RuntimeError("INVALID_COMMIT_ACK") from None
        if (
            not isinstance(ack, dict)
            or ack.get("committed") is not True
            or (key is not None and ack.get(key) != identity)
        ):
            raise RuntimeError("INVALID_COMMIT_ACK")

    def _flush_event_batch(self, journal, events, cancelled):
        if cancelled():
            return
        if events is not None:
            batch_id = journal["batch_id"]
            response = self._request(
                "post",
                self.endpoint + "/events",
                json={"batch_id": batch_id, "events": events},
            )
            self._check_ack(response, "batch_id", batch_id)
        with self.lock:
            if cancelled():
                return
            if events is not None:
                self._write(self.event_batch_path, {**journal, "acknowledged": True})
            for name in journal["paths"]:
                (self.directory / name).unlink(missing_ok=True)
            self._sync_directory()
            self.event_batch_path.unlink()
            self._sync_directory()

    def _request(self, method, endpoint, **kwargs):
        send = getattr(self.transport, method)
        headers = kwargs.pop("headers", {})
        response = send(
            endpoint,
            headers={**headers, "Authorization": "Bearer " + self.token},
            **kwargs,
        )
        if response.status_code == 401 and self.refresh_token:
            refreshed = self.transport.post(
                self.server_url + "/v1/capabilities/refresh",
                headers={"Authorization": "Bearer " + self.refresh_token},
            )
            refreshed.raise_for_status()
            payload = refreshed.json()
            if (
                payload.get("run_id") != self.run_id
                or payload.get("trial_id") != self.trial_id
                or not isinstance(payload.get("token"), str)
            ):
                raise RuntimeError("INVALID_REFRESH_SCOPE")
            self.token = payload["token"]
            response = send(
                endpoint,
                headers={**headers, "Authorization": "Bearer " + self.token},
                **kwargs,
            )
        return response

    def finish(self):
        self.flush()
        if self.owns_transport:
            self.transport.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.finish()

    def __getattr__(self, name):
        raise NotImplementedError("Cloud scalar SDK does not support: " + name)


class ManagedCloudRun(CloudRun):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wake = threading.Event()
        self.stopped = threading.Event()
        self.progress = threading.Condition(self.lock)
        self._finishers = 0
        self._worker_retired = False
        self._last_error = None
        self.delivery_status = {"committed": False, "pending": 0, "dropped": 0}
        self.monitors = []
        self.system_started = False
        self.closed = False
        previous = self.directory / "delivery-status"
        if previous.exists():
            dropped = json.loads(previous.read_text())["dropped"]
            if type(dropped) is not int or dropped < 0:
                raise ValueError("INVALID_DELIVERY_RECEIPT")
            self.delivery_status["dropped"] = dropped
        self._start_worker()

    def _start_worker(self):
        self.worker = threading.Thread(target=self._pump, daemon=True)
        self.worker.start()
        self.wake.set()

    def start_system(
        self,
        auto_log_gpu=None,
        auto_log_cpu=None,
        gpu_log_interval=10,
        cpu_log_interval=10,
    ):
        if "compute" not in self.categories or self.system_started or self.closed:
            return
        for interval in (gpu_log_interval, cpu_log_interval):
            if (
                type(interval) not in (int, float)
                or not math.isfinite(interval)
                or not 1 <= interval <= 3600
            ):
                raise ValueError("INVALID_SYSTEM_METRICS_INTERVAL")
        self.system_started = True
        choices = []
        cpu_enabled = auto_log_cpu is True or (
            auto_log_cpu is None and cpu_metrics.cpu_available()
        )
        if auto_log_gpu is not False:
            if gpu_metrics.gpu_available():
                choices.append((gpu_metrics.GpuMonitor, {"interval": gpu_log_interval}))
            elif apple_metrics.apple_gpu_available():
                choices.append(
                    (
                        apple_metrics.AppleGpuMonitor,
                        {
                            "interval": gpu_log_interval,
                            "include_cpu_metrics": not cpu_enabled,
                        },
                    )
                )
        if cpu_enabled:
            choices.append((cpu_metrics.CpuMonitor, {"interval": cpu_log_interval}))
        for monitor_class, options in choices:
            try:
                monitor = monitor_class(self, **options)
                monitor.start()
                self.monitors.append(monitor)
            except Exception:
                self._record_gap()

    def _send(self):
        self.wake.set()

    def _pump(self):
        while True:
            self.wake.wait(1)
            self.wake.clear()
            with self.progress:
                if self.stopped.is_set():
                    self._worker_retired = True
                    transport = self.transport
                    break
            category = None
            try:
                self._flush(self.stopped)
            except (
                httpx2.HTTPError,
                OSError,
                RuntimeError,
                ValueError,
                KeyError,
            ) as error:
                category = self._error_category(error)
            with self.progress:
                if not self.stopped.is_set():
                    self._last_error = category
                self.progress.notify_all()
            if category:
                self.stopped.wait(1)
        if self.owns_transport:
            transport.close()

    @staticmethod
    def _error_category(error):
        if isinstance(error, httpx2.HTTPStatusError):
            status = error.response.status_code
            if status in (401, 403):
                return "permission"
            if status >= 500 or status in (408, 429):
                return "transport"
            return "rejected"
        if isinstance(error, httpx2.HTTPError):
            return "transport"
        if str(error) == "INVALID_REFRESH_SCOPE":
            return "permission"
        if str(error) in (
            "INVALID_COMMIT_ACK",
            "INVALID_RECORD_ACK",
            "INVALID_OBJECT_ACK",
        ):
            return "invalid_ack"
        return "spool"

    def log(self, metrics, step=None):
        with self.lock:
            if not self.closed:
                super().log(metrics, step)

    def _log(self, *args):
        try:
            with self.lock:
                if not self.closed:
                    super()._log(*args)
        except (OSError, BufferError, ValueError):
            self._record_gap()

    def queue_record(self, *args, **kwargs):
        try:
            super().queue_record(*args, **kwargs)
        except ValueError:
            self._record_gap()

    def _record_gap(self):
        with self.lock:
            self.delivery_status["dropped"] += 1
        warnings.warn(
            "TRACKIO_METRICS_GAP: metrics could not be durably buffered",
            RuntimeWarning,
            stacklevel=2,
        )

    def finish(self, timeout=5):
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("INVALID_FINISH_TIMEOUT")
        deadline = time.monotonic() + timeout
        with self.progress:
            self.closed = True
            monitors, self.monitors = self.monitors, []
            self._finishers += 1
            if timeout > 0 and self._pending_count():
                self.stopped.clear()
                if self._worker_retired:
                    if self.owns_transport:
                        self.transport = self._new_transport()
                    self._worker_retired = False
                    self._start_worker()
                else:
                    self.wake.set()
        try:
            for monitor in monitors:
                try:
                    monitor.stop(timeout=max(0, min(2, deadline - time.monotonic())))
                except Exception:
                    self._record_gap()
            with self.progress:
                while self._pending_count():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self.progress.wait(min(remaining, 0.05))
        finally:
            with self.progress:
                self._finishers -= 1
                if not self._finishers:
                    self.stopped.set()
                    self.wake.set()
                status = self._delivery_receipt()
        if status["pending"]:
            warnings.warn(
                "TRACKIO_METRICS_DELAYED: pending metrics remain in the private spool",
                RuntimeWarning,
                stacklevel=2,
            )
        return status

    def _pending_count(self):
        pending = len(list(self.directory.glob("*.json")))
        if not pending and self.event_batch_path.exists():
            pending = 1
        return pending + int((self.directory / "config.pending").exists())

    def _delivery_receipt(self):
        pending = self._pending_count()
        dropped = self.delivery_status["dropped"]
        status = {
            "committed": not pending and not dropped,
            "pending": pending,
            "dropped": dropped,
        }
        if pending:
            status["error_category"] = self._last_error or "timeout"
        elif dropped:
            status["error_category"] = "dropped"
        try:
            self._write(self.directory / "delivery-status", status)
        except (OSError, BufferError):
            self._record_gap()
            status.update(committed=False, dropped=dropped + 1, error_category="spool")
        self.delivery_status = status
        return dict(status)


def init_from_context(path, *, config=None, system=None):
    context_path = Path(path)
    metadata = context_path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError("PRIVATE_CONTEXT_FILE_REQUIRED")
    context = json.loads(context_path.read_text())
    address = urlsplit(context["server_url"])
    if (
        not address.hostname
        or address.username
        or address.password
        or address.query
        or address.fragment
        or address.scheme not in ("http", "https")
    ):
        raise ValueError("INVALID_METRICS_ADDRESS")
    if address.scheme == "http" and address.hostname not in (
        "127.0.0.1",
        "::1",
        "localhost",
    ):
        raise ValueError("HTTPS_METRICS_ADDRESS_REQUIRED")
    run = ManagedCloudRun(
        context["server_url"],
        context["token"],
        context["run_id"],
        context["spool_dir"],
        trial_id=context.get("trial_id"),
        refresh_token=context.get("refresh_token"),
        categories=context.get("categories"),
    )
    try:
        run.config.update(config)
        run.start_system(**(system or {}))
    except Exception:
        run.finish(timeout=0)
        raise
    return run
