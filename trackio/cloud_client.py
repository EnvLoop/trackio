"""Scalar SDK transport with bounded, durable, acknowledged local spooling."""

import json
import math
import os
import stat
import threading
import time
import uuid
import warnings
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx2

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
    }
)


class CloudConfig(dict):
    def __init__(self, run):
        super().__init__()
        self.run = run

    def update(self, values=None, *, allow_val_change=False, **kwargs):
        selected = {
            key: value
            for key, value in {**(values or {}), **kwargs}.items()
            if key in CONFIG_FIELDS
            and isinstance(value, (str, int, float, bool, type(None)))
        }
        if not selected:
            return
        try:
            with self.run.lock:
                super().update(selected)
                self.run._write(self.run.directory / "config.pending", dict(self))
        except OSError:
            self.run._record_gap()
            return
        self.run._send()


class CloudRun:
    def __init__(
        self,
        server_url,
        token,
        run_id,
        spool_dir,
        *,
        trial_id=None,
        transport=None,
        max_bytes=16 * 1024 * 1024,
    ):
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.run_id = run_id
        self.endpoint = self.server_url + "/v1/runs/" + quote(run_id, safe="")
        self.trial_id = trial_id
        self.name = run_id
        self.max_bytes = max_bytes
        self.directory = Path(spool_dir)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        metadata = self.directory.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("PRIVATE_SPOOL_DIRECTORY_REQUIRED")
        self.transport = transport or httpx2.Client(
            timeout=10, follow_redirects=False, trust_env=False
        )
        self.owns_transport = transport is None
        self.lock = threading.RLock()
        self.flush_lock = threading.Lock()
        self.config = CloudConfig(self)
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

    def _write(self, path, payload):
        temporary = path.with_suffix("." + uuid.uuid4().hex + ".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w") as stream:
                json.dump(payload, stream, allow_nan=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            descriptor = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def log(self, metrics, step=None):
        return self._log(metrics, step, "experiment")

    def log_system(self, metrics):
        return self._log(metrics, None, "compute")

    def _log(self, metrics, step, category):
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
            if (
                sum(path.stat().st_size for path in self.directory.glob("*.json"))
                + size
                > self.max_bytes
            ):
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
        with self.flush_lock:
            config_path = self.directory / "config.pending"
            with self.lock:
                config = config_path.read_bytes() if config_path.exists() else None
                paths = sorted(self.directory.glob("*.json"))
            if config is not None:
                response = self.transport.put(
                    self.endpoint + "/config",
                    json=json.loads(config),
                    headers={"Authorization": "Bearer " + self.token},
                )
                response.raise_for_status()
                if response.json().get("committed") is not True:
                    raise RuntimeError("INVALID_COMMIT_ACK")
                with self.lock:
                    if config_path.exists() and config_path.read_bytes() == config:
                        config_path.unlink()
            for path in paths:
                payload = json.loads(path.read_text())
                response = self.transport.post(
                    self.endpoint + "/events",
                    headers={"Authorization": "Bearer " + self.token},
                    json=payload,
                )
                response.raise_for_status()
                ack = response.json()
                if (
                    not ack.get("committed")
                    or ack.get("batch_id") != payload["batch_id"]
                ):
                    raise RuntimeError("INVALID_COMMIT_ACK")
                with self.lock:
                    path.unlink(missing_ok=True)

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
        self.delivery_status = {"committed": False, "pending": 0, "dropped": 0}
        previous = self.directory / "delivery-status"
        if previous.exists():
            self.delivery_status["dropped"] = json.loads(previous.read_text())[
                "dropped"
            ]
        self.worker = threading.Thread(target=self._pump, daemon=True)
        self.worker.start()

    def _send(self):
        self.wake.set()

    def _pump(self):
        try:
            while not self.stopped.is_set():
                self.wake.wait(1)
                self.wake.clear()
                try:
                    self.flush()
                except (OSError, httpx2.HTTPError, RuntimeError):
                    self.delivery_status["committed"] = False
                    self.stopped.wait(1)
        finally:
            if self.owns_transport:
                self.transport.close()

    def _log(self, *args):
        try:
            super()._log(*args)
        except (OSError, BufferError):
            self._record_gap()

    def _record_gap(self):
        self.delivery_status["dropped"] += 1
        warnings.warn(
            "TRACKIO_METRICS_GAP: metrics could not be durably buffered",
            RuntimeWarning,
            stacklevel=2,
        )

    def finish(self, timeout=5):
        deadline = time.monotonic() + timeout
        self.wake.set()
        while time.monotonic() < deadline:
            pending = len(list(self.directory.glob("*.json"))) + int(
                (self.directory / "config.pending").exists()
            )
            if not pending:
                break
            self.stopped.wait(0.05)
        self.stopped.set()
        self.worker.join(timeout=0.1)
        pending = len(list(self.directory.glob("*.json"))) + int(
            (self.directory / "config.pending").exists()
        )
        self.delivery_status.update(
            committed=pending == 0 and self.delivery_status["dropped"] == 0,
            pending=pending,
        )
        try:
            self._write(self.directory / "delivery-status", self.delivery_status)
        except OSError:
            self._record_gap()
            self.delivery_status["committed"] = False
        if not self.delivery_status["committed"]:
            warnings.warn(
                "TRACKIO_METRICS_DELAYED: pending metrics remain in the private spool",
                RuntimeWarning,
                stacklevel=2,
            )
        return dict(self.delivery_status)


def init_from_context(path, *, config=None):
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
    )
    run.config.update(config)
    return run
