import json
import subprocess
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import httpx2
import pytest
from starlette.testclient import TestClient

from trackio.cloud import CloudConfig, MetricsRepository, create_app
from trackio.cloud_client import CloudRun, ManagedCloudRun


@pytest.fixture
def delivery(tmp_path):
    repository = MetricsRepository(f"sqlite:///{tmp_path / 'metrics.sqlite'}")
    config = CloudConfig(
        environment="local",
        database_url=repository.url,
        service_token="s" * 32,
        signing_key="k" * 32,
    )
    with TestClient(create_app(config, repository)) as client:
        binding = client.post(
            "/v1/runs",
            headers={"Authorization": "Bearer " + config.service_token},
            json={
                "workspace_id": "w",
                "project_id": "p",
                "job_id": "j",
                "run_id": "r",
                "config": {},
            },
        )
        assert binding.status_code == 200
        yield client, repository, binding.json()["token"]
    repository.close()


def step_values(repository):
    return [
        (event["step"], event["metrics"]["loss"])
        for event in repository.events("w", "p", "j")
    ]


def queue_legacy_events(spool, steps):
    spool.mkdir(mode=0o700)
    for step in steps:
        event_id = f"{step:032x}"
        event = {
            "event_id": event_id,
            "writer_id": "original-writer",
            "trial_id": None,
            "step": step,
            "observed_at": 100 + step / 1000,
            "category": "experiment",
            "metrics": {"loss": step / 10},
        }
        path = spool / f"{step:019}-{event_id}.json"
        path.write_text(json.dumps({"batch_id": event_id, "events": [event]}))
        path.chmod(0o600)
    state = spool / "state"
    state.write_text(
        json.dumps(
            {
                "run_id": "r",
                "trial_id": None,
                "writer_id": "original-writer",
                "step": step,
            }
        )
    )
    state.chmod(0o600)


def test_late_recovery_delivers_all_1815_metric_steps(delivery, tmp_path, monkeypatch):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    prefix_sent = threading.Event()
    tail_attempted = threading.Event()
    recovered = threading.Event()
    attempts = []
    post = client.post

    def interrupted_post(url, *, json, **kwargs):
        steps = [event["step"] for event in json["events"]]
        attempts.append(steps)
        if steps[0] >= 1592 and not recovered.is_set():
            tail_attempted.set()
            return httpx2.Response(503, request=httpx2.Request("POST", url))
        response = post(url, json=json, **kwargs)
        if 1591 in steps:
            prefix_sent.set()
        return response

    queue_legacy_events(spool, range(1, 1592))
    monkeypatch.setattr(client, "post", interrupted_post)
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    assert prefix_sent.wait(15)
    assert step_values(repository) == [(step, step / 10) for step in range(1, 1592)]
    for step in range(1592, 1816):
        run.log({"loss": step / 10}, step=step)
    assert tail_attempted.wait(5)
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
        first = run.finish(timeout=0.03)
    assert first["pending"] == 224 and first["committed"] is False
    assert len(list(spool.glob("*.json"))) == 224
    assert json.loads((spool / "delivery-status").read_text()) == first
    recovered.set()
    assert run.finish(timeout=2) == {"committed": True, "pending": 0, "dropped": 0}
    assert step_values(repository) == [(step, step / 10) for step in range(1, 1816)]
    assert max(map(len, attempts)) <= 256
    assert len(attempts) < 100


@pytest.mark.parametrize("initial_steps", [[1], [1, 2]], ids=["singleton", "batch"])
@pytest.mark.parametrize("lost_ack", [True, False], ids=["lost-ack", "late-ack"])
def test_reopen_reuses_original_batch_after_new_events_arrive(
    delivery, tmp_path, monkeypatch, initial_steps, lost_ack
):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    queue_legacy_events(spool, initial_steps)
    last_step = len(initial_steps) + 1
    written = {
        event["event_id"]: event
        for path in spool.glob("*.json")
        for event in json.loads(path.read_text())["events"]
    }
    committed_without_ack = threading.Event()
    release_response = threading.Event()
    attempts = []
    post = client.post

    def interrupt_first_ack(url, *, json, **kwargs):
        attempts.append(json)
        response = post(url, json=json, **kwargs)
        if len(attempts) == 1:
            committed_without_ack.set()
            assert release_response.wait(5)
            if lost_ack:
                raise httpx2.ReadError("synthetic credential marker")
        return response

    monkeypatch.setattr(client, "post", interrupt_first_ack)
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    try:
        assert committed_without_ack.wait(3)
        assert step_values(repository) == [(step, step / 10) for step in initial_steps]
        run.log({"loss": last_step / 10}, step=last_step)
        before = time.monotonic()
        with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
            timed_out = run.finish(timeout=0.03)
        assert time.monotonic() - before < 0.5
        assert timed_out == {
            "committed": False,
            "pending": last_step,
            "dropped": 0,
            "error_category": "timeout",
        }
        assert len(list(spool.glob("*.json"))) == last_step
    finally:
        release_response.set()
        run.worker.join(3)
    assert not run.worker.is_alive()
    assert len(list(spool.glob("*.json"))) == last_step
    reopened = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    assert reopened.finish(timeout=2)["committed"] is True
    assert attempts[1] == attempts[0]
    assert step_values(repository) == [
        (step, step / 10) for step in [*initial_steps, last_step]
    ]
    recovered = {event["event_id"]: event for event in repository.events("w", "p", "j")}
    assert {
        event_id: {key: recovered[event_id][key] for key in event}
        for event_id, event in written.items()
    } == written
    assert {event["writer_id"] for event in recovered.values()} == {"original-writer"}


def test_concurrent_finish_does_not_cancel_longer_wait_or_accept_late_config(
    delivery, tmp_path, monkeypatch
):
    client, repository, token = delivery
    started = threading.Event()
    release_response = threading.Event()
    finishing = threading.Event()
    post = client.post

    class Monitor:
        def stop(self, timeout):
            finishing.set()

    def block_response(url, **kwargs):
        started.set()
        assert release_response.wait(5)
        return post(url, **kwargs)

    monkeypatch.setattr(client, "post", block_response)
    spool = tmp_path / "spool"
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    run.monitors.append(Monitor())
    run.log({"loss": 0.1}, step=1)
    assert started.wait(2)
    with ThreadPoolExecutor(max_workers=1) as caller:
        longer = caller.submit(run.finish, timeout=2)
        try:
            assert finishing.wait(2)
            run.log({"loss": 0.2}, step=2)
            run.config.update({"lr": 0.5})
            with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
                earlier = run.finish(timeout=0.03)
            assert earlier["pending"] == 1
            assert not (spool / "config.pending").exists()
        finally:
            release_response.set()
        assert longer.result()["committed"] is True
    assert step_values(repository) == [(1, 0.1)]
    assert json.loads((spool / "delivery-status").read_text())["committed"] is True


def test_malformed_ack_retains_pending_and_safe_receipt(
    delivery, tmp_path, monkeypatch
):
    client, repository, token = delivery
    post = client.post

    def invalid_ack(url, *, json, **kwargs):
        response = post(url, json=json, **kwargs)
        assert response.status_code == 200
        return httpx2.Response(
            200,
            request=httpx2.Request("POST", url),
            json={"committed": "synthetic secret", "batch_id": json["batch_id"]},
        )

    monkeypatch.setattr(client, "post", invalid_ack)
    spool = tmp_path / "spool"
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    run.log({"loss": 0.1}, step=1)
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
        status = run.finish(timeout=0.2)
    assert status == {
        "committed": False,
        "pending": 1,
        "dropped": 0,
        "error_category": "invalid_ack",
    }
    assert step_values(repository) == [(1, 0.1)]
    assert list(spool.glob("*.json"))
    assert "secret" not in (spool / "delivery-status").read_text()


def test_scalar_batches_respect_wire_limit_and_rich_record_order(
    delivery, tmp_path, monkeypatch
):
    client, _, token = delivery
    spool = tmp_path / "spool"
    seed = CloudRun("http://testserver", token, "r", spool)
    monkeypatch.setattr(seed, "_send", lambda: None)
    large = {"🚀" * 250 + str(index): 1e100 for index in range(128)}
    for step in range(3):
        seed.log(large, step=step)
    seed.log({"preview": "rich"}, step=3)
    for step in range(4, 6):
        seed.log(large, step=step)
    seed.transport.close()
    sent = []
    post = client.post

    def capture(url, *, json, **kwargs):
        sent.append(
            [event["step"] for event in json["events"]]
            if url.endswith("/events")
            else json["step"]
        )
        return post(url, json=json, **kwargs)

    monkeypatch.setattr(client, "post", capture)
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    assert run.finish(timeout=4)["committed"] is True
    assert sent == [[0, 1], [2], 3, [4, 5]]


def test_repeat_finish_reopens_sdk_owned_transport(delivery, tmp_path, monkeypatch):
    client, repository, token = delivery
    offline = threading.Event()
    offline.set()
    tried = threading.Event()
    transports = []

    def request_handler(request):
        if offline.is_set():
            tried.set()
            return httpx2.Response(503)
        response = client.post(
            request.url.path, content=request.content, headers=dict(request.headers)
        )
        return httpx2.Response(response.status_code, content=response.content)

    def new_transport():
        transport = httpx2.Client(transport=httpx2.MockTransport(request_handler))
        transports.append(transport)
        return transport

    monkeypatch.setattr(ManagedCloudRun, "_new_transport", staticmethod(new_transport))
    run = ManagedCloudRun("http://testserver", token, "r", tmp_path / "spool")
    run.log({"loss": 0.1}, step=1)
    assert tried.wait(2)
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
        assert run.finish(timeout=0.05)["pending"] == 1
    run.worker.join(2)
    assert transports[0].is_closed
    offline.clear()
    assert run.finish(timeout=2)["committed"] is True
    run.worker.join(2)
    assert len(transports) == 2 and transports[1].is_closed
    assert step_values(repository) == [(1, 0.1)]


def test_reopen_completes_partial_acknowledged_batch_cleanup(
    delivery, tmp_path, monkeypatch
):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    queue_legacy_events(spool, [1, 2])
    last = sorted(spool.glob("*.json"))[-1]
    unlink = type(last).unlink

    def interrupted_unlink(path, *args, **kwargs):
        if path == last:
            raise OSError("synthetic spool cleanup error")
        return unlink(path, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(type(last), "unlink", interrupted_unlink)
        run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
        with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
            status = run.finish(timeout=0.2)
        assert status["pending"] == 1 and status["error_category"] == "spool"
        run.worker.join(2)
    assert list(spool.glob("*.json")) == [last]
    assert step_values(repository) == [(1, 0.1), (2, 0.2)]

    def forbid_repost(*args, **kwargs):
        pytest.fail("cleanup of an acknowledged batch sent another request")

    monkeypatch.setattr(client, "post", forbid_repost)
    recovered = ManagedCloudRun(
        "http://testserver", token, "r", spool, transport=client
    )
    assert recovered.finish(timeout=2) == {
        "committed": True,
        "pending": 0,
        "dropped": 0,
    }
    assert not list(spool.glob("*.json"))
    reopened = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    assert reopened.finish(timeout=0) == {"committed": True, "pending": 0, "dropped": 0}


def test_gap_receipt_survives_process_exit_before_finish(tmp_path):
    spool = tmp_path / "spool"
    produced = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os
import sys
from trackio.cloud_client import ManagedCloudRun
run = ManagedCloudRun("http://testserver", "token", "r", sys.argv[1], max_bytes=512)
run.log({"long-" + "x" * 225: 1}, step=1)
assert run.delivery_status["dropped"] == 1
os._exit(0)
""",
            str(spool),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "TRACKIO_METRICS_GAP" in produced.stderr
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import json
import sys
from trackio.cloud_client import ManagedCloudRun
run = ManagedCloudRun("http://testserver", "token", "r", sys.argv[1], max_bytes=512)
print(json.dumps(run.finish(timeout=0)))
""",
            str(spool),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) == {
        "committed": False,
        "pending": 0,
        "dropped": 1,
        "error_category": "dropped",
    }


def test_admitted_singleton_drains_from_a_512_byte_spool(delivery, tmp_path):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    run = ManagedCloudRun(
        "http://testserver", token, "r", spool, max_bytes=512, transport=client
    )
    run.log({"loss": 1}, step=1)
    assert run.finish(timeout=0.4) == {"committed": True, "pending": 0, "dropped": 0}
    assert step_values(repository) == [(1, 1)]
    assert sum(path.stat().st_size for path in spool.iterdir()) <= 512


def test_finish_deadline_does_not_wait_for_spool_collection(
    delivery, tmp_path, monkeypatch
):
    client, _, token = delivery
    spool = tmp_path / "spool"
    queue_legacy_events(spool, range(1, 4097))
    collecting = threading.Event()
    release = threading.Event()
    original = ManagedCloudRun._collect_spool_objects

    def blocked_collection(run):
        collecting.set()
        assert release.wait(5)
        original(run)

    monkeypatch.setattr(ManagedCloudRun, "_collect_spool_objects", blocked_collection)
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    try:
        assert collecting.wait(2)
        with ThreadPoolExecutor(max_workers=1) as caller:

            def bounded_finish():
                with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
                    return run.finish(timeout=0.02)

            attempt = caller.submit(bounded_finish)
            try:
                assert attempt.result(timeout=0.25) == {
                    "committed": False,
                    "pending": 4096,
                    "dropped": 0,
                    "error_category": "timeout",
                }
            finally:
                release.set()
    finally:
        release.set()
        run.worker.join(2)


def test_finish_never_commits_while_monitor_stop_outcome_is_pending(delivery, tmp_path):
    client, _, token = delivery
    entered = threading.Event()
    release = threading.Event()

    class Monitor:
        def stop(self, timeout):
            entered.set()
            assert release.wait(5)
            raise RuntimeError("synthetic monitor stop failure")

    run = ManagedCloudRun(
        "http://testserver", token, "r", tmp_path / "spool", transport=client
    )
    run.monitors.append(Monitor())
    with (
        ThreadPoolExecutor(max_workers=1) as caller,
        warnings.catch_warnings(record=True),
    ):
        first = caller.submit(run.finish, timeout=2)
        try:
            assert entered.wait(2)
            earlier = run.finish(timeout=0.02)
            assert earlier == {
                "committed": False,
                "pending": 0,
                "dropped": 0,
                "error_category": "timeout",
            }
        finally:
            release.set()
        assert first.result() == {
            "committed": False,
            "pending": 0,
            "dropped": 1,
            "error_category": "dropped",
        }


def test_new_scalar_keys_match_service_validation(delivery, tmp_path):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_GAP"):
        run.log({"bad\nkey": 2}, step=2)
    run.log({"loss": 0.1}, step=1)
    status = run.finish(timeout=1)
    assert status == {
        "committed": False,
        "pending": 0,
        "dropped": 1,
        "error_category": "dropped",
    }
    assert step_values(repository) == [(1, 0.1)]


def test_invalid_legacy_key_cannot_poison_earlier_valid_events(
    delivery, tmp_path, monkeypatch
):
    client, repository, token = delivery
    spool = tmp_path / "spool"
    queue_legacy_events(spool, [1, 2, 3])
    invalid_path = sorted(spool.glob("*.json"))[-1]
    payload = json.loads(invalid_path.read_text())
    payload["events"][0]["metrics"] = {"bad\nkey": 0.3}
    invalid_path.write_text(json.dumps(payload))
    post = client.post

    def request_handler(request):
        response = post(
            request.url.path, content=request.content, headers=dict(request.headers)
        )
        return httpx2.Response(response.status_code, content=response.content)

    with httpx2.Client(transport=httpx2.MockTransport(request_handler)) as transport:
        run = ManagedCloudRun(
            "http://testserver", token, "r", spool, transport=transport
        )
        with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_DELAYED"):
            status = run.finish(timeout=0.4)
        run.worker.join(2)
    assert status["pending"] == 1 and status["error_category"] == "rejected"
    assert step_values(repository) == [(1, 0.1), (2, 0.2)]
    assert [json.loads(path.read_text()) for path in spool.glob("*.json")] == [payload]


def test_finish_reports_unsettled_admission_until_durable_write_returns(
    delivery, tmp_path, monkeypatch
):
    client, repository, token = delivery
    writing = threading.Event()
    release = threading.Event()
    run = ManagedCloudRun(
        "http://testserver", token, "r", tmp_path / "spool", transport=client
    )
    write = run._write_file

    def hold_write(path, payload):
        if path.suffix == ".json":
            writing.set()
            assert release.wait(2)
        return write(path, payload)

    monkeypatch.setattr(run, "_write_file", hold_write)
    with ThreadPoolExecutor(max_workers=1) as caller:
        admitted = caller.submit(run.log, {"loss": 0.1}, step=1)
        try:
            assert writing.wait(2)
            assert run.finish(timeout=0.02) == {
                "committed": False,
                "pending": 0,
                "dropped": 0,
                "error_category": "timeout",
            }
        finally:
            release.set()
        admitted.result()
    assert run.finish(timeout=1) == {"committed": True, "pending": 0, "dropped": 0}
    assert step_values(repository) == [(1, 0.1)]


def test_finish_deadline_includes_a_stalled_receipt_write(
    delivery, tmp_path, monkeypatch
):
    client, _, token = delivery
    writing_receipt = threading.Event()
    release = threading.Event()
    spool = tmp_path / "spool"
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    write = run._write_file

    def hold_receipt(path, payload):
        if path.name == "delivery-status":
            writing_receipt.set()
            assert release.wait(2)
        return write(path, payload)

    monkeypatch.setattr(run, "_write_file", hold_receipt)
    with ThreadPoolExecutor(max_workers=1) as caller:
        finish = caller.submit(run.finish, timeout=0.03)
        try:
            assert writing_receipt.wait(2)
            assert finish.result(timeout=0.25) == {
                "committed": False,
                "pending": 0,
                "dropped": 0,
                "error_category": "timeout",
            }
            assert not (spool / "delivery-status").exists()
        finally:
            release.set()
    assert run.finish(timeout=1) == {"committed": True, "pending": 0, "dropped": 0}
    assert json.loads((spool / "delivery-status").read_text())["committed"] is True


def test_older_finish_cannot_claim_an_unsaved_newer_receipt(
    delivery, tmp_path, monkeypatch
):
    client, _, token = delivery
    paused = threading.Event()
    release = threading.Event()
    spool = tmp_path / "spool"
    run = ManagedCloudRun("http://testserver", token, "r", spool, transport=client)
    persist = run._persist_receipt
    write = run._write_file

    def await_newer_status(status, sequence, deadline):
        if sequence == 1:
            paused.set()
            assert release.wait(2)
        return persist(status, sequence, deadline)

    def fail_receipt_write(path, payload):
        if path.name == "delivery-status":
            raise OSError("synthetic disk error")
        return write(path, payload)

    monkeypatch.setattr(run, "_persist_receipt", await_newer_status)
    monkeypatch.setattr(run, "_write_file", fail_receipt_write)
    with ThreadPoolExecutor(max_workers=1) as caller:
        older = caller.submit(run.finish, timeout=1)
        try:
            assert paused.wait(2)
            with pytest.warns(RuntimeWarning, match="TRACKIO_METRICS_GAP"):
                newer = run.finish(timeout=0.3)
            assert newer["committed"] is False and newer["error_category"] == "spool"
        finally:
            release.set()
        assert older.result()["committed"] is False
    assert not (spool / "delivery-status").exists()
