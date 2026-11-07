import json

import httpx2

from trackio.cloud_client import ManagedCloudRun


def test_managed_gpu_monitor_preserves_all_devices_and_stops_on_finish(
    tmp_path, monkeypatch
):
    requests = []
    stopped = []

    def response(request):
        body = json.loads(request.content)
        requests.extend(body.get("events", []))
        return httpx2.Response(
            200, json={"batch_id": body.get("batch_id"), "committed": True}
        )

    class Monitor:
        def __init__(self, run, interval):
            self.run = run
            assert interval == 10

        def start(self):
            self.run.log_system(
                {f"gpu/{index}/utilization": index for index in range(256)}
            )

        def stop(self, timeout=2):
            stopped.append(True)

    monkeypatch.setattr("trackio.gpu.gpu_available", lambda: True)
    monkeypatch.setattr("trackio.gpu.GpuMonitor", Monitor)
    monkeypatch.setattr("trackio.cpu.cpu_available", lambda: False)
    with httpx2.Client(transport=httpx2.MockTransport(response)) as transport:
        run = ManagedCloudRun(
            "http://127.0.0.1", "t" * 32, "r", tmp_path / "spool", transport=transport
        )
        try:
            run.start_system()
        finally:
            receipt = run.finish()
    assert receipt["committed"] is True
    assert stopped == [True]
    assert sum(len(event["metrics"]) for event in requests) == 256
    assert all(
        event["step"] is None and event["category"] == "compute" for event in requests
    )


def test_frozen_category_selection_does_not_write_unselected_sources(tmp_path):
    requests = []

    def response(request):
        body = json.loads(request.content)
        requests.extend(body.get("events", []))
        return httpx2.Response(
            200, json={"batch_id": body.get("batch_id"), "committed": True}
        )

    with httpx2.Client(transport=httpx2.MockTransport(response)) as transport:
        run = ManagedCloudRun(
            "http://127.0.0.1",
            "t" * 32,
            "r",
            tmp_path / "spool",
            transport=transport,
            categories=["experiment"],
        )
        run.log_system({"gpu/utilization": 50})
        run.log({"loss": 0.5}, step=7)
        run.finish()
    assert len(requests) == 1 and requests[0]["category"] == "experiment"


def test_system_monitors_start_once_without_duplicate_apple_cpu(tmp_path, monkeypatch):
    starts = []

    class Monitor:
        def __init__(self, run, **options):
            starts.append(options)

        def start(self):
            pass

        def stop(self, timeout=2):
            pass

    monkeypatch.setattr("trackio.gpu.gpu_available", lambda: False)
    monkeypatch.setattr("trackio.apple_gpu.apple_gpu_available", lambda: True)
    monkeypatch.setattr("trackio.apple_gpu.AppleGpuMonitor", Monitor)
    monkeypatch.setattr("trackio.cpu.cpu_available", lambda: True)
    monkeypatch.setattr("trackio.cpu.CpuMonitor", Monitor)
    run = ManagedCloudRun("http://127.0.0.1", "t" * 32, "r", tmp_path / "spool")
    try:
        run.start_system()
        run.start_system()
    finally:
        run.finish(timeout=0)
    assert starts == [
        {"interval": 10, "include_cpu_metrics": False},
        {"interval": 10},
    ]


def test_finish_closes_ingress_and_passes_remaining_deadline(tmp_path):
    waits = []
    run = ManagedCloudRun("http://127.0.0.1", "t" * 32, "r", tmp_path / "spool")

    class Monitor:
        def stop(self, timeout):
            waits.append(timeout)
            run.log_system({"late_gpu": 1})

    run.monitors.append(Monitor())
    run.finish(timeout=0)
    run.log({"late_loss": 1}, step=1)
    assert waits == [0]
    assert list(run.directory.glob("*.json")) == []
