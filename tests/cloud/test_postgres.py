import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from trackio.cloud import Conflict, MetricsRepository


def test_real_postgres_restart_concurrency_and_transaction_rollback():
    url = Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
    address = urlsplit(url)
    assert address.hostname == "127.0.0.1" and address.port is not None
    assert address.path.startswith("/trackio_test_")
    repository = MetricsRepository(url)
    job_id, run_id = "job_" + uuid4().hex, "run_" + uuid4().hex
    repository.bind("w1", "p1", job_id, run_id, {"model": "synthetic"})
    event = {
        "event_id": "e1",
        "writer_id": "wr1",
        "trial_id": "t1",
        "step": 7,
        "observed_at": 100,
        "category": "experiment",
        "metrics": {"loss": 0.5},
    }
    batch = {"batch_id": "b1", "events": [event]}
    with ThreadPoolExecutor(max_workers=3) as pool:
        accepted = list(
            pool.map(
                lambda _: repository.append("w1", "p1", job_id, run_id, batch), range(6)
            )
        )
    assert sum(accepted) == 1
    try:
        repository.append(
            "w1",
            "p1",
            job_id,
            run_id,
            {
                "batch_id": "b2",
                "events": [
                    {**event, "event_id": "e2"},
                    {**event, "metrics": {"loss": 9}},
                ],
            },
        )
    except Conflict:
        pass
    else:
        raise AssertionError("Expected conflict and full rollback")
    repository.close()
    reopened = MetricsRepository(url, initialize=False)
    assert len(reopened.events("w1", "p1", job_id, trial_id="t1")) == 1
    assert reopened.events("w1", "p1", job_id, trial_id="other") == []
    assert reopened.events("w2", "p1", job_id) == []
    assert reopened.events("w1", "p1", job_id)[0]["step"] == 7
    reopened.close()
