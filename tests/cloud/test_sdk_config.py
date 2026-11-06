import json

import httpx2

from trackio.cloud_client import CloudRun


def test_native_trainer_direct_config_assignment_cannot_leak_unapproved_fields(
    tmp_path,
):
    requests = []

    def response(request):
        requests.append(request)
        return httpx2.Response(200, json={"committed": True})

    with httpx2.Client(transport=httpx2.MockTransport(response)) as transport:
        run = CloudRun(
            "http://127.0.0.1", "t" * 32, "r", tmp_path / "spool", transport=transport
        )
        run.config["api_key"] = "private-value"
        run.config["model/num_parameters"] = 1234
        run.config.update({"lr": 0.1})
        payload = json.loads(requests[-1].content)
        assert payload == {"lr": 0.1, "model/num_parameters": 1234}
        assert "private-value" not in json.dumps(dict(run.config))
        run.finish()
