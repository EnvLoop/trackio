import json
import runpy
import ssl
import sys

import pytest

from trackio.cloud_server import tls_options


def test_module_entrypoint_preserves_repository_conflict_response(
    tmp_path, monkeypatch
):
    from starlette.testclient import TestClient

    from trackio import cloud

    configuration = tmp_path / "server.toml"
    configuration.write_text(
        "\n".join(
            (
                'environment="local"',
                "database_url="
                + json.dumps("sqlite:///" + str(tmp_path / "metrics.sqlite")),
                'service_token_file="service-token"',
                'signing_key_file="signing-key"',
            )
        )
    )
    for filename, value in (("service-token", "s" * 32), ("signing-key", "k" * 32)):
        path = tmp_path / filename
        path.write_text(value)
        path.chmod(0o600)
    configuration.chmod(0o600)
    monkeypatch.setattr(sys, "argv", ["trackio.cloud", "--config", str(configuration)])
    responses = []

    def serve(application, **options):
        with TestClient(application) as client:
            binding = client.post(
                "/v1/runs",
                headers={"Authorization": "Bearer " + "s" * 32},
                json={
                    "workspace_id": "w1",
                    "project_id": "p1",
                    "job_id": "j1",
                    "run_id": "r1",
                    "config": {},
                },
            )
            assert binding.status_code == 200
            headers = {"Authorization": "Bearer " + binding.json()["token"]}
            payload = {
                "batch_id": "b1",
                "events": [
                    {
                        "event_id": "e1",
                        "writer_id": "writer1",
                        "trial_id": None,
                        "step": 1,
                        "observed_at": 100.0,
                        "category": "experiment",
                        "metrics": {"loss": 0.5},
                    }
                ],
            }
            assert (
                client.post(
                    "/v1/runs/r1/events", headers=headers, json=payload
                ).status_code
                == 200
            )
            payload["events"][0]["metrics"]["loss"] = 0.9
            response = client.post("/v1/runs/r1/events", headers=headers, json=payload)
            responses.append((response.status_code, response.json()))

    monkeypatch.setattr(cloud.uvicorn, "run", serve)
    runpy.run_path(cloud.__file__, run_name="__main__")
    assert responses == [(409, {"error": "BATCH_CONTENT_CONFLICT"})]


def test_public_server_cli_passes_validated_tls_to_uvicorn(monkeypatch):
    from trackio import cloud, cloud_server

    monkeypatch.setattr(
        sys, "argv", ["trackio.cloud", "--config", "/private/server.toml"]
    )
    monkeypatch.setattr(cloud.CloudConfig, "from_file", lambda filename: object())
    monkeypatch.setattr(cloud, "create_app", lambda config: object())
    monkeypatch.setattr(
        cloud_server,
        "tls_options",
        lambda filename: {
            "ssl_certfile": "/private/cert.pem",
            "ssl_keyfile": "/private/key.pem",
        },
    )
    calls = []
    monkeypatch.setattr(
        cloud.uvicorn, "run", lambda application, **options: calls.append(options)
    )
    cloud.main()
    assert calls[0]["ssl_certfile"] == "/private/cert.pem"
    assert calls[0]["ssl_keyfile"] == "/private/key.pem"


def test_nonlocal_server_rejects_missing_or_partial_tls(tmp_path):
    config = tmp_path / "server.toml"
    config.write_text('environment="staging"\n')
    config.chmod(0o600)
    with pytest.raises(ValueError, match="TRACKIO_TLS_REQUIRED"):
        tls_options(config)
    config.write_text('environment="local"\n')
    assert tls_options(config) == {}
    config.write_text('environment="local"\ntls_cert_file="cert.pem"\n')
    with pytest.raises(ValueError, match="TRACKIO_TLS_REQUIRED"):
        tls_options(config)


def test_tls_key_must_be_private_regular_file_and_valid_pair(tmp_path, monkeypatch):
    config = tmp_path / "server.toml"
    config.write_text(
        'environment="staging"\ntls_cert_file="cert.pem"\ntls_key_file="key.pem"\n'
    )
    config.chmod(0o600)
    certificate = tmp_path / "cert.pem"
    certificate.write_text("synthetic certificate")
    certificate.chmod(0o600)
    key = tmp_path / "key.pem"
    key.write_text("synthetic key")
    key.chmod(0o644)
    with pytest.raises(ValueError, match="PRIVATE_TLS_FILE_REQUIRED"):
        tls_options(config)
    key.chmod(0o600)
    calls = []
    monkeypatch.setattr(
        ssl.SSLContext,
        "load_cert_chain",
        lambda self, cert, private: calls.append((cert, private)),
    )
    assert tls_options(config) == {
        "ssl_certfile": str(certificate),
        "ssl_keyfile": str(key),
    }
    assert calls == [(str(certificate), str(key))]
    key.unlink()
    key.symlink_to(certificate)
    with pytest.raises(ValueError, match="PRIVATE_TLS_FILE_REQUIRED"):
        tls_options(config)
