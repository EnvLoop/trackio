import ssl
import sys

import pytest

from trackio.cloud_server import tls_options


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
