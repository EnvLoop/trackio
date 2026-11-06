import json
import subprocess
import sys

import pytest
from sqlalchemy import select

import trackio
from trackio.cloud import MetricsRepository
from trackio.cloud_client import CloudRun


def test_public_cli_migrates_v2_preserving_cutoff(tmp_path):
    url = f"sqlite:///{tmp_path / 'v2.sqlite'}"
    repository = MetricsRepository(url)
    with repository.engine.begin() as connection:
        repository.records.drop(connection)
        repository.objects.drop(connection)
        connection.execute(repository.schema.update().values(version=2))
        connection.execute(
            repository.migrations.update().values(version=2, legacy_tokens_until=12345)
        )
    repository.close()
    for name in ("service", "signing"):
        secret = tmp_path / name
        secret.write_text(name * 32)
        secret.chmod(0o600)
    config = tmp_path / "server.toml"
    config.write_text(
        f'environment="local"\ndatabase_url="{url}"\n'
        'service_token_file="service"\nsigning_key_file="signing"\n'
    )
    config.chmod(0o600)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "trackio.cloud",
            "--config",
            str(config),
            "--migrate-from",
            "2",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    reopened = MetricsRepository(url, initialize=False)
    with reopened.engine.connect() as connection:
        assert (
            connection.execute(
                select(reopened.migrations.c.legacy_tokens_until).where(
                    reopened.migrations.c.version == 3
                )
            ).scalar_one()
            == 12345
        )
    reopened.close()


def test_scalar_admission_counts_pending_media(tmp_path, monkeypatch):
    run = CloudRun("http://localhost", "token", "r", tmp_path / "spool", max_bytes=4096)
    monkeypatch.setattr(run, "_send", lambda: None)
    run.log({"preview": trackio.Html("A" * 1000, inject=False)}, step=0)
    for step in range(100):
        try:
            run.log({"loss": 0.5}, step=step)
        except BufferError:
            break
        assert run._spool_size() <= run.max_bytes
    else:
        pytest.fail("spool admission never rejected")
    run.transport.close()


def test_manifest_failure_reclaims_only_unreferenced_blobs(tmp_path, monkeypatch):
    run = CloudRun("http://localhost", "token", "r", tmp_path / "spool", max_bytes=8192)
    monkeypatch.setattr(run, "_send", lambda: None)
    run.log({"preview": trackio.Html("shared", inject=False)}, step=0)
    manifest = next(run.directory.glob("*.json"))
    shared = json.loads(manifest.read_text())["objects"][0]["digest"]
    original = run._write

    def fail_manifest(path, payload):
        if path.suffix == ".json":
            raise OSError("synthetic manifest failure")
        original(path, payload)

    monkeypatch.setattr(run, "_write", fail_manifest)
    with pytest.raises(OSError):
        run.log(
            {
                "shared": trackio.Html("shared", inject=False),
                "new": trackio.Html("B" * 1400, inject=False),
            }
        )
    assert [path.stem for path in run.directory.glob("*.blob")] == [shared]
    monkeypatch.setattr(run, "_write", original)
    run.log({"new": trackio.Html("C" * 1400, inject=False)})
    assert len(list(run.directory.glob("*.json"))) == 2
    run.transport.close()
