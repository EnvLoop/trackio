import json
import subprocess
import sys

from sqlalchemy import inspect

from trackio.cloud import MetricsRepository


def test_deployment_probe_reads_exact_schema_without_initialization_or_secret_output(
    tmp_path,
):
    url = f"sqlite:///{tmp_path / 'probe.sqlite'}"
    repository = MetricsRepository(url)
    for name in ("service", "signing"):
        path = tmp_path / name
        path.write_text(name * 32)
        path.chmod(0o600)
    path = tmp_path / "server.toml"
    path.write_text(
        f'environment="local"\ndatabase_url="{url}"\nservice_token_file="service"\nsigning_key_file="signing"\n'
    )
    path.chmod(0o600)
    command = [sys.executable, "-m", "trackio.cloud_probe", "--config", str(path)]
    result = subprocess.run(command, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["schema_version"] == 3
    assert str(tmp_path) not in result.stdout and "signingsigning" not in result.stdout
    with repository.engine.begin() as connection:
        repository.objects.drop(connection)
    rejected = subprocess.run(command, text=True, capture_output=True)
    assert rejected.returncode == 1
    assert rejected.stderr.strip() == "TRACKIO_PREFLIGHT_FAILED"
    assert not inspect(repository.engine).has_table("trackio_cloud_objects")
    repository.close()
