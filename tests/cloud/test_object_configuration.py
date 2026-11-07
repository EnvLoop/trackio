import pytest

from trackio.cloud import CloudConfig, configured_objects
from trackio.cloud_objects import FileObjects


def test_local_objects_use_explicit_private_configuration(tmp_path):
    path = tmp_path / "objects.toml"
    path.write_text('backend="file"\ndirectory="objects"\n')
    path.chmod(0o600)
    config = CloudConfig(
        "local",
        "sqlite:///synthetic.sqlite",
        "s" * 32,
        "k" * 32,
        objects_config_file=str(path),
    )
    assert isinstance(configured_objects(config), FileObjects)
    path.chmod(0o644)
    with pytest.raises(ValueError, match="PRIVATE_OBJECT_CONFIG_REQUIRED"):
        configured_objects(config)


def test_cloud_objects_cannot_fall_back_to_local_files_or_another_environment(tmp_path):
    path = tmp_path / "objects.toml"
    path.write_text('backend="file"\ndirectory="objects"\n')
    path.chmod(0o600)
    config = CloudConfig(
        "staging",
        "postgresql+psycopg://synthetic",
        "s" * 32,
        "k" * 32,
        database_schema="trackio",
        objects_config_file=str(path),
    )
    with pytest.raises(ValueError, match="MANAGED_OBJECT_STORAGE_REQUIRED"):
        configured_objects(config)
    path.write_text('backend="s3"\nprefix="envplatform/prod/trackio/"\n')
    with pytest.raises(ValueError, match="ENVIRONMENT_OBJECT_PREFIX_REQUIRED"):
        configured_objects(config)
