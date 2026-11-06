"""Read-only deployment compatibility probe with fixed diagnostic output."""

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy.engine import make_url

from trackio.cloud import (
    SCHEMA_VERSION,
    CloudConfig,
    MetricsRepository,
    configured_objects,
)


def check(filename, *, require_objects=False):
    configuration = CloudConfig.from_file(filename)
    if configuration.database_url.startswith("sqlite:///"):
        if not Path(make_url(configuration.database_url).database).is_file():
            raise ValueError("DATABASE_NOT_INITIALIZED")
    repository = MetricsRepository(
        configuration.database_url,
        initialize=False,
        schema=configuration.database_schema,
    )
    try:
        objects = configured_objects(configuration)
        if require_objects and objects is None:
            raise ValueError("OBJECT_STORAGE_REQUIRED")
        return dict(
            status="ready",
            api_version=1,
            schema_version=SCHEMA_VERSION,
            objects_configured=objects is not None,
        )
    finally:
        repository.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Verify initialized Trackio storage without DDL"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--require-objects", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = check(args.config, require_objects=args.require_objects)
    except Exception:
        print("TRACKIO_PREFLIGHT_FAILED", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
