import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


@pytest.fixture
def postgres_database():
    url = Path(os.environ["TRACKIO_TEST_DATABASE_FILE"]).read_text().strip()
    parsed = make_url(url)
    assert parsed.host == "127.0.0.1" and parsed.database.startswith("trackio_test_")
    schema = "trackio_test_" + uuid4().hex[:16]
    try:
        yield url, schema
    finally:
        engine = create_engine(url, hide_parameters=True)
        try:
            with engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            engine.dispose()


@pytest.fixture(params=["sqlite", "postgres"])
def workspace_database(request, tmp_path):
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'workspace.sqlite'}", None
    else:
        yield request.getfixturevalue("postgres_database")
