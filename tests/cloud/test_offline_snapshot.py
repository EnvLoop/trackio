import hashlib
import importlib
import importlib.util
import json
import sqlite3

import pytest


def snapshot_module():
    assert importlib.util.find_spec("trackio.cloud_snapshot") is not None, (
        "Complete offline snapshot API is missing"
    )
    return importlib.import_module("trackio.cloud_snapshot")


def test_snapshot_preserves_wal_tables_and_all_file_families_offline(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    database = sqlite3.connect(source / "legacy.db")
    database.execute("PRAGMA journal_mode=WAL")
    database.execute(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, name TEXT, config TEXT)"
    )
    database.execute(
        "CREATE TABLE metrics (log_id TEXT PRIMARY KEY, run_id TEXT, metrics TEXT)"
    )
    database.execute("INSERT INTO runs VALUES ('historical', 'display', '{}')")
    database.execute("INSERT INTO metrics VALUES ('e', 'historical', '{\"loss\":0.5}')")
    database.commit()
    for name in (
        "media/p/image.png",
        "artifacts/p/blobs/sha256/ab/blob",
        "traces/session.json",
        "inbox/p/pending.jsonl",
        ".trackio/logbook/page.md",
        "metadata.json",
    ):
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(name.encode())
    module = snapshot_module()
    snapshot = module.capture(source, tmp_path / "snapshot", writers_stopped=True)
    digest = snapshot["digest"]
    assert len(digest) == 64
    assert snapshot["databases"]["legacy.db"]["tables"]["metrics"]["rows"] == 1
    assert len(snapshot["files"]) == 7
    database.close()
    source.rename(tmp_path / "offline-source")
    verified = module.verify(tmp_path / "snapshot")
    assert verified == snapshot
    rows = list(module.rows(tmp_path / "snapshot", "legacy.db", "metrics"))
    assert rows[0]["log_id"] == "e"
    assert json.loads(rows[0]["metrics"]) == {"loss": 0.5}
    path = tmp_path / "snapshot" / "files" / "media/p/image.png"
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SNAPSHOT_INTEGRITY"):
        module.verify(tmp_path / "snapshot")
    assert (
        hashlib.sha256(b"corrupt").hexdigest()
        != snapshot["files"]["media/p/image.png"]["digest"]
    )


def test_snapshot_requires_freeze_and_rejects_symlinks_and_limits(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    module = snapshot_module()
    with pytest.raises(ValueError, match="WRITERS_MUST_BE_STOPPED"):
        module.capture(source, tmp_path / "snapshot", writers_stopped=False)
    (source / "link").symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="SYMLINK"):
        module.capture(source, tmp_path / "snapshot", writers_stopped=True)
    (source / "link").unlink()
    (source / "large.bin").write_bytes(b"x" * 16)
    with pytest.raises(ValueError, match="SNAPSHOT_SIZE_LIMIT"):
        module.capture(source, tmp_path / "snapshot", writers_stopped=True, max_bytes=8)


def test_snapshot_detects_wal_writer_after_backup(tmp_path, monkeypatch):
    module = snapshot_module()
    source = tmp_path / "source"
    source.mkdir()
    database = sqlite3.connect(source / "legacy.db")
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("CREATE TABLE metrics (value INTEGER)")
    database.execute("INSERT INTO metrics VALUES (1)")
    database.commit()
    original = module._database_manifest

    def concurrent_writer(path):
        result = original(path)
        database.execute("INSERT INTO metrics VALUES (2)")
        database.commit()
        return result

    monkeypatch.setattr(module, "_database_manifest", concurrent_writer)
    try:
        with pytest.raises(ValueError, match="SNAPSHOT_SOURCE_CHANGED"):
            module.capture(source, tmp_path / "snapshot", writers_stopped=True)
    finally:
        database.close()
