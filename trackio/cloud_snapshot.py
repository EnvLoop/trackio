"""Offline, bounded snapshots of legacy SQLite databases and accompanying files."""

import base64
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
from contextlib import closing
from pathlib import Path, PurePosixPath

FORMAT_VERSION = 1
DEFAULT_MAX_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_FILES = 10000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def relative_path(value):
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in value
        or str(path) != value
    ):
        raise ValueError("INVALID_SNAPSHOT_PATH")
    return path


def file_digest(path, max_bytes=DEFAULT_MAX_BYTES):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    digest = hashlib.sha256()
    size = 0
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("SNAPSHOT_REGULAR_FILE_REQUIRED")
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
            if size > max_bytes:
                raise ValueError("SNAPSHOT_SIZE_LIMIT")
            digest.update(chunk)
    return {"digest": digest.hexdigest(), "size": size}


def _inventory(root, max_files):
    result = {}
    for directory, directories, files in os.walk(root, followlinks=False):
        for name in sorted(directories + files):
            path = Path(directory) / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise ValueError("SNAPSHOT_SYMLINK_REJECTED")
            if not stat.S_ISDIR(metadata.st_mode) and not stat.S_ISREG(
                metadata.st_mode
            ):
                raise ValueError("SNAPSHOT_REGULAR_FILE_REQUIRED")
        for name in sorted(files):
            path = Path(directory) / name
            result[path.relative_to(root).as_posix()] = path
            if len(result) > max_files:
                raise ValueError("SNAPSHOT_FILE_LIMIT")
    return result


def _open_database(path):
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA trusted_schema=OFF")
    return connection


def _quoted(value):
    return '"' + value.replace('"', '""') + '"'


def _table_rows(connection, table):
    columns = [
        row[1]
        for row in connection.execute("PRAGMA table_info(" + _quoted(table) + ")")
    ]
    if not columns:
        raise ValueError("SNAPSHOT_TABLE_HAS_NO_COLUMNS")
    ordering = ",".join(_quoted(column) for column in columns)
    for row in connection.execute(
        "SELECT * FROM " + _quoted(table) + " ORDER BY " + ordering
    ):
        yield dict(zip(columns, row, strict=True))


def _database_manifest(path):
    with closing(_open_database(path)) as connection:
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("SNAPSHOT_DATABASE_INTEGRITY")
        tables = {}
        for name, sql in connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ):
            digest = hashlib.sha256()
            count = 0
            for row in _table_rows(connection, name):
                encoded = {
                    key: {"bytes": base64.b64encode(value).decode()}
                    if isinstance(value, bytes)
                    else value
                    for key, value in row.items()
                }
                digest.update((canonical(encoded) + "\n").encode())
                count += 1
            tables[name] = {"rows": count, "digest": digest.hexdigest(), "sql": sql}
        return {
            "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
            "tables": tables,
        }


def capture(
    source,
    destination,
    *,
    writers_stopped,
    max_bytes=DEFAULT_MAX_BYTES,
    max_files=DEFAULT_MAX_FILES,
):
    if writers_stopped is not True:
        raise ValueError("WRITERS_MUST_BE_STOPPED")
    source, destination = Path(source).resolve(), Path(destination).absolute()
    if (
        not source.is_dir()
        or destination.exists()
        or source == destination
        or source in destination.parents
    ):
        raise ValueError("INVALID_SNAPSHOT_DESTINATION")
    if (
        type(max_bytes) is not int
        or max_bytes <= 0
        or type(max_files) is not int
        or max_files <= 0
    ):
        raise ValueError("INVALID_SNAPSHOT_LIMIT")
    inventory = _inventory(source, max_files)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(prefix=".trackio-snapshot-", dir=destination.parent)
    )
    try:
        output = temporary / "files"
        output.mkdir(mode=0o700)
        files, databases = {}, {}
        total = 0
        database_names = {
            name
            for name, path in inventory.items()
            if path.suffix in (".db", ".sqlite", ".sqlite3")
        }
        skipped = {
            name + suffix
            for name in database_names
            for suffix in ("-wal", "-shm", "-journal")
        }
        volatile = {name + "-shm" for name in database_names}
        before = {
            name: file_digest(path, max_bytes)
            for name, path in inventory.items()
            if name not in volatile
        }
        if sum(value["size"] for value in before.values()) > max_bytes:
            raise ValueError("SNAPSHOT_SIZE_LIMIT")
        database_input = temporary / "sqlite-input"
        database_input.mkdir(mode=0o700)
        for name, path in sorted(inventory.items()):
            if name in skipped:
                continue
            relative_path(name)
            target = output / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if name in database_names:
                copied_source = database_input / name
                copied_source.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                for suffix in ("", "-wal", "-journal"):
                    source_name = name + suffix
                    if source_name in inventory:
                        source_path = inventory[source_name]
                        descriptor = os.open(source_path, os.O_RDONLY | os.O_NOFOLLOW)
                        with (
                            os.fdopen(descriptor, "rb") as reader,
                            Path(str(copied_source) + suffix).open("xb") as writer,
                        ):
                            remaining = before[source_name]["size"]
                            while remaining:
                                chunk = reader.read(min(1024 * 1024, remaining))
                                if not chunk:
                                    raise ValueError("SNAPSHOT_SOURCE_CHANGED")
                                writer.write(chunk)
                                remaining -= len(chunk)
                            if reader.read(1):
                                raise ValueError("SNAPSHOT_SOURCE_CHANGED")
                        if (
                            file_digest(Path(str(copied_source) + suffix), max_bytes)
                            != before[source_name]
                        ):
                            raise ValueError("SNAPSHOT_SOURCE_CHANGED")
                with (
                    closing(_open_database(copied_source)) as reader,
                    closing(sqlite3.connect(target)) as writer,
                ):
                    reader.backup(writer)
                    writer.execute("PRAGMA journal_mode=DELETE")
                databases[name] = _database_manifest(target)
            else:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(descriptor, "rb") as reader, target.open("xb") as writer:
                    copied = 0
                    while chunk := reader.read(min(1024 * 1024, max_bytes + 1)):
                        copied += len(chunk)
                        if total + copied > max_bytes:
                            raise ValueError("SNAPSHOT_SIZE_LIMIT")
                        writer.write(chunk)
            target.chmod(0o600)
            files[name] = file_digest(target, max_bytes - total)
            total += files[name]["size"]
            with target.open("rb") as stream:
                os.fsync(stream.fileno())
        after_inventory = _inventory(source, max_files)
        if set(after_inventory) - volatile != set(inventory) - volatile:
            raise ValueError("SNAPSHOT_SOURCE_CHANGED")
        if before != {
            name: file_digest(path, max_bytes)
            for name, path in after_inventory.items()
            if name not in volatile
        }:
            raise ValueError("SNAPSHOT_SOURCE_CHANGED")
        manifest = {
            "format_version": FORMAT_VERSION,
            "files": files,
            "databases": databases,
            "bytes": total,
            "sqlite_sidecars": sorted(set(inventory) & skipped),
        }
        shutil.rmtree(database_input)
        manifest["digest"] = hashlib.sha256(canonical(manifest).encode()).hexdigest()
        manifest_path = temporary / "manifest.json"
        manifest_path.write_text(canonical(manifest))
        manifest_path.chmod(0o600)
        with manifest_path.open("rb") as stream:
            os.fsync(stream.fileno())
        os.rename(temporary, destination)
        return manifest
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def verify(snapshot, *, max_bytes=DEFAULT_MAX_BYTES, max_files=DEFAULT_MAX_FILES):
    root = Path(snapshot)
    manifest = json.loads((root / "manifest.json").read_text())
    content = {key: value for key, value in manifest.items() if key != "digest"}
    if manifest.get("format_version") != FORMAT_VERSION or hashlib.sha256(
        canonical(content).encode()
    ).hexdigest() != manifest.get("digest"):
        raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
    inventory = _inventory(root / "files", max_files)
    if set(inventory) != set(manifest["files"]) or manifest["bytes"] > max_bytes:
        raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
    total = 0
    for name, expected in manifest["files"].items():
        relative_path(name)
        total += expected["size"]
        if total > max_bytes or file_digest(inventory[name], max_bytes) != expected:
            raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
    if total != manifest["bytes"]:
        raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
    for name, expected in manifest["databases"].items():
        relative_path(name)
        if name not in inventory or _database_manifest(inventory[name]) != expected:
            raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
    return manifest


def rows(snapshot, database, table):
    manifest = verify(snapshot)
    if (
        database not in manifest["databases"]
        or table not in manifest["databases"][database]["tables"]
    ):
        raise ValueError("UNKNOWN_SNAPSHOT_TABLE")
    with closing(_open_database(Path(snapshot) / "files" / database)) as connection:
        yield from _table_rows(connection, table)
