"""Scoped content-addressed attachments, independent of database credentials."""

import hashlib
import os
import re
import stat
import uuid
from pathlib import Path
from typing import Protocol

MAX_OBJECT_BYTES = 32 * 1024 * 1024


class Objects(Protocol):
    def put(self, key: str, payload: bytes, digest: str, content_type: str) -> None: ...

    def get(self, key: str, digest: str, size: int) -> bytes: ...

    def delete(self, key: str) -> None: ...


def object_key(key):
    if not isinstance(key, str) or not re.fullmatch(r"[a-f0-9]{64}/[a-f0-9]{64}", key):
        raise ValueError("INVALID_OBJECT_KEY")
    return key


def verify(payload, digest, size=None):
    if len(payload) > MAX_OBJECT_BYTES:
        raise ValueError("OBJECT_TOO_LARGE")
    if hashlib.sha256(payload).hexdigest() != digest:
        raise ValueError(
            "OBJECT_DIGEST_MISMATCH" if size is None else "OBJECT_INTEGRITY_FAILURE"
        )
    if size is not None and len(payload) != size:
        raise ValueError("OBJECT_INTEGRITY_FAILURE")


def validate_size(size):
    if type(size) is not int or not 0 <= size <= MAX_OBJECT_BYTES:
        raise ValueError("INVALID_OBJECT_SIZE")


class FileObjects:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._private_directory(self.directory)

    @staticmethod
    def _private_directory(path):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("PRIVATE_OBJECT_DIRECTORY_REQUIRED")

    def _path(self, key, *, create=False):
        path = self.directory / object_key(key)
        if create:
            path.parent.mkdir(mode=0o700, exist_ok=True)
        if path.parent.exists() or path.parent.is_symlink():
            self._private_directory(path.parent)
        return path

    def put(self, key, payload, digest, content_type):
        verify(payload, digest)
        if object_key(key).split("/")[1] != digest:
            raise ValueError("OBJECT_DIGEST_MISMATCH")
        path = self._path(key, create=True)
        temporary = path.with_name(uuid.uuid4().hex + ".pending")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def get(self, key, digest, size):
        validate_size(size)
        path = self._path(key)
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            payload = stream.read(min(MAX_OBJECT_BYTES, size) + 1)
        verify(payload, digest, size)
        return payload

    def delete(self, key):
        self._path(key).unlink(missing_ok=True)


class S3Objects:
    def __init__(self, client, bucket, prefix):
        if (
            not bucket
            or not prefix
            or not prefix.endswith("/")
            or prefix.startswith("/")
            or ".." in prefix.split("/")
            or not re.fullmatch(r"[a-zA-Z0-9/_-]+", prefix)
        ):
            raise ValueError("EXPLICIT_OBJECT_PREFIX_REQUIRED")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix

    def _arguments(self, key):
        return {"Bucket": self.bucket, "Key": self.prefix + object_key(key)}

    def put(self, key, payload, digest, content_type):
        verify(payload, digest)
        if object_key(key).split("/")[1] != digest:
            raise ValueError("OBJECT_DIGEST_MISMATCH")
        arguments = self._arguments(key)
        self.client.put_object(
            **arguments,
            Body=payload,
            ContentType=content_type,
            Metadata={"sha256": digest},
        )
        stored = self.client.head_object(**arguments)
        if (
            stored.get("ContentLength") != len(payload)
            or stored.get("Metadata", {}).get("sha256") != digest
        ):
            raise ValueError("OBJECT_UPLOAD_NOT_VERIFIED")

    def get(self, key, digest, size):
        validate_size(size)
        stream = self.client.get_object(**self._arguments(key))["Body"]
        try:
            payload = stream.read(min(MAX_OBJECT_BYTES, size) + 1)
        finally:
            stream.close()
        verify(payload, digest, size)
        return payload

    def delete(self, key):
        self.client.delete_object(**self._arguments(key))
