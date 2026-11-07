import hashlib
import io
import stat

import pytest


def test_filesystem_objects_are_private_immutable_and_verified_on_read(tmp_path):
    from trackio.cloud_objects import FileObjects

    root = tmp_path / "objects"
    store = FileObjects(root)
    payload = b"synthetic-media"
    digest = hashlib.sha256(payload).hexdigest()
    key = "a" * 64 + "/" + digest
    store.put(key, payload, digest, "application/octet-stream")
    store.put(key, payload, digest, "application/octet-stream")
    assert store.get(key, digest, len(payload)) == payload
    assert stat.S_IMODE((root / key).stat().st_mode) == 0o600
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    with pytest.raises(ValueError, match="INVALID_OBJECT_KEY"):
        store.get("../secret", digest, len(payload))
    with pytest.raises(ValueError, match="OBJECT_DIGEST_MISMATCH"):
        store.put(key, b"wrong", digest, "application/octet-stream")
    (root / key).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="OBJECT_INTEGRITY_FAILURE"):
        store.get(key, digest, len(payload))
    store.delete(key)
    store.delete(key)


def test_local_object_backend_rejects_symlinks_and_public_directories(tmp_path):
    from trackio.cloud_objects import FileObjects

    public = tmp_path / "public"
    public.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="PRIVATE_OBJECT_DIRECTORY_REQUIRED"):
        FileObjects(public)
    link = tmp_path / "link"
    link.symlink_to(public, target_is_directory=True)
    with pytest.raises(ValueError, match="PRIVATE_OBJECT_DIRECTORY_REQUIRED"):
        FileObjects(link)


def test_s3_content_is_read_back_and_verified_without_accepting_keys_or_urls_from_clients():
    from trackio.cloud_objects import S3Objects

    class Storage:
        def put_object(self, **kwargs):
            self.row = kwargs
            return {"ETag": "provider-opaque"}

        def head_object(self, **kwargs):
            assert kwargs == {"Bucket": "synthetic", "Key": self.row["Key"]}
            return {
                "ContentLength": len(self.row["Body"]),
                "Metadata": self.row["Metadata"],
            }

        def get_object(self, **kwargs):
            assert kwargs == {"Bucket": "synthetic", "Key": self.row["Key"]}
            return {"Body": io.BytesIO(self.row["Body"])}

        def delete_object(self, **kwargs):
            self.deleted = kwargs

    storage = Storage()
    objects = S3Objects(storage, "synthetic", "staging/trackio/")
    payload = b"attachment"
    digest = hashlib.sha256(payload).hexdigest()
    key = "a" * 64 + "/" + digest
    objects.put(key, payload, digest, "application/octet-stream")
    assert storage.row["Key"] == "staging/trackio/" + key
    assert objects.get(key, digest, len(payload)) == payload
    storage.row["Body"] = b"corrupt"
    with pytest.raises(ValueError, match="OBJECT_INTEGRITY_FAILURE"):
        objects.get(key, digest, len(payload))
    objects.delete(key)
    assert storage.deleted["Key"] == "staging/trackio/" + key


def test_s3_forged_head_cannot_acknowledge_corrupted_stored_body():
    from trackio.cloud_objects import S3Objects

    class Storage:
        def put_object(self, **values):
            self.values = values

        def head_object(self, **values):
            return {
                "ContentLength": len(self.values["Body"]),
                "Metadata": self.values["Metadata"],
            }

        def get_object(self, **values):
            self.body = io.BytesIO(b"X" * len(self.values["Body"]))
            return {"Body": self.body}

    storage = Storage()
    objects = S3Objects(storage, "synthetic", "staging/trackio/")
    payload = b"attachment"
    digest = hashlib.sha256(payload).hexdigest()
    with pytest.raises(ValueError, match="OBJECT_INTEGRITY_FAILURE"):
        objects.put(
            "a" * 64 + "/" + digest, payload, digest, "application/octet-stream"
        )
    assert storage.body.closed
