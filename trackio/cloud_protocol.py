"""Dependency-free canonical content identities for cloud clients and services."""

import hashlib
import json

SCOPE_FIELDS = ("workspace_id", "project_id", "job_id", "run_id")


def canonical(value):
    def normalized(item):
        if isinstance(item, dict):
            return {key: normalized(child) for key, child in item.items()}
        if isinstance(item, list):
            return [normalized(child) for child in item]
        if type(item) is float and item.is_integer():
            return int(item)
        return item

    return json.dumps(
        normalized(value), sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def content_digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()
