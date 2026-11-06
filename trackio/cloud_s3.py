from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from trackio.cloud_objects import S3Objects


def s3_objects(settings, credentials):
    address = urlsplit(settings["endpoint_url"])
    if (
        address.scheme != "https"
        or not address.hostname
        or address.username
        or address.password
        or address.query
        or address.fragment
        or address.path not in ("", "/")
    ):
        raise ValueError("HTTPS_OBJECT_ENDPOINT_REQUIRED")
    client = boto3.client(
        "s3",
        endpoint_url=settings["endpoint_url"],
        region_name=settings["region"],
        aws_access_key_id=credentials["access_key_id"],
        aws_secret_access_key=credentials["secret_access_key"],
        config=Config(
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=3,
            retries={"max_attempts": 2},
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )
    return S3Objects(client, settings["bucket"], settings["prefix"])
