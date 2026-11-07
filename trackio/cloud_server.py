"""Private TLS configuration shared by the service and container health check."""

import json
import ssl
import stat
import sys
import urllib.request
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def private_file(path):
    path = Path(path)
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError("PRIVATE_TLS_FILE_REQUIRED")
    return path


def tls_options(filename):
    filename = private_file(filename)
    configuration = tomllib.loads(filename.read_text())
    certificate = configuration.get("tls_cert_file")
    key = configuration.get("tls_key_file")
    if not certificate and not key and configuration.get("environment") == "local":
        return {}
    if not certificate or not key:
        raise ValueError("TRACKIO_TLS_REQUIRED")
    certificate = private_file(filename.parent / certificate)
    key = private_file(filename.parent / key)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certificate), str(key))
    return {"ssl_certfile": str(certificate), "ssl_keyfile": str(key)}


def ready(filename="/run/trackio/server.toml", port=18570):
    filename = private_file(filename)
    configuration = tomllib.loads(filename.read_text())
    options = tls_options(filename)
    context = None
    if options:
        authority = private_file(filename.parent / configuration["tls_ca_file"])
        context = ssl.create_default_context(cafile=str(authority))
    address = ("https" if options else "http") + "://localhost:" + str(port) + "/ready"
    handler = urllib.request.HTTPSHandler(context=context)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), handler)
    with opener.open(address, timeout=3) as response:
        if response.status != 200 or json.load(response).get("status") != "ready":
            raise ValueError("TRACKIO_NOT_READY")


if __name__ == "__main__":
    try:
        ready()
    except Exception:
        raise SystemExit(1) from None
