# EnvLoop scalar metrics service

This is an incremental implementation for the EnvLoop fork. It does not replace
the upstream Trackio dashboard or all W&B APIs. Cloud storage is implemented for
runs, scalar metrics, system metrics and filtered configuration only. Media,
artifacts, registry, traces, alerts, exports and project mutation are not supported
by the cloud service. The original local Trackio path remains available when no
managed context is supplied. Cloud mode never falls back to that SQLite path.

## Service and schema

Use Python 3.11 or later and the committed dependency lock:

```sh
uv sync --locked --extra cloud --extra dev
python -m trackio.cloud --config /absolute/private/server.toml --initialize
python -m trackio.cloud --config /absolute/private/server.toml --host 127.0.0.1 --port 18763
```

The configuration and both referenced key files must be regular files with mode
0600. Relative key paths resolve against the configuration directory. Use distinct,
random service and signing keys of at least 32 characters. Never put these keys or
the database URL in a worker context, command argument or public repository.

```toml
environment = "local"
database_url = "sqlite:////absolute/private/trackio-metrics.sqlite"
service_token_file = "service-token"
signing_key_file = "signing-key"
token_ttl = 3600
```

`staging` and `prod` require a `postgresql+psycopg://` URL. Their normal startup
checks the schema version and does not create tables; initialization is an explicit
operation with a separate DDL identity. The additive `trackio_cloud_*` tables do not
reuse or migrate an upstream dashboard database. Production TLS, DML grants,
capacity, backup, retention and network exposure require separate acceptance.

`GET /ready` reports API/schema versions and scalar capabilities. A service bearer
may bind an immutable workspace/project/Job/run and query its events. The bind
response contains a short-lived, HMAC-signed worker capability scoped to that run
and optional Trial. A worker may append events and update filtered configuration;
it cannot list runs or query metrics. HTTP responses use `Cache-Control: no-store`.
An event batch is acknowledged only after its transaction commits. Identical
replays are idempotent; conflicting event/batch identities fail atomically.

## Managed SDK

The controller supplies a private JSON context file through
`TRACKIO_ENVLOOP_CONTEXT_FILE`. Its fields are `server_url`, `token`, `run_id`,
`spool_dir` and optional `trial_id`. The context binds the destination; `init`
rejects a simultaneous Spaces, dataset, bucket or server destination. Remote
addresses require HTTPS; plain HTTP is restricted to loopback development.

```python
import trackio

run = trackio.init("display-name", config={"learning_rate": 0.0001})
trackio.log({"train/loss": 0.5, "train/global_step": 7})
trackio.log_system({"cpu_percent": 12.0})
trackio.finish()
```

Only finite scalars are accepted. Explicit `step` or `train/global_step` preserves
the actual training step; missing steps remain unknown. Configuration fields are
allowlisted; arbitrary credentials and training arguments are not uploaded.
Reports use a bounded private file spool, asynchronous delivery and durable ACKs.
Reopening the same run/Trial spool preserves its writer and pending event IDs.
`finish` has a bounded flush deadline and writes `delivery-status`; pending data
is retained and a metrics-gap warning does not establish training failure.
Each spool is for one reporting process. Distributed writers need separate spools.

## Tests and current limits

```sh
uv sync --locked --extra cloud --extra dev
python -m pytest --cloud tests/cloud/test_metrics.py -q
TRACKIO_TEST_DATABASE_FILE=/absolute/private/local-test-url \
  python -m pytest --cloud tests/cloud/test_postgres.py -q
```

The PostgreSQL URL must point to `127.0.0.1`, an explicit port and a database whose
name starts with `trackio_test_`. Tests cover transaction rollback, concurrent
idempotency, restart/readback and tenant/Trial filtering against real PostgreSQL.
The platform companion adds browser tests from this SDK through the real HTTP
service, Worker authorization and the Job metrics page, including offline replay.

Access credentials renew using a separate run/Trial-scoped refresh credential.
The controller fixes the maximum renewal deadline at binding time; the service
persists revocation and checks it inside each write transaction. Revocation
survives service restart and immediately invalidates both access and refresh.
Refresh cannot query metrics or issue another run's credentials.

Schema version 2 adds persistent capabilities. Existing version 1 installations
must stop the metrics service and explicitly run `--migrate-from 1` using the
migration identity and, where configured, `--owner-role`. Ordinary startup
rejects mismatched versions without modifying tables. The application identity
requires DML only; staging/prod also require an explicit private PostgreSQL schema.
The v1 migration records a bounded 24-hour legacy-token grace in the database.
Existing signed worker tokens retain their original expiry and scope, cannot
refresh, and are checked against durable run revocation inside each write
transaction. Fresh v2 installations have no legacy grace. Revoking a run also
prevents a previously unused legacy token or a new Trial from reopening it.

Automatic Job controller binding, recipe activation,
object storage and SQLite import, quotas/retention, platform release/deployment
integration and production operational acceptance are still required. A one-hour
test capability does not establish controller integration or terminal revocation.
Do not deploy this incremental implementation as the completed integration.
