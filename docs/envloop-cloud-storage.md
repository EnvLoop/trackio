# Managed records and object storage

The managed service keeps scalar events and structured experiment records in
PostgreSQL. Image, video, audio, HTML, Object3D and experiment attachment bytes
belong in an explicitly configured S3-compatible bucket. Local testing can use a
private filesystem directory. Staging and production reject that fallback.

Schema 3 adds `trackio_cloud_objects` and `trackio_cloud_records`. An explicit
`--migrate-from 1` or `--migrate-from 2` is required for existing databases.
PostgreSQL migration uses a transaction-scoped advisory lock for the selected
schema. Migration preserves the original legacy-token deadline; upgrading again
does not renew old credentials. Ordinary non-local startup performs no DDL.

The private server TOML references `objects_config_file`. Its private TOML uses
`backend="s3"`, an HTTPS `endpoint_url`, `region`, `bucket`, a nonempty `prefix`
containing the exact environment segment, `access_key_id_file` and
`secret_access_key_file`. Referenced keys and configuration files require mode
0600. Keys stay on the service, never in worker contexts. A local configuration
can instead use `backend="file"` and an explicit `directory`.

The service generates content-addressed keys from the immutable run scope and
SHA-256 digest. Uploads require live run capabilities, verify bytes and size, and
return a committed acknowledgment only after object upload and database commit.
Structured records cannot reference an object absent from that run's committed
catalog. Uploads are limited to 32 MiB per object and 100 MiB per run. The SDK's
smaller private spool limit remains authoritative on the worker.

`PUT /v1/runs/{run_id}/objects/{digest}` and
`POST /v1/runs/{run_id}/records` accept worker capabilities. Replays preserve
record identity and reject different content. `GET /v1/records` and
`GET /v1/runs/{run_id}/objects/{digest}` require service authentication plus the
complete authorized workspace/project/Job/run scope. Downloads always use an
attachment disposition, `nosniff` and a restrictive sandbox policy. No caller can
submit a storage key, signed URL or SQL expression.

The managed SDK serializes native Image, Audio, Video, HTML, Object3D, Table,
Trace, Histogram and Markdown values into its private spool. Objects upload before
referencing records; local bytes are reclaimed only after the matching committed
record acknowledgment. Scalar values retain the existing metrics projection.
Summary updates and alerts are structured records. Webhook destination overrides
are disabled for managed workers.

This storage increment does not complete the full integration proposal. Native
artifact/registry operations, project browsing and lifecycle controls, complete
legacy migration, public read proxies and dual release/deployment integration
still require their own implementation and acceptance evidence. Local PostgreSQL
and filesystem tests do not prove real R2 credentials or a deployed service.
# Connection capacity

See [cloud database pools](envloop-cloud-pools.md) for PostgreSQL read/write budgets, SQLite compatibility, request admission and object-read transaction boundaries.

