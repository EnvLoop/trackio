# EnvLoop Trackio component 0.46.1

This fork adds a managed cloud SDK and service alongside the original local
Trackio implementation. The component version is read from
`trackio/package.json` by both Python and Hatch. The dynamic editable entry in
`uv.lock` intentionally has no static project version. The dependency lock
retains its resolved versions; the HF requirement is `>=1.31.0,<3`.
Product release versions are independent. This source change does not release
a package or create a tag.

## Schema and explicit owner migration

The final service uses **core schema 3 and workspace schema 2: 21 tables**.

| Schema | Tables | Purpose |
| --- | --- | --- |
| Core v1, retained | `trackio_cloud_schema`, `trackio_cloud_runs`, `trackio_cloud_events`, `trackio_cloud_batches`, `trackio_cloud_configs` | Version, immutable run binding, scalar events, batch receipts, selected config |
| Core v3 additions | `trackio_cloud_capabilities`, `trackio_cloud_migrations`, `trackio_cloud_objects`, `trackio_cloud_records` | Durable scoped capability/revocation, migration receipt, object catalog, rich records |
| Workspace v1 | `trackio_workspace_schema`, `trackio_workspace_heads`, `trackio_workspace_resources`, `trackio_workspace_aliases` | Workspace stamp, monotonic versions, immutable artifacts/registry/logbook, aliases |
| Workspace v1 | `trackio_workspace_operations`, `trackio_workspace_links`, `trackio_workspace_runs` | Content-bound operation receipts/audit, lineage, run name/retention/tombstone |
| Workspace v1 | `trackio_workspace_imports`, `trackio_workspace_legacy_rows`, `trackio_workspace_legacy_files`, `trackio_workspace_uploads` | Offline import receipts/raw data/files, durable physical-upload intent and GC fence |
| Workspace v2 addition | `trackio_workspace_upload_locations` | Per-run physical location/generation journal; preserves earlier project prefixes across retry and move |

Use the backed-up database's private owner configuration, with the target schema
explicitly selected. The existing schema owner must have DDL permissions and
the migrator must be allowed to assume that role. Set the owner's default table
privileges for the restricted app role before migration; existing-table grants
alone do not cover new tables. Neither the app role nor worker receives owner
credentials.

```sh
python -m trackio.cloud --config /private/trackio-owner.toml \
  --migrate-from 1 --owner-role trackio_owner
```

This one invocation upgrades core and creates workspace tables in **one
PostgreSQL transaction**, under the schema advisory lock and selected owner role.
A workspace migration fault rolls back the core version, receipt and DDL too.
It exits after migration and does not start a listener. `--initialize` is for a
fresh schema, or adding workspace to an already compatible core v3; it rejects
core v1 rather than silently migrating it. For existing core v3/workspace v1,
use the separate owner action:

```sh
python -m trackio.cloud --config /private/trackio-owner.toml \
  --migrate-workspace-from 1 --owner-role trackio_owner
```

This requires a compatible existing core and workspace stamp exactly 1, upgrades
only workspace to v2, seeds the location journal, and exits without a listener.
The three CLI actions `--initialize`, `--migrate-from` and
`--migrate-workspace-from` are mutually exclusive. `--migrate-from 3` stays invalid:
core schema remains 3. The workspace upgrade and stamp update share one
transaction; normal startup rejects workspace v1 without changing it. Repeating
the exact-source workspace migration after success fails with
`WORKSPACE_MIGRATION_SOURCE_MISMATCH` and leaves the new schema intact; use the
normal no-DDL probe/startup to confirm success. A preexisting core v2 uses
`--migrate-from 2`. The migrated v1 legacy-token compatibility window is 24 hours;
normal new bindings use durable capabilities.

```sh
python -m trackio.cloud --config /private/trackio-app.toml \
  --host 0.0.0.0 --port 18763
```

Staging/production startup constructs `WorkspaceRepository(initialize=False,
initialize_workspace=False)`. It checks both complete table sets and stamps,
and performs **no DDL**. Missing tables/stamps fail startup. Private TLS remains
mandatory for nonlocal listeners; the app configuration must include its private
certificate/key and any health-probe CA configuration. Database schema stamps
and migration receipts can have only SELECT granted to the app. Business tables
need their scoped SELECT/INSERT/UPDATE/DELETE permissions. Application methods
cannot bootstrap or migrate a production schema.

## SDK and HTTP contracts

Managed initialization selects CloudRun/ManagedCloudRun. `Api()` and native
Artifact/Registry handles route through the managed service. `sync()`/`freeze()`
flush the cloud spool and return no public HF identity. `save()` queues scoped
artifact files. Project deletion requires the scoped service API; managed
workers cannot fall through to local deletion. `show()` returns the private
service location, and does not start an upstream dashboard or tunnel.

The additional worker routes are exactly:

1. `POST /v1/workspace/query`: own-run list/filter, compare, export, resources,
   lineage, operation readback, and scoped legacy metadata readback. Live
   capability/legacy validation and all data reads share a run-locked transaction
   after the bounded request body is read. Revocation serializes on that lock.
2. `POST /v1/workspace/publish`: artifact/logbook/registry publication and own-run
   use/unlink/alias operations, all fenced again inside the run transaction.
3. `GET /v1/workspace/objects/{64-lowercase-hex}`: catalog-authorized byte download.

The proxy must retain JSON ACKs unchanged and allow at most 1 MiB request JSON.
Resource content is capped at 512 KiB. Objects are capped at 32 MiB and per-run
catalog quota at 100 MiB. Download returns `application/octet-stream`, attachment
disposition, `nosniff`, `Content-Security-Policy: sandbox; default-src 'none'`,
and `Cache-Control: no-store`. No presigned URL or redirect is returned. Every
download reauthorizes the live run/capability, even for a cached Artifact handle,
and verifies size and SHA-256. HTML is downloaded as an isolated attachment.

`POST /v1/workspace/mutate` is service-only: explicit full scopes for run
rename/move/delete/retention, cross-run links, purge and GC. A move can change
project only, preserves workspace/Job/run identity, and revokes old capabilities.
Workers cannot enumerate another run, supply a foreign scope, link a foreign
source or move a Job. The legacy-file endpoint is a separate administrative
readback path and is not required or permitted through the worker proxy.

Artifacts retain native attachment manifests, immutable versions, aliases,
registry collections, use lineage, link/unlink and authenticated download.
Logbooks publish immutable page/attachment versions; pages are returned as data.
Formal model checkpoints remain EnvHub ANS assets; experiment attachments and
registry links never grant formal checkpoint identity. External references are
preserved during migration but require explicit asset resolution before download;
the service never silently fetches them from HF or another provider.

## Queue and E2E assertions

The spool has a fixed total byte budget including object blobs, pending JSON,
selected config and reserved control state. Config writes coalesce into
`config.pending`; delivery may interleave config, scalar batches, rich records,
object uploads and workspace operations. An object ACK is emitted only after
physical upload, full GET/hash verification, a second capability fence and
catalog commit. Resources reference only committed same-run objects.

Scalar retries retain their original batch/event IDs. Workspace retries retain
their operation ID and content digest; a changed request under an existing ID
is rejected. Local files are removed only after the corresponding committed ACK.
The managed `finish(timeout=...)` stops monitors and bounds delivery waiting; it
writes local delivery status, retaining pending data on timeout/full spool.
It does not guarantee a fixed number of HTTP requests or database batches, and
does not synthesize a Job, Trial or training step. Native callbacks may update
config or emit their own terminal metrics before finish.

E2E should assert the required event IDs/metric values and original step/Trial
identity exactly once, selected config readback, operation/resource digests,
and committed=true/pending=0/dropped=0 when delivery is expected to finish.
Do not assert that init+log+finish equals one batch, or count object/config
requests as extra training events. Finish success alone does not establish that
the platform installer has activated a built wheel.

## Offline snapshots and exports

`cloud_snapshot.capture(source, destination, writers_stopped=True)` snapshots
every SQLite table and accompanying file under the explicit legacy root, within
the file/byte budget. SQLite and WAL are copied first and backed up from that
private copy, avoiding source mutation from read-only SQLite cleanup. Source
inventory/digests are checked again before atomic publication. Symlinks and
concurrent writers fail the snapshot. Verify the snapshot before import.

`LegacyImporter.import_snapshot(snapshot, bindings, source_id=...)` requires
explicit database old-run/name/default-scope and complete file bindings to
already existing cloud runs. Missing Job/Trial history is never fabricated.
Source identity is namespaced by workspace; content/binding changes under the
same identity conflict. Raw tables (including blob values), pending fragments,
media, CAS artifact bytes, aliases/versions, registry collections/links/events,
traces, alerts and logbook files remain in the offline record. Common metrics,
media, artifacts, local registry and logbooks additionally receive native cloud
projections. Registry sources must be present and explicitly bound; missing
external content does not trigger a network fetch.

Use `read_rows`, `read_file`, `receipt`, or scoped administrative HTTP readback
after closing/moving the original legacy source. Actual upstream SQLite/image/
artifact/registry fixtures are tested with the original source moved offline,
including repository close/reopen and actual zero-byte native Artifact files.
Empty files have a committed SHA-256 empty object and remain downloadable.
The default snapshot budget is 512 MiB and
10,000 files; object/run quotas still apply to import. This bounded operator API
does not automatically discover arbitrary external upload paths or running
writers: include all required legacy files in the explicit snapshot root.

Run export returns a bounded, locked JSON snapshot with a content digest, raw
legacy records/files and resource/operation metadata. CloudApiRun can write JSON
or optional Parquet (`trackio[spaces]`) without SQLite. Parquet retains original
event payloads and the full content identity metadata. Object bytes are
downloaded separately with fresh authorization; exporting catalog metadata does
not create permanent download authority. Compare preserves each run's original
step/Trial/writer and supports time/metric/step filtering, with explicit limits.
Time and step filters are applied before the result point budget. Tombstoned
runs are also excluded from the original scalar metrics read endpoint.

Registry source content and its digest retain historical provenance after a
project move. Resolution separately supplies the current scope for the same
stable workspace/Job/Run identity. Native use checks the current scope and exact
source artifact digest; cross-run native use still needs explicit service scopes.
Service lineage includes cross-run links; worker lineage only exposes links
whose resolved target is its own current scope.

Physical GC requires service authorization, the complete run scope and a minimum
one-hour orphan retention (default one day). It commits catalog invalidation and
a durable collecting claim before deleting any physical bytes. Each subsequent
delete rechecks the physical location generation under the run lock. A partial
delete or database failure cannot restore an ACKable catalog for missing bytes;
collecting claims recover on retry/restart. A new upload generation prevents an
older collector from deleting its replacement. Collected location tombstones
remain available to catch late writes, and committed resource/record/import
references protect their actual catalog location. Bounded inventory pagination
also covers pre-journal objects under the current prefix and historical prefixes
proven by committed same-identity project-move receipts. Purge retains these
receipts as physical-cleanup audit evidence, including after workspace-v1
migration. Before retaining or using a move receipt, the service
verifies its request digest and complete scope, including stable workspace,
Job and Run identity. Other business operation receipts are purged normally.
Retained cleanup evidence survives repeated purge and repository restart;
purging business data does not revoke the service's historical cleanup authority.
Cursors are opaque and must be passed back unchanged; foreign-prefix cursors are
rejected. Each call
processes at most 1,000 journal candidates and one bounded inventory page;
operators repeat collection and follow `next` until inventory is exhausted.
Move history is bounded to 1,000 receipts and reference/catalog scans to 10,000
rows, with explicit failure when exceeded. Durable intents and location records
retain prior scope prefixes across project moves. Local-file
and S3-compatible inventory paths are implemented; no claim of live R2 testing
is made by the synthetic/local tests.

## Validation boundary

The cloud suite requires `TRACKIO_TEST_DATABASE_FILE` pointing to the isolated
local PostgreSQL task database. Missing configuration fails; it does not skip
PostgreSQL. CPU/unit and cloud jobs remain separate. Owner migration tests use
isolated roles/schemas, invoke the real CLI and verify restricted app operation.
See [native callbacks](envloop-native-callbacks.md) for real installed framework
results and the Transformers 5.17/HF 2.4 import incompatibility. Final wheel
installation, public platform proxy E2E and live R2 acceptance remain separate
integration gates. No release, cloud migration, GPU or HF upload is performed.
