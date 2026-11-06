"""Scoped scalar-metrics service. Independent of the upstream SQLite dashboard."""

import argparse
import base64
import hashlib
import hmac
import importlib
import json
import math
import re
import secrets
import stat
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import uvicorn
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateSchema
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from trackio.cloud_objects import MAX_OBJECT_BYTES
from trackio.cloud_records import RichRepository, rich_tables

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

SCHEMA_VERSION = 3
MAX_EVENTS = 256
MAX_BODY = 1024 * 1024
REVOKED_RUN_TRIAL = "__trackio_run_revoked__"


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


def identifier(value):
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("INVALID_IDENTITY")
    return value


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def validate_batch(batch):
    identifier(batch["batch_id"])
    events = batch["events"]
    if not isinstance(events, list) or not 1 <= len(events) <= MAX_EVENTS:
        raise ValueError("INVALID_BATCH_SIZE")
    seen = set()
    for event in events:
        event_id = identifier(event["event_id"])
        identifier(event["writer_id"])
        if event_id in seen:
            raise ValueError("DUPLICATE_EVENT")
        seen.add(event_id)
        if event.get("trial_id") is not None:
            identifier(event["trial_id"])
        if event["step"] is not None and (
            type(event["step"]) is not int or event["step"] < 0
        ):
            raise ValueError("INVALID_STEP")
        if not finite(event["observed_at"]) or event["category"] not in (
            "experiment",
            "compute",
        ):
            raise ValueError("INVALID_OBSERVATION")
        metrics = event["metrics"]
        if not isinstance(metrics, dict) or not 1 <= len(metrics) <= 128:
            raise ValueError("INVALID_METRICS")
        for key, value in metrics.items():
            identifier(key)
            if not finite(value):
                raise ValueError("INVALID_METRIC_VALUE")
    if len(canonical(batch).encode()) > MAX_BODY:
        raise ValueError("BATCH_TOO_LARGE")


@dataclass(frozen=True)
class CloudConfig:
    environment: str
    database_url: str = field(repr=False)
    service_token: str = field(repr=False)
    signing_key: str = field(repr=False)
    token_ttl: int = 3600
    database_schema: str | None = None
    objects_config_file: str | None = field(default=None, repr=False)

    def __post_init__(self):
        if self.environment not in ("local", "staging", "prod"):
            raise ValueError("INVALID_ENVIRONMENT")
        if self.environment != "local" and not self.database_url.startswith(
            "postgresql+psycopg://"
        ):
            raise ValueError("staging/prod require PostgreSQL")
        if (
            len(self.service_token) < 32
            or len(self.signing_key) < 32
            or self.service_token == self.signing_key
        ):
            raise ValueError("DISTINCT_STRONG_SERVICE_AND_SIGNING_KEYS_REQUIRED")
        if not 60 <= self.token_ttl <= 86400:
            raise ValueError("INVALID_TOKEN_TTL")
        if self.database_schema is not None:
            validate_schema(self.database_schema)
            if not self.database_url.startswith("postgresql+psycopg://"):
                raise ValueError("POSTGRESQL_SCHEMA_REQUIRED")
        if self.environment != "local" and self.database_schema is None:
            raise ValueError("EXPLICIT_POSTGRESQL_SCHEMA_REQUIRED")

    @classmethod
    def from_file(cls, path):
        path = Path(path)

        def private_text(source):
            metadata = source.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise ValueError("PRIVATE_CONFIG_FILE_REQUIRED")
            return source.read_text().strip()

        raw = tomllib.loads(private_text(path))
        return cls(
            environment=raw["environment"],
            database_url=raw["database_url"],
            service_token=private_text(path.parent / raw["service_token_file"]),
            signing_key=private_text(path.parent / raw["signing_key_file"]),
            token_ttl=raw.get("token_ttl", 3600),
            database_schema=raw.get("database_schema"),
            objects_config_file=str(path.parent / raw["objects_config_file"])
            if raw.get("objects_config_file")
            else None,
        )


def validate_schema(value):
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value)
        or value == "public"
        or value.startswith("pg_")
    ):
        raise ValueError("PRIVATE_POSTGRESQL_SCHEMA_REQUIRED")
    return value


def issue_token(
    key,
    workspace_id,
    project_id,
    job_id,
    run_id,
    *,
    expires_at,
    trial_id=None,
    capability_id=None,
):
    claims = dict(
        workspace_id=workspace_id,
        project_id=project_id,
        job_id=job_id,
        run_id=run_id,
        trial_id=trial_id,
        expires_at=expires_at,
        version=1,
        capability_id=capability_id,
    )
    payload = base64.urlsafe_b64encode(canonical(claims).encode()).decode().rstrip("=")
    signature = hmac.new(key.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return payload + "." + signature


def verify_token(key, token):
    try:
        payload, signature = token.split(".")
        expected = hmac.new(key.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError
        claims = json.loads(
            base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        )
        if (
            claims["version"] != 1
            or not finite(claims["expires_at"])
            or claims["expires_at"] <= time.time()
        ):
            raise ValueError
        for name in ("workspace_id", "project_id", "job_id", "run_id"):
            identifier(claims[name])
        return claims
    except (ValueError, KeyError, TypeError):
        raise PermissionError("INVALID_OR_EXPIRED_CAPABILITY") from None


class Conflict(ValueError):
    pass


class MetricsRepository(RichRepository):
    document_json = staticmethod(canonical)
    validate_identity = staticmethod(identifier)
    is_finite = staticmethod(finite)
    record_conflict = Conflict

    def __init__(
        self, url, *, initialize=True, schema=None, migrate_from=None, owner_role=None
    ):
        if not url.startswith(("sqlite:///", "postgresql+psycopg://")):
            raise ValueError("UNSUPPORTED_DATABASE")
        self.url = url
        self.engine = create_engine(
            url,
            pool_pre_ping=True,
            hide_parameters=True,
            pool_size=3,
            max_overflow=0,
            pool_timeout=5,
            connect_args={"check_same_thread": False, "timeout": 30}
            if url.startswith("sqlite")
            else {},
        )
        if schema is not None:
            validate_schema(schema)
            if not url.startswith("postgresql+psycopg://"):
                raise ValueError("POSTGRESQL_SCHEMA_REQUIRED")
        metadata = MetaData(schema=schema)
        self.schema = Table(
            "trackio_cloud_schema",
            metadata,
            Column("version", Integer, primary_key=True),
        )
        self.runs = Table(
            "trackio_cloud_runs",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("workspace_id", String(256), nullable=False, index=True),
            Column("project_id", String(256), nullable=False),
            Column("job_id", String(256), nullable=False, index=True),
            Column("config", Text, nullable=False),
            Column("updated_at", Float, nullable=False),
        )
        self.records = Table(
            "trackio_cloud_events",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("event_id", String(256), primary_key=True),
            Column("observed_at", Float, nullable=False, index=True),
            Column("trial_id", String(256), index=True),
            Column("payload", Text, nullable=False),
        )
        self.batches = Table(
            "trackio_cloud_batches",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("batch_id", String(256), primary_key=True),
            Column("digest", String(64), nullable=False),
        )
        self.configs = Table(
            "trackio_cloud_configs",
            metadata,
            Column("run_id", String(256), primary_key=True),
            Column("payload", Text, nullable=False),
        )
        self.capabilities = Table(
            "trackio_cloud_capabilities",
            metadata,
            Column("capability_id", String(64), primary_key=True),
            Column("run_id", String(256), nullable=False, index=True),
            Column("trial_key", String(256), nullable=False),
            Column("renew_until", Float, nullable=False),
            Column("revoked", Integer, nullable=False),
            UniqueConstraint("run_id", "trial_key"),
        )
        self.migrations = Table(
            "trackio_cloud_migrations",
            metadata,
            Column("version", Integer, primary_key=True),
            Column("applied_at", Float, nullable=False),
            Column("legacy_tokens_until", Float),
        )
        self.objects, self.documents = rich_tables(metadata)
        try:
            with self.engine.begin() as connection:
                if url.startswith("postgresql+psycopg://") and (
                    initialize or migrate_from
                ):
                    lock_id = int.from_bytes(
                        hashlib.sha256((schema or "public").encode()).digest()[:8],
                        "big",
                        signed=True,
                    )
                    connection.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_id)"),
                        {"lock_id": lock_id},
                    )
                if schema is not None and initialize:
                    connection.execute(CreateSchema(schema, if_not_exists=True))
                if owner_role is not None:
                    validate_schema(owner_role)
                    if not url.startswith("postgresql+psycopg://") or not (
                        initialize or migrate_from
                    ):
                        raise ValueError("EXPLICIT_POSTGRESQL_DDL_REQUIRED")
                    connection.execute(text(f'SET LOCAL ROLE "{owner_role}"'))
                known = inspect(connection).has_table(self.schema.name, schema=schema)
                versions = (
                    connection.execute(select(self.schema.c.version)).scalars().all()
                    if known
                    else []
                )
                if migrate_from is not None:
                    if migrate_from not in (1, 2) or versions != [migrate_from]:
                        raise ValueError("INCOMPATIBLE_MIGRATION_SOURCE")
                    legacy_until = (
                        time.time() + 86400
                        if migrate_from == 1
                        else connection.execute(
                            select(self.migrations.c.legacy_tokens_until).where(
                                self.migrations.c.version == migrate_from
                            )
                        ).scalar_one()
                    )
                    metadata.create_all(connection)
                    connection.execute(
                        self.migrations.insert().values(
                            version=SCHEMA_VERSION,
                            applied_at=time.time(),
                            legacy_tokens_until=legacy_until,
                        )
                    )
                    connection.execute(
                        self.schema.update().values(version=SCHEMA_VERSION)
                    )
                elif initialize:
                    if versions and versions != [SCHEMA_VERSION]:
                        raise ValueError("INCOMPATIBLE_SCHEMA")
                    metadata.create_all(connection)
                    if not versions:
                        connection.execute(
                            self.schema.insert().values(version=SCHEMA_VERSION)
                        )
                    if (
                        connection.execute(
                            select(self.migrations.c.version).where(
                                self.migrations.c.version == SCHEMA_VERSION
                            )
                        ).first()
                        is None
                    ):
                        connection.execute(
                            self.migrations.insert().values(
                                version=SCHEMA_VERSION,
                                applied_at=time.time(),
                                legacy_tokens_until=None,
                            )
                        )
                elif versions != [SCHEMA_VERSION]:
                    raise ValueError("SCHEMA_NOT_INITIALIZED_OR_INCOMPATIBLE")
                if any(
                    not inspect(connection).has_table(table.name, schema=schema)
                    for table in metadata.tables.values()
                ):
                    raise ValueError("INCOMPLETE_SCHEMA")
                if (
                    connection.execute(
                        select(self.migrations.c.version).where(
                            self.migrations.c.version == SCHEMA_VERSION
                        )
                    ).first()
                    is None
                ):
                    raise ValueError("INCOMPLETE_MIGRATION_RECEIPT")
        except BaseException:
            self.engine.dispose()
            raise

    def close(self):
        self.engine.dispose()

    def capability(self, identity, trial_id, renew_until=None):
        now = time.time()
        if trial_id is not None:
            identifier(trial_id)
            if trial_id == REVOKED_RUN_TRIAL:
                raise ValueError("RESERVED_TRIAL_IDENTITY")
        if renew_until is not None and (
            not finite(renew_until) or not now < renew_until <= now + 604800
        ):
            raise ValueError("INVALID_CAPABILITY_DEADLINE")
        with self.engine.begin() as connection:
            matched = connection.execute(
                self.runs.update()
                .where(
                    *[
                        self.runs.c[key] == identity[key]
                        for key in ("workspace_id", "project_id", "job_id", "run_id")
                    ]
                )
                .values(updated_at=now)
            )
            if matched.rowcount != 1:
                raise PermissionError("RUN_SCOPE_MISMATCH")
            if (
                connection.execute(
                    select(self.capabilities.c.capability_id).where(
                        self.capabilities.c.run_id == identity["run_id"],
                        self.capabilities.c.trial_key == REVOKED_RUN_TRIAL,
                    )
                ).first()
                is not None
            ):
                raise PermissionError("CAPABILITY_REVOKED_OR_EXPIRED")
            row = (
                connection.execute(
                    select(self.capabilities).where(
                        self.capabilities.c.run_id == identity["run_id"],
                        self.capabilities.c.trial_key == (trial_id or ""),
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                values = dict(
                    capability_id=secrets.token_hex(32),
                    run_id=identity["run_id"],
                    trial_key=trial_id or "",
                    renew_until=renew_until or now + 3600,
                    revoked=0,
                )
                connection.execute(self.capabilities.insert().values(**values))
                row = values
            elif renew_until is not None and row["renew_until"] != renew_until:
                raise Conflict("CAPABILITY_DEADLINE_IS_IMMUTABLE")
            if row["revoked"] or row["renew_until"] <= now:
                raise PermissionError("CAPABILITY_REVOKED_OR_EXPIRED")
            return {
                **identity,
                "trial_id": trial_id,
                "capability_id": row["capability_id"],
                "renew_until": row["renew_until"],
            }

    def authorize_capability(self, capability_id):
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(
                        self.capabilities,
                        self.runs.c.workspace_id,
                        self.runs.c.project_id,
                        self.runs.c.job_id,
                    )
                    .join(self.runs, self.capabilities.c.run_id == self.runs.c.run_id)
                    .where(self.capabilities.c.capability_id == capability_id)
                )
                .mappings()
                .first()
            )
        if row is None or row["revoked"] or row["renew_until"] <= time.time():
            raise PermissionError("CAPABILITY_REVOKED_OR_EXPIRED")
        return {
            key: row[key]
            for key in (
                "capability_id",
                "run_id",
                "workspace_id",
                "project_id",
                "job_id",
                "renew_until",
            )
        } | {"trial_id": row["trial_key"] or None}

    def revoke(self, identity):
        with self.engine.begin() as connection:
            matched = connection.execute(
                self.runs.update()
                .where(
                    *[
                        self.runs.c[key] == identity[key]
                        for key in ("workspace_id", "project_id", "job_id", "run_id")
                    ]
                )
                .values(updated_at=time.time())
            )
            if matched.rowcount != 1:
                raise PermissionError("RUN_SCOPE_MISMATCH")
            if (
                connection.execute(
                    select(self.capabilities.c.capability_id).where(
                        self.capabilities.c.run_id == identity["run_id"],
                        self.capabilities.c.trial_key == REVOKED_RUN_TRIAL,
                    )
                ).first()
                is None
            ):
                connection.execute(
                    self.capabilities.insert().values(
                        capability_id=secrets.token_hex(32),
                        run_id=identity["run_id"],
                        trial_key=REVOKED_RUN_TRIAL,
                        renew_until=0,
                        revoked=1,
                    )
                )
            connection.execute(
                self.capabilities.update()
                .where(self.capabilities.c.run_id == identity["run_id"])
                .values(revoked=1)
            )

    def bind(self, workspace_id, project_id, job_id, run_id, config):
        identity = {
            key: identifier(value)
            for key, value in dict(
                workspace_id=workspace_id,
                project_id=project_id,
                job_id=job_id,
                run_id=run_id,
            ).items()
        }
        if not isinstance(config, dict) or len(canonical(config)) > 65536:
            raise ValueError("INVALID_CONFIG")
        values = dict(**identity, config=canonical(config), updated_at=time.time())
        try:
            with self.engine.begin() as connection:
                connection.execute(self.runs.insert().values(**values))
        except IntegrityError:
            with self.engine.connect() as connection:
                existing = (
                    connection.execute(
                        select(self.runs).where(self.runs.c.run_id == run_id)
                    )
                    .mappings()
                    .one()
                )
                if (
                    any(existing[key] != value for key, value in identity.items())
                    or existing["config"] != values["config"]
                ):
                    raise Conflict("RUN_BINDING_CONFLICT") from None
        return identity

    def _live_capability(self, connection, capability_id, run_id):
        row = (
            connection.execute(
                select(self.capabilities).where(
                    self.capabilities.c.capability_id == capability_id,
                    self.capabilities.c.run_id == run_id,
                )
            )
            .mappings()
            .first()
        )
        if row is None or row["revoked"] or row["renew_until"] <= time.time():
            raise PermissionError("CAPABILITY_REVOKED_OR_EXPIRED")

    def _live_legacy(self, connection, claims):
        now = time.time()
        cutoff = connection.execute(
            select(self.migrations.c.legacy_tokens_until).where(
                self.migrations.c.version == SCHEMA_VERSION
            )
        ).scalar_one()
        if cutoff is None or now >= cutoff or claims["expires_at"] <= now:
            raise PermissionError("LEGACY_CAPABILITY_EXPIRED")
        bound = connection.execute(
            select(self.runs.c.run_id).where(
                *[
                    self.runs.c[key] == claims[key]
                    for key in ("workspace_id", "project_id", "job_id", "run_id")
                ]
            )
        ).first()
        revoked = connection.execute(
            select(self.capabilities.c.capability_id).where(
                self.capabilities.c.run_id == claims["run_id"],
                self.capabilities.c.revoked == 1,
            )
        ).first()
        if bound is None or revoked is not None:
            raise PermissionError("CAPABILITY_REVOKED_OR_EXPIRED")

    def authorize_legacy(self, claims):
        with self.engine.connect() as connection:
            self._live_legacy(connection, claims)

    def append(
        self,
        workspace_id,
        project_id,
        job_id,
        run_id,
        batch,
        capability_id=None,
        legacy_claims=None,
    ):
        validate_batch(batch)
        digest = hashlib.sha256(canonical(batch).encode()).hexdigest()
        with self.engine.begin() as connection:
            matched = connection.execute(
                self.runs.update()
                .where(
                    self.runs.c.run_id == run_id,
                    self.runs.c.workspace_id == workspace_id,
                    self.runs.c.project_id == project_id,
                    self.runs.c.job_id == job_id,
                )
                .values(updated_at=time.time())
            ).rowcount
            if matched != 1:
                raise PermissionError("RUN_SCOPE_MISMATCH")
            if capability_id is not None:
                self._live_capability(connection, capability_id, run_id)
            elif legacy_claims is not None:
                self._live_legacy(connection, legacy_claims)
            previous = connection.execute(
                select(self.batches.c.digest).where(
                    self.batches.c.run_id == run_id,
                    self.batches.c.batch_id == batch["batch_id"],
                )
            ).scalar_one_or_none()
            if previous is not None:
                if previous != digest:
                    raise Conflict("BATCH_CONTENT_CONFLICT")
                return 0
            accepted = 0
            for event in batch["events"]:
                payload = canonical(event)
                previous = connection.execute(
                    select(self.records.c.payload).where(
                        self.records.c.run_id == run_id,
                        self.records.c.event_id == event["event_id"],
                    )
                ).scalar_one_or_none()
                if previous is not None:
                    if previous != payload:
                        raise Conflict("EVENT_CONTENT_CONFLICT")
                    continue
                connection.execute(
                    self.records.insert().values(
                        run_id=run_id,
                        event_id=event["event_id"],
                        observed_at=event["observed_at"],
                        trial_id=event["trial_id"],
                        payload=payload,
                    )
                )
                accepted += 1
            connection.execute(
                self.batches.insert().values(
                    run_id=run_id, batch_id=batch["batch_id"], digest=digest
                )
            )
            return accepted

    def events(
        self,
        workspace_id,
        project_id,
        job_id,
        *,
        start=None,
        end=None,
        trial_id=None,
        limit=2001,
    ):
        statement = (
            select(self.records.c.payload, self.records.c.run_id)
            .join(self.runs, self.records.c.run_id == self.runs.c.run_id)
            .where(
                self.runs.c.workspace_id == workspace_id,
                self.runs.c.project_id == project_id,
                self.runs.c.job_id == job_id,
            )
        )
        if start is not None:
            statement = statement.where(self.records.c.observed_at >= start)
        if end is not None:
            statement = statement.where(self.records.c.observed_at <= end)
        if trial_id is not None:
            statement = statement.where(self.records.c.trial_id == trial_id)
        statement = statement.order_by(
            self.records.c.observed_at, self.records.c.event_id
        ).limit(limit)
        results = []
        with self.engine.connect() as connection:
            for payload, run_id in connection.execute(statement):
                record = {**json.loads(payload), "run_id": run_id}
                if trial_id is None or record["trial_id"] == trial_id:
                    results.append(record)
                if len(results) >= limit:
                    break
        return results

    def update_config(self, claims, values):
        if not isinstance(values, dict) or len(canonical(values)) > 65536:
            raise ValueError("INVALID_CONFIG")
        with self.engine.begin() as connection:
            matched = connection.execute(
                self.runs.update()
                .where(
                    self.runs.c.run_id == claims["run_id"],
                    self.runs.c.workspace_id == claims["workspace_id"],
                    self.runs.c.project_id == claims["project_id"],
                    self.runs.c.job_id == claims["job_id"],
                )
                .values(updated_at=time.time())
            ).rowcount
            if matched != 1:
                raise PermissionError("RUN_SCOPE_MISMATCH")
            if claims.get("capability_id") is not None:
                self._live_capability(
                    connection, claims["capability_id"], claims["run_id"]
                )
            else:
                self._live_legacy(connection, claims)
            previous = connection.execute(
                select(self.configs.c.payload).where(
                    self.configs.c.run_id == claims["run_id"]
                )
            ).scalar_one_or_none()
            merged = {**(json.loads(previous) if previous else {}), **values}
            if previous is None:
                connection.execute(
                    self.configs.insert().values(
                        run_id=claims["run_id"], payload=canonical(merged)
                    )
                )
            else:
                connection.execute(
                    self.configs.update()
                    .where(self.configs.c.run_id == claims["run_id"])
                    .values(payload=canonical(merged))
                )


def configured_objects(config):
    if config.objects_config_file is None:
        return None
    path = Path(config.objects_config_file)

    def private_file(filename):
        value = path.parent / filename
        metadata = value.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise ValueError("PRIVATE_OBJECT_CONFIG_REQUIRED")
        return value.read_text().strip()

    settings = tomllib.loads(private_file(path.name))
    if settings["backend"] == "file":
        if config.environment != "local":
            raise ValueError("MANAGED_OBJECT_STORAGE_REQUIRED")
        return importlib.import_module("trackio.cloud_objects").FileObjects(
            path.parent / settings["directory"]
        )
    if settings["backend"] != "s3" or (
        config.environment != "local"
        and config.environment not in settings["prefix"].split("/")
    ):
        raise ValueError("ENVIRONMENT_OBJECT_PREFIX_REQUIRED")
    credentials = {
        key: private_file(settings[key + "_file"])
        for key in ("access_key_id", "secret_access_key")
    }
    return importlib.import_module("trackio.cloud_s3").s3_objects(settings, credentials)


def create_app(config, repository=None, *, objects=None):
    objects = objects if objects is not None else configured_objects(config)
    owns_repository = repository is None
    repository = repository or MetricsRepository(
        config.database_url,
        initialize=config.environment == "local",
        schema=config.database_schema,
    )

    def service(request):
        if not hmac.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + config.service_token
        ):
            raise PermissionError("SERVICE_AUTH_REQUIRED")

    async def body(request):
        content = bytearray()
        async for chunk in request.stream():
            content.extend(chunk)
            if len(content) > MAX_BODY:
                raise ValueError("REQUEST_TOO_LARGE")
        return json.loads(content)

    async def ready(request):
        with repository.engine.connect() as connection:
            connection.execute(select(repository.schema.c.version)).one()
        return JSONResponse(
            {
                "status": "ready",
                "schema_version": SCHEMA_VERSION,
                "api_version": 1,
                "capabilities": ["runs", "scalar_metrics", "system_metrics", "records"]
                + (["objects"] if objects is not None else []),
            }
        )

    async def bind_run(request):
        service(request)
        data = await body(request)
        identity = repository.bind(
            **{
                key: data[key]
                for key in ("workspace_id", "project_id", "job_id", "run_id", "config")
            }
        )
        capability = repository.capability(
            identity, data.get("trial_id"), data.get("renew_until")
        )
        return JSONResponse(tokens(capability))

    def refresh_secret(capability_id):
        return (
            capability_id
            + "."
            + hmac.new(
                config.signing_key.encode(),
                ("trackio:refresh:" + capability_id).encode(),
                hashlib.sha256,
            ).hexdigest()
        )

    def tokens(capability):
        expires_at = min(time.time() + config.token_ttl, capability["renew_until"])
        token = issue_token(
            config.signing_key,
            **{
                key: capability[key]
                for key in ("workspace_id", "project_id", "job_id", "run_id")
            },
            expires_at=expires_at,
            trial_id=capability["trial_id"],
            capability_id=capability["capability_id"],
        )
        return {
            **capability,
            "token": token,
            "expires_at": expires_at,
            "refresh_token": refresh_secret(capability["capability_id"]),
        }

    def authorized(request):
        claims = verify_token(
            config.signing_key,
            request.headers.get("authorization", "").removeprefix("Bearer "),
        )
        if claims.get("capability_id") is None:
            repository.authorize_legacy(claims)
            return claims
        capability = repository.authorize_capability(claims["capability_id"])
        if any(
            claims.get(key) != capability.get(key)
            for key in ("run_id", "workspace_id", "project_id", "job_id", "trial_id")
        ):
            raise PermissionError("CAPABILITY_SCOPE_MISMATCH")
        return claims

    async def refresh(request):
        raw = request.headers.get("authorization", "").removeprefix("Bearer ")
        capability_id = raw.partition(".")[0]
        if not hmac.compare_digest(raw, refresh_secret(capability_id)):
            raise PermissionError("INVALID_REFRESH_CAPABILITY")
        return JSONResponse(tokens(repository.authorize_capability(capability_id)))

    async def revoke(request):
        service(request)
        identity = await body(request)
        if identity["run_id"] != request.path_params["run_id"]:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        repository.revoke(identity)
        return JSONResponse({"revoked": True})

    async def append(request):
        claims = authorized(request)
        if claims["run_id"] != request.path_params["run_id"]:
            return JSONResponse({"error": "RUN_SCOPE_MISMATCH"}, status_code=403)
        data = await body(request)
        validate_batch(data)
        if any(
            event.get("trial_id") != claims.get("trial_id") for event in data["events"]
        ):
            raise PermissionError("TRIAL_SCOPE_MISMATCH")
        accepted = repository.append(
            **{
                key: claims[key]
                for key in ("workspace_id", "project_id", "job_id", "run_id")
            },
            batch=data,
            capability_id=claims.get("capability_id"),
            legacy_claims=claims if claims.get("capability_id") is None else None,
        )
        return JSONResponse(
            {"batch_id": data["batch_id"], "accepted": accepted, "committed": True}
        )

    async def query(request):
        service(request)
        scope = {
            key: identifier(request.query_params[key])
            for key in ("workspace_id", "project_id", "job_id")
        }
        for key in ("start", "end"):
            if key in request.query_params:
                scope[key] = float(request.query_params[key])
                if not finite(scope[key]):
                    raise ValueError("INVALID_TIME_RANGE")
        scope["trial_id"] = request.query_params.get("trial_id")
        events = repository.events(**scope)
        return JSONResponse({"events": events[:2000], "truncated": len(events) > 2000})

    async def update_config(request):
        claims = authorized(request)
        if claims["run_id"] != request.path_params["run_id"]:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        repository.update_config(claims, await body(request))
        return JSONResponse({"committed": True})

    async def upload_object(request):
        claims = authorized(request)
        if claims["run_id"] != request.path_params["run_id"]:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        payload = bytearray()
        async for chunk in request.stream():
            payload.extend(chunk)
            if len(payload) > MAX_OBJECT_BYTES:
                raise ValueError("OBJECT_TOO_LARGE")
        repository.put_object(
            claims,
            request.path_params["digest"],
            bytes(payload),
            request.headers.get("content-type", "application/octet-stream"),
            objects,
        )
        return JSONResponse(
            {"committed": True, "digest": request.path_params["digest"]}
        )

    async def append_record(request):
        claims = authorized(request)
        if claims["run_id"] != request.path_params["run_id"]:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        record = await body(request)
        accepted = repository.append_record(claims, record)
        return JSONResponse(
            {"committed": True, "record_id": record["record_id"], "accepted": accepted}
        )

    def read_scope(request):
        service(request)
        return {
            key: identifier(request.query_params[key])
            for key in ("workspace_id", "project_id", "job_id", "run_id")
        }

    async def query_records(request):
        rows = repository.read_records(
            read_scope(request),
            request.query_params.get("kind"),
            request.query_params.get("after"),
        )
        return JSONResponse(
            {
                "records": rows[:500],
                "truncated": len(rows) > 500,
                "next": rows[499]["record_id"] if len(rows) > 500 else None,
            }
        )

    async def download_object(request):
        scope = read_scope(request)
        if scope["run_id"] != request.path_params["run_id"]:
            raise PermissionError("RUN_SCOPE_MISMATCH")
        payload = repository.read_object(scope, request.path_params["digest"], objects)
        return Response(
            payload,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": 'attachment; filename="'
                + request.path_params["digest"]
                + '"',
                "X-Content-Type-Options": "nosniff",
                "Content-Security-Policy": "sandbox; default-src 'none'",
            },
        )

    async def forbidden(request):
        raise PermissionError("SERVICE_AUTH_REQUIRED")

    async def error(request, exception):
        status = (
            409
            if isinstance(exception, Conflict)
            else 401
            if isinstance(exception, PermissionError)
            else 422
        )
        if isinstance(exception, PermissionError) and str(exception) in (
            "SERVICE_AUTH_REQUIRED",
            "TRIAL_SCOPE_MISMATCH",
            "RUN_SCOPE_MISMATCH",
        ):
            status = 403
        return JSONResponse(
            {
                "error": str(exception)
                if isinstance(exception, (Conflict, PermissionError))
                else "INVALID_REQUEST"
            },
            status_code=status,
        )

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            if owns_repository:
                repository.close()

    app = Starlette(
        lifespan=lifespan,
        routes=[
            Route("/ready", ready),
            Route("/v1/runs", bind_run, methods=["POST"]),
            Route("/v1/runs", forbidden),
            Route("/v1/runs/{run_id}/events", append, methods=["POST"]),
            Route("/v1/metrics", query),
            Route("/v1/runs/{run_id}/config", update_config, methods=["PUT"]),
            Route("/v1/capabilities/refresh", refresh, methods=["POST"]),
            Route("/v1/runs/{run_id}/revoke", revoke, methods=["POST"]),
            Route("/v1/runs/{run_id}/objects/{digest}", upload_object, methods=["PUT"]),
            Route(
                "/v1/runs/{run_id}/objects/{digest}", download_object, methods=["GET"]
            ),
            Route("/v1/runs/{run_id}/records", append_record, methods=["POST"]),
            Route("/v1/records", query_records, methods=["GET"]),
        ],
        exception_handlers={
            ValueError: error,
            KeyError: error,
            TypeError: error,
            PermissionError: error,
        },
    )

    async def no_cache(request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    app.add_middleware(BaseHTTPMiddleware, dispatch=no_cache)
    return app


def main():
    parser = argparse.ArgumentParser(description="Scoped Trackio metrics service")
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=18763, type=int)
    schema_action = parser.add_mutually_exclusive_group()
    schema_action.add_argument(
        "--initialize",
        action="store_true",
        help="Explicitly initialize the additive schema, then exit",
    )
    schema_action.add_argument("--migrate-from", choices=[1], type=int)
    parser.add_argument("--owner-role")
    args = parser.parse_args()
    config = CloudConfig.from_file(args.config)
    if args.initialize or args.migrate_from is not None:
        MetricsRepository(
            config.database_url,
            schema=config.database_schema,
            initialize=args.initialize,
            migrate_from=args.migrate_from,
            owner_role=args.owner_role,
        ).close()
        return
    uvicorn.run(
        create_app(config),
        host=args.host,
        port=args.port,
        access_log=False,
    )


if __name__ == "__main__":
    main()
