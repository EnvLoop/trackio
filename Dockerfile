FROM node:22-bookworm-slim@sha256:5ec23b7c622b00547753bc6ffd78085dd868a68aeb41c8e0a1418f033e13d96d AS frontend
WORKDIR /frontend
COPY trackio/frontend/package.json trackio/frontend/package-lock.json ./
RUN npm ci
COPY trackio/frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df AS build
WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.19
COPY pyproject.toml uv.lock README.md LICENSE hatch_build.py ./
COPY trackio/ ./trackio/
COPY --from=frontend /frontend/dist/ ./trackio/frontend/dist/
ENV SKIP_FRONTEND_BUILD=1
RUN uv sync --locked --extra cloud --no-dev --no-editable

FROM python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df
ARG SOURCE_SHA
ARG TRACKIO_VERSION
ARG MIGRATION_SHA256
ARG SDK_SHA256
ARG API_VERSION
ARG SCHEMA_READ
ARG SCHEMA_WRITE
LABEL org.opencontainers.image.source="https://github.com/EnvLoop/trackio" \
      org.opencontainers.image.revision="${SOURCE_SHA}" \
      org.opencontainers.image.version="${TRACKIO_VERSION}" \
      io.envloop.trackio.migration-sha256="${MIGRATION_SHA256}" \
      io.envloop.trackio.sdk-sha256="${SDK_SHA256}" \
      io.envloop.trackio.api-version="${API_VERSION}" \
      io.envloop.trackio.schema-read="${SCHEMA_READ}" \
      io.envloop.trackio.schema-write="${SCHEMA_WRITE}"
COPY --from=build /app/.venv/ /app/.venv/
ENV PATH="/app/.venv/bin:${PATH}" \
    HOME="/tmp/trackio" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 18570
HEALTHCHECK --interval=15s --timeout=5s --retries=4 CMD ["python", "-m", "trackio.cloud_server"]
ENTRYPOINT ["python", "-m", "trackio.cloud"]
CMD ["--config", "/run/trackio/server.toml", "--host", "0.0.0.0", "--port", "18570"]
