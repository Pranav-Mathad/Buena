# Keystone backend image.
#
# Stage 1 installs only the runtime dependencies declared in pyproject.toml
# into a virtualenv; stage 2 copies that virtualenv and the source into a slim
# image that runs as an unprivileged user. Dev tools (pytest, ruff, mypy) are
# never installed. Every dependency ships manylinux wheels, so no compiler or
# system packages are needed.

FROM python:3.11-slim AS deps
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build
COPY pyproject.toml ./
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies']))" > requirements.txt \
 && python -m venv /opt/venv \
 && /opt/venv/bin/pip install -r requirements.txt

FROM python:3.11-slim
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN useradd --create-home --uid 10001 keystone
WORKDIR /app
COPY --from=deps /opt/venv /opt/venv
COPY --chown=keystone:keystone backend ./backend
COPY --chown=keystone:keystone connectors ./connectors
COPY --chown=keystone:keystone seed ./seed
COPY --chown=keystone:keystone mcp_server ./mcp_server
COPY --chown=keystone:keystone Extracted_demo ./Extracted_demo
COPY --chown=keystone:keystone scripts/entrypoint.sh ./scripts/entrypoint.sh
USER keystone
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
  CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\", \"8000\")}/health', timeout=2)"
ENTRYPOINT ["./scripts/entrypoint.sh"]
