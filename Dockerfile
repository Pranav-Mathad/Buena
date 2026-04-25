FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

RUN pip install --no-cache-dir -e ".[dev]"

COPY . .

ENV EXTRACTED_ROOT=/app/Extracted_demo

CMD python -m seed.seed \
    && python -c "from connectors.migrations import apply_all; apply_all()" \
    && python -m connectors.cli load-stammdaten --source buena \
    && uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8080}
