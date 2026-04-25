# Use the official Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if any are needed by pgvector/psycopg2)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy project configuration files
COPY pyproject.toml ./

# Install project dependencies
RUN pip install --no-cache-dir -e ".[dev]"

# Copy the rest of the application code
COPY . .

# Run the seed script (applies base schema idempotently), then migrations, and start Uvicorn
# Cloud Run sets the PORT environment variable (default 8080)
CMD python -m seed.seed && python -c "from connectors.migrations import apply_all; apply_all()" && uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8080}
