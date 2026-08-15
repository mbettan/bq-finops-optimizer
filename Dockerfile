# P2: Digest-pinned multi-arch index (linux/amd64 + linux/arm64).
# Do not pin a single-arch image digest from `docker inspect` on Apple Silicon;
# Cloud Run requires linux/amd64.
FROM python:3.11-slim@sha256:a630a63cdb314e2d138a2fca3e375e319e8568346ffafac5b980f888630ac4f1

# Set working directory
WORKDIR /app

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create non-root user for security (P1)
RUN useradd -r -u 10001 -s /usr/sbin/nologin appuser && chown -R appuser /app

# Copy the rest of the application code
COPY --chown=appuser src/ src/
COPY --chown=appuser static/ static/

# Ensure stdout/stderr are unbuffered so logs are not lost on SIGKILL
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER 10001

# Expose port (Cloud Run defaults to 8080)
EXPOSE 8080

# Honour Cloud Run's $PORT environment variable (P4)
CMD exec uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8080}
