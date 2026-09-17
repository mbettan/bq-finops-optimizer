# P2: Digest-pinned base image for reproducible builds.
# Regenerate with: docker pull python:3.11-slim && docker inspect --format='{{index .RepoDigests 0}}' python:3.11-slim
FROM python:3.11-slim@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93

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
