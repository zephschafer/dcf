FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY dcf/ dcf/

RUN pip install --no-cache-dir -e ".[app]"

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=60s \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["uvicorn", "dcf.app.server:app", "--host", "0.0.0.0", "--port", "8080"]
