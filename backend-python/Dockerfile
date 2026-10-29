# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime
WORKDIR /app

# curl for the container healthcheck; libev for the Cassandra driver's event loop.
RUN apt-get update && apt-get install -y --no-install-recommends curl libev4 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# NO_EXTENSIONS lets cassandra-driver install without a C compiler if no wheel matches.
ENV CASS_DRIVER_NO_EXTENSIONS=1 CASS_DRIVER_NO_CYTHON=1
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY debounce.lua ./debounce.lua

RUN useradd -r -u 1001 syncpulse
USER syncpulse

EXPOSE 8080
ENV SERVER_PORT=8080
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${SERVER_PORT:-8080} --workers 1"]
