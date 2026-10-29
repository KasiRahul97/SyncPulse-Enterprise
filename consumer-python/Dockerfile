# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN useradd -r -u 1002 companion
USER companion
EXPOSE 8081
ENV SERVER_PORT=8081
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${SERVER_PORT:-8081} --workers 1"]
