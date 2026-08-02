FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

ENV INGEST_WATCH_DIR=/downloads \
    INGEST_JELLYFIN_ROOT=/jellyfin \
    INGEST_WHISPER_MODEL=small.en \
    INGEST_WHISPER_DEVICE=cpu \
    INGEST_POLL_SECONDS=30 \
    HF_HUB_CACHE=/cache/hub

CMD ["python", "-u", "ingest.py"]
