FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot and API source
COPY bot/ ./bot/
COPY api/ ./api/

# cache-bust: 2026-04-29-v5
# Default: run the trading bot
# Railway will use the start command from railway.toml
CMD ["python", "bot/main.py"]
