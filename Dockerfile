FROM python:3.12-slim

WORKDIR /app

# gcc required — ta-lib and alpaca-py C extensions need it on python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY bot/ ./bot/

# cache-bust: 2026-05-19-v44
# Default: run the trading bot
# Railway will use the start command from railway.toml
CMD ["python", "bot/main.py"]
