FROM python:3.12.10-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY bot/ ./bot/

# cache-bust: 2026-05-21-v64
# Default: run the trading bot
# Railway will use the start command from railway.toml
CMD ["python", "bot/main.py"]
