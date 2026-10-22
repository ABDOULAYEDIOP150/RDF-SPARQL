# Image de l'API FastAPI (publiée par la CI sur ghcr.io)
FROM python:3.12-slim

WORKDIR /app
COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY kg/ kg/
COPY api/ api/
COPY data/processed/ data/processed/

EXPOSE 8000
CMD ["sh", "-c", "uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
