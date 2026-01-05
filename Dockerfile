FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY config ./config
COPY sql ./sql
COPY docs ./docs
COPY README.md ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.pipeline", "--source", "books", "--pages", "1", "--no-minio"]
