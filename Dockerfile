FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
      apt-get install -y gcc build-essential && \
      pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src
