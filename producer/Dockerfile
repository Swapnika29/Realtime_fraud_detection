FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for confluent-kafka
RUN apt-get update && apt-get install -y build-essential librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
