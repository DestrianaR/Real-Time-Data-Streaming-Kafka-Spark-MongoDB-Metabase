FROM bitnami/spark:latest

# Install pip and other dependencies
USER root
RUN apt-get update --fix-missing && \
    apt-get install -y python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements.txt and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r /app/requirements.txt

# Copy application code
COPY KafkaSparkStreaming.py /app/KafkaSparkStreaming.py

WORKDIR /app