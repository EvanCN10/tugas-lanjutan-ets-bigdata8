FROM python:3.10-slim

# Install system dependencies, Java (required for PySpark), and Docker CLI
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    docker.io \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java Home to the default java symlink folder in Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Set environment defaults
ENV KAFKA_BOOTSTRAP=kafka-broker:9094
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["python", "dashboard/app.py"]
