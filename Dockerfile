# Dockerfile.initial
FROM python:3.9-slim

# Prevent Python buffering and set working directory
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
WORKDIR /app

# Install Git, SSH client, wget, and OpenJDK
RUN apt-get update && apt-get install -y \
    git \
    openssh-client \
    wget \
    openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable (adjust if needed)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Add GitHub to known_hosts so SSH connections can verify the host key
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN --mount=type=ssh pip install --upgrade pip && pip install -r requirements.txt

# Create a directory for jars outside /app so it won't be overwritten, and download the required jars.
RUN mkdir -p /opt/jars && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar \
    -O /opt/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar \
    -O /opt/jars/kafka-clients-3.9.0.jar && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar \
      -O /opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar && \
    ls -la /opt/jars/

# Set PYSPARK_SUBMIT_ARGS to include both jars; comma-separate multiple jars.
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,/opt/jars/kafka-clients-3.9.0.jar,/opt/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar pyspark-shell"

# Copy the entire application code (this will not affect /opt/jars)
COPY . /app

# Expose port 8000 (reserved for a potential future GUI)
EXPOSE 8000

# Use spark-submit so that PYSPARK_SUBMIT_ARGS is honored and the jars are added to Spark's classpath.
CMD ["spark-submit", "--master", "local[*]", "main.py"]
