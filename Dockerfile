# Dockerfile.initial
FROM python:3.9-slim

# Install Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

# Set Tini as the entrypoint
ENTRYPOINT ["/tini", "--"]

# Prevent Python buffering and set working directory
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
WORKDIR /app

# Install Git and SSH client
RUN apt-get update && apt-get install -y git openssh-client openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Add GitHub to known_hosts so SSH connections can verify the host key
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN --mount=type=ssh pip install --upgrade pip && pip install -r requirements.txt

# Copy the entire application code
COPY . /app

# Expose port 8000 (reserved for a potential future GUI)
EXPOSE 8000

# Default command; can be overridden by docker-compose
CMD ["python", "main.py"]
