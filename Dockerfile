# Use an official Python image
FROM python:3.10-slim


RUN apt-get update && apt-get install -y \
    gcc \
    libglib2.0-0 \
    git \
    openssh-client \
    wget \
    openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Add GitHub to known_hosts for SSH cloning
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Install dependencies (including private git repo)
COPY requirements.txt ./
RUN --mount=type=ssh pip install --upgrade pip
RUN --mount=type=ssh pip install -r requirements.txt

COPY . /app

CMD ["python", "main.py"]
