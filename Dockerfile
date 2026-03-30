# ============================
# Base image
# ============================
FROM python:3.12-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc \
      libglib2.0-0 \
      git \
      openssh-client \
      wget \
      procps \
      openjdk-17-jre-headless \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Silence root warning from pip
ENV PIP_ROOT_USER_ACTION=ignore PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app


# ============================
# App setup
# ============================
WORKDIR /app

# Python deps
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App code + env
COPY . /app
COPY .env /app/.env.docker

# Keep Ivy happy and mirror env knobs
ENV ENV_TYPE=docker \
    SPARK_IVY_PATH=/tmp/.ivy2

CMD ["python", "main.py"]
