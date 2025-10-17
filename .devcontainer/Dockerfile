# Use a stable Debian tag where JDK17 is available
FROM python:3.11-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    JAVA_TOOL_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

# Install Java with a safe fallback
RUN apt-get update && \
    (apt-get install -y --no-install-recommends openjdk-17-jre-headless || apt-get install -y --no-install-recommends default-jre-headless) && \
    apt-get install -y --no-install-recommends curl ca-certificates bash tini procps git && \
    rm -rf /var/lib/apt/lists/*

# (optional) create a dev user; comment out if you prefer root
RUN useradd -m -s /bin/bash -u 1000 vscode
USER vscode
WORKDIR /workspace
