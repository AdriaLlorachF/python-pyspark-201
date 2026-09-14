FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# La imagen base trae un repo de Yarn con GPG caducado; si no se quita, apt-get update falla
# y Codespaces cae a un contenedor Alpine de recuperación (sin Java ni pip).
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
