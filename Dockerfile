FROM python:3.12-slim

# udev/serial group access is handled by device mapping in compose
RUN useradd --create-home --uid 1000 meshcored

WORKDIR /app

COPY pyproject.toml ./
COPY meshcore_daemon ./meshcore_daemon
RUN pip install --no-cache-dir .

RUN mkdir -p /data/logs && chown -R meshcored:meshcored /data

USER meshcored
ENV MESHCORED_DATA_DIR=/data
VOLUME ["/data"]

# gRPC API + MeshCore TCP proxy
EXPOSE 50051 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-m", "meshcore_daemon.healthcheck"]

ENTRYPOINT ["meshcore-daemon"]
