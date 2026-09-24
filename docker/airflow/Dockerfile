# Airflow image with a JRE and PySpark, so the batch jobs run in LOCAL mode inside
# the Airflow worker (SPEC 8.1).
#
# Why local mode rather than submitting to the standalone cluster: one simulated
# day is ~35k events, which a single JVM chews through in seconds. Client-mode
# submission would need the driver inside the Airflow container to be routable
# from the Spark executors, which is exactly the networking problem that wastes an
# afternoon in a Docker Compose demo. Recorded in docs/decisions.md.
FROM apache/airflow:2.10.2-python3.11

USER root

# PySpark needs a JRE. headless keeps the layer small (no X11 deps).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=/opt/fleet

USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root
# The entrypoint is COPIED into the image, not bind-mounted from the host.
# Bind-mounting a single file over a path that does not exist in the image fails
# on Docker Desktop, and a Windows checkout could hand the container a CRLF
# script that bash refuses to run. It lives under /opt/fleet/bin, a directory
# this image already owns.
RUN mkdir -p /opt/fleet/bin /lake /landing /shared /reports
COPY docker/airflow/airflow_entrypoint.sh /opt/fleet/bin/airflow_entrypoint.sh
COPY common/ /opt/fleet/common/
COPY batch/ /opt/fleet/batch/
RUN chmod +x /opt/fleet/bin/airflow_entrypoint.sh \
    && chown -R airflow:root /opt/fleet /lake /landing /shared /reports

USER airflow
