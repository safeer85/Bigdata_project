# ONE custom Spark image, used for the master, the worker and both submit
# containers (SPEC 7.1).
#
# Why one image and not three: PySpark serialises Python objects between the
# driver and the executors. If the driver ran Python 3.11 and an executor ran
# Python 3.8, `applyInPandasWithState` fails at runtime with an opaque pickle
# error. Building every Spark role from the same image makes that impossible.
FROM apache/spark:3.5.3-python3

USER root

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/fleet \
    SPARK_VERSION=3.5.3 \
    SCALA_BINARY=2.12

# --- Kafka connector jars ---------------------------------------------------
# Baked into the image rather than resolved with `--packages` at submit time.
# `--packages` makes every container start depend on Maven Central being
# reachable and on a warm Ivy cache; baking them in means `make up` works on a
# flaky conference wifi and the versions are pinned in git.
#
# Versions are the exact transitive set Spark 3.5.3 expects: kafka-clients 3.4.1
# and commons-pool2 2.11.1. Mixing in a different kafka-clients is the classic
# NoSuchMethodError in Structured Streaming.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar; \
    curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# `common` is mounted read-only at runtime too, but copying it in means the image
# is self-contained for executors, which do not see the compose bind mounts on a
# multi-host cluster.
COPY common/ /opt/fleet/common/
COPY streaming/ /opt/fleet/streaming/

RUN mkdir -p /lake /shared /opt/fleet \
    && chown -R 185:0 /lake /shared /opt/fleet

USER 185
WORKDIR /opt/fleet
