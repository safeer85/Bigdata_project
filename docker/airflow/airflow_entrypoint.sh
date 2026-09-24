#!/usr/bin/env bash
# Bring up Airflow as a single container: migrate, seed the admin user, then run
# the scheduler and the webserver side by side under LocalExecutor.
#
# The stock Airflow compose file uses four containers (init, webserver, scheduler,
# triggerer). For a DAG with nine short tasks that is roughly 1.5 GB of JVM- and
# gunicorn-shaped overhead we do not have on a laptop already running Kafka and
# two Spark applications. LocalExecutor still gives real task parallelism, which
# is what SPEC 8.1 asks for.
set -euo pipefail

echo "[airflow-entrypoint] waiting for the metadata database"
for _ in $(seq 1 60); do
  if airflow db check >/dev/null 2>&1; then break; fi
  sleep 2
done

echo "[airflow-entrypoint] running migrations"
airflow db migrate

echo "[airflow-entrypoint] ensuring admin user exists"
# `|| true`: on a restart the user is already there and `users create` exits 1.
airflow users create \
  --username "${AIRFLOW_ADMIN_USER:-admin}" \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Fleet --lastname Admin \
  --role Admin --email admin@example.com 2>/dev/null || true

# Connection used by the DAG to reach the fleet database through Airflow hooks.
airflow connections delete fleet_postgres >/dev/null 2>&1 || true
airflow connections add fleet_postgres \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST:-postgres}" \
  --conn-port "${POSTGRES_PORT:-5432}" \
  --conn-schema "${POSTGRES_DB:-fleet}" \
  --conn-login "${POSTGRES_USER:-fleet}" \
  --conn-password "${POSTGRES_PASSWORD:-fleet_dev_pw}" >/dev/null

# Stop both processes together if either dies, so the healthcheck fails loudly
# instead of leaving a half-running Airflow that looks up but schedules nothing.
term() {
  echo "[airflow-entrypoint] shutting down"
  kill -TERM "${SCHEDULER_PID:-0}" "${WEB_PID:-0}" 2>/dev/null || true
}
trap term SIGTERM SIGINT

echo "[airflow-entrypoint] starting scheduler"
airflow scheduler &
SCHEDULER_PID=$!

echo "[airflow-entrypoint] starting webserver"
airflow webserver &
WEB_PID=$!

wait -n "${SCHEDULER_PID}" "${WEB_PID}"
term
wait || true
