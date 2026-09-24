# Shared image for the two simulators and the FastAPI serving layer.
# One image rather than three: they need the same `common` package and the same
# thin dependency set, and a 16 GB laptop should not build three near-identical
# Python layers. The entrypoint differs per service in docker-compose.yml.
FROM python:3.11.10-slim-bookworm

# No apt layer at all. The healthchecks for these services run `python -c` with
# urllib from the standard library rather than installing curl: it keeps the image
# small, and it avoids pinning a Debian point-release version that goes stale as
# soon as the security archive rolls forward (the base image tag is the real pin).

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY docker/app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common/ /app/common/
COPY simulators/ /app/simulators/
COPY api/ /app/api/
COPY scripts/ /app/scripts/

# Run as a non-root user: the containers write only to bind-mounted volumes that
# compose creates, so there is no reason to hold root.
RUN useradd --create-home --uid 10001 fleet \
    && mkdir -p /lake /landing /shared /reports \
    && chown -R fleet:fleet /app /lake /landing /shared /reports
USER fleet

CMD ["python", "-m", "api.main"]
