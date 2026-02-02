# Copyright 2024 - 2026 Dave Hall, Skwashd Services https://gata.works, MIT License

FROM public.ecr.aws/chainguard/python:latest-dev AS builder

ENV LANG=C.UTF-8
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/venv/bin:$PATH"
ENV UV_PROJECT_ENVIRONMENT="/app/venv"

COPY . /app

USER root

# scikit_learn requires scipy, which in turn requires numpy, which in turn requires gfortran, linux-headers and openblas 🙄
RUN set -ex; \
    apk add --no-cache \
        gfortran \
        linux-headers \
        openblas-dev; \
    uv sync --no-dev --frozen --compile-bytecode --directory /app; \
    rm -rf /var/cache/apk/*

FROM public.ecr.aws/chainguard/wolfi-base:latest AS packages
RUN set -ex; \
    apk add --no-cache \
        libgomp \
        openblas; \
    rm -rf /var/cache/apk/*


FROM public.ecr.aws/chainguard/python:latest

LABEL org.opencontainers.image.source=https://github.com/gata-router/data-prep
LABEL org.opencontainers.image.description="Gata router ticket data preparation task."
LABEL org.opencontainers.image.licenses=MIT

ENV PYTHONUNBUFFERED=1
ENV PATH="/app/venv/bin:$PATH"

USER root

COPY --from=builder /app /app

COPY --from=packages /usr/lib/libgomp.so.* /usr/lib/libopenblas* /usr/lib/
COPY --from=packages /var/lib/db/sbom/libgomp-*.spdx.json /var/lib/db/sbom/openblas-*.spdx.json /var/lib/db/sbom/

WORKDIR /app

ENTRYPOINT ["python", "prepare.py"]