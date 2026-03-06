# ---------------------------------------------------------------------------------------
# BUILDER
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc g++ build-essential git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /install

COPY requirements.txt .

RUN uv pip install --system --upgrade pip && \
    uv pip install --system build && \
    uv pip install --system --prefix=/install -r requirements.txt

COPY pyproject.toml README.md .
COPY src ./src

RUN uv pip install --system --prefix=/install .
# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS prod

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# COPY PYTHON PACKAGES
COPY --from=builder /install /usr/local

# APACHE BEAM INTEGRATION
COPY --from=apache/beam_python3.12_sdk:2.71.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

WORKDIR /opt/project

# Temporary until assets packaged properly
COPY ./assets ./assets

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

WORKDIR /opt/project

COPY . .
RUN uv pip install --system -e .[lint,dev,build] && \
    uv pip install --system -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./requirements-test.txt .
RUN pip install -r requirements-test.txt

COPY ./tests ./tests
