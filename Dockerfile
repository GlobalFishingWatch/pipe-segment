# ---------------------------------------------------------------------------------------
# BUILDER (install dependencies)
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc g++ build-essential git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /install

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --prefix=/install -r requirements.txt

# ---------------------------------------------------------------------------------------
# PRODUCTION IMAGE
# ---------------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS prod

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /opt/project

# COPY DEPENDENCIES
COPY --from=builder /install /usr/local

# APACHE BEAM INTEGRATION
COPY --from=apache/beam_python3.12_sdk:2.71.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

# INSTALL PACKAGE
COPY . /opt/project
RUN pip install --no-cache-dir --no-deps . && \
    rm -rf /root/.cache/pip && \
    rm -rf /opt/project/*

# Temporary until assets packaged properly
COPY ./assets /opt/project/assets

# ---------------------------------------------------------------------------------------
# DEVELOPMENT IMAGE
# ---------------------------------------------------------------------------------------
FROM builder AS dev

WORKDIR /opt/project

COPY . /opt/project
RUN pip install -e .[lint,dev,build] && \
    pip install -r requirements-test.txt

# ---------------------------------------------------------------------------------------
# TEST IMAGE
# ---------------------------------------------------------------------------------------
FROM prod AS test

COPY ./tests /opt/project/tests
COPY ./requirements-test.txt /opt/project/
RUN pip install -r requirements-test.txt