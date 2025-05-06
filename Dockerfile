# ---------------------------------------------------------------------------------------
# BASE
# ---------------------------------------------------------------------------------------
FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest-python3.8 AS base

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.8_sdk:2.56.0 /opt/apache/beam /opt/apache/beam

# Install SDK. (needed for Python SDK)
RUN pip install --no-cache-dir apache-beam[gcp]==2.56.0

# Install application dependencies
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]

# ---------------------------------------------------------------------------------------
# PROD
# ---------------------------------------------------------------------------------------
FROM base AS prod

# Install app package
COPY . /opt/project
RUN pip install .

# ---------------------------------------------------------------------------------------
# DEV
# ---------------------------------------------------------------------------------------
FROM base AS dev

COPY ./requirements/dev.txt ./
COPY ./requirements/test.txt ./

RUN pip install --no-cache-dir -r dev.txt
RUN pip install --no-cache-dir -r test.txt

# Install app package
COPY . /opt/project
ENV PYTHONPATH /opt/project
RUN cd /usr/local/lib/python3.8/site-packages && \
    python /opt/project/setup.py develop

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/bin/bash"]
