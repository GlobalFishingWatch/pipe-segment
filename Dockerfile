FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-bash-pipeline:latest-python3.8

# Install SDK. (needed for Python SDK)
RUN pip install --no-cache-dir apache-beam[gcp]==2.56.0

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.8_sdk:2.56.0 /opt/apache/beam /opt/apache/beam

# Perform any additional customizations if desired
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Temporary. TODO: Use a local test docker image with extra dependencies.
COPY ./requirements/test.txt ./
RUN pip install -r test.txt

# Temporary. TODO: Use a local dev docker image with extra dependencies.
COPY ./requirements/dev.txt ./
RUN pip install -r dev.txt

# Setup local packages
COPY . /opt/project
RUN pip install -e .

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
