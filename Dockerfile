FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-bash-pipeline:latest-python3.8

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Temporary. TODO: Use a test docker image with extra dependencies.
COPY ./requirements/test.txt ./
RUN pip install -r test.txt

# Setup local packages
COPY . /opt/project
RUN pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run.sh"]

