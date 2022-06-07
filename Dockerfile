FROM gcr.io/world-fishing-827/github.com/globalfishingwatch/gfw-pipeline:latest

# Setup local application dependencies
COPY . /opt/project
RUN pip install cython
RUN pip install -r requirements.txt
RUN pip install -e .

# Setup the entrypoint for quickly executing the pipelines
ENTRYPOINT ["scripts/run.sh"]