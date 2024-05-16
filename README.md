# Segment pipeline

This repository contains the segment pipeline,
a dataflow pipeline which divides vessel tracks into contiguous "segments",
separating out noise and signals that may come from two or more vessels
which are broadcasting using the same MMSI at the same time.

[configure a SSH-key for GitHub]: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account
[docker official instructions]: https://docs.docker.com/engine/install/
[docker compose plugin]: https://docs.docker.com/compose/install/linux/
[examples]: examples/
[git installed]: https://git-scm.com/downloads
[git workflow documentation]: GIT-WORKFLOW.md
[Makefile]: Makefile
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[Semantic Versioning]: https://semver.org
[scheduler.in]: requirements/scheduler.in
[worker.in]: requirements/worker.in

# How to run

First, make sure you have [git installed], and [configure a SSH-key for GitHub].
Then, clone the repository:
```bash
git clone git@github.com:GlobalFishingWatch/pipe-segment.git
```

## Dependencies

Install Docker Engine using the [docker official instructions] (avoid snap packages)
and the [docker compose plugin]. No other dependencies are required.

## Building docker images

To build the docker image, run:
```bash
docker compose build
```

Remember to re-run this command everytime you update dependencies or modify the code.

## Google Cloud setup

The pipeline reads it's input from (and write its output to) BigQuery,
so you need to first authenticate with your google cloud account inside the docker images.
To do that, you need to run this command and follow the instructions:
```bash
docker compose run gcloud auth application-default login
```

You also need to configure the project:
```bash
docker compose run gcloud config set project world-fishing-827
docker compose run gcloud auth application-default set-quota-project world-fishing-827
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs.

Wtih `docker compose run dev --help` you can see the available processes:
```bash
$ docker compose run dev --help
Available Commands
  segment                     run the segmenter in dataflow
  segment_identity_daily      generate daily summary of identity messages
                              per segment
  segment_vessel_daily        generate daily vessel_ids per segment
  segment_info                create a segment_info table with one row
                              per segment
  vessel_info                 create a vessel_info table with one row
                              per vessel_id
  segment_vessel              Create a many-to-many table mapping between
                              segment_id, vessel_id and ssvid
```

If you want to know the parameters of one of the processes, run for example:
```shell
docker compose run dev segment --help
```

# How to contribute

The pipeline is only tested on python 3.8 for the moment.
Make sure you have that version installed.
The [Makefile] should ease the development process.

Create a virtual environment:
```shell
python3.8 -m venv .venv
. .venv/bin/activate
```

Install dependencies:
```shell
make install
```

Run unit tests:
```shell
make test
```

Alternatively, you can run the unit tests inside the docker container:
```shell
make testdocker
```

Run all tests including ones that hit some GCP API
```shell
make testdocker-all
```

## Git Workflow

Please refer to our [git workflow documentation] to know how to manage branches in this repository.

## Updating dependencies

Requirements files are compiled with [pip-tools].
Inside [scheduler.in] and [worker.in] production dependencies are specified with restrictions.

The [scheduler.in] is a superset of the [worker.in].
Thus, if you changed something in [worker.in], you must also re-compile [scheduler.in].
This is enforced using a unique Makefile command:
```shell
make requirements-worker
```
If you only modified something in [scheduler.in], you can just run
```shell
make requirements-scheduler
```

If you want to upgrade all dependencies to latest available versions
(compatible with restrictions declared), just run:
```shell
make requirements-upgrade
```

## Schema

To get the schema for an existing bigquery table - use something like this
```shell
bq show --format=prettyjson world-fishing-827:pipeline_measures_p_p516_daily.20170923 | jq '.schema'`
```