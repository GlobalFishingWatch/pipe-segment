<h1 align="center" style="border-bottom: none;"> pipe-segment </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-segment">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-segment/branch/develop/graph/badge.svg?token=OO2L9SXVG0">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-segment">
  </a>
</p>

This repository contains the segment pipeline,
a dataflow pipeline which divides vessel tracks into contiguous "segments",
separating out noise and signals that may come from two or more vessels
which are broadcasting using the same MMSI at the same time.

[bigquery-emulator]: https://github.com/goccy/bigquery-emulator
[configure a SSH-key for GitHub]: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account
[docker official instructions]: https://docs.docker.com/engine/install/
[docker compose plugin]: https://docs.docker.com/compose/install/linux/
[examples]: examples/
[git installed]: https://git-scm.com/downloads
[git workflow documentation]: GIT-WORKFLOW.md
[Makefile]: Makefile
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[requirements.txt]: requirements.txt
[requirements/prod.in]: requirements/prod.in
[Semantic Versioning]: https://semver.org


If you are going to be contribuiting, jump directly to the [How to contribute](#how-to-contribute) section.
If you just want to run the pipeline, use the following instructions.

# How to run

First, make sure you have [git installed], and [configure a SSH-key for GitHub].
Then, clone the repository:
```bash
git clone git@github.com:GlobalFishingWatch/pipe-segment.git
```

## Dependencies

Install Docker Engine using the [docker official instructions] (avoid snap packages)
and the [docker compose plugin]. No other dependencies are required.

## Google Cloud setup

The pipeline reads it's input from (and write its output to) BigQuery,
so you need to first authenticate with your google cloud account inside the docker images.

1. Create external volume to share GCP authentication across containers:
```bash
docker volume create --name=gcp
```

2. Run authentication service
```bash
docker compose run gcloud auth application-default login
```

3. Configure the project:
```bash
docker compose run gcloud config set project world-fishing-827
docker compose run gcloud auth application-default set-quota-project world-fishing-827
```

## Building docker image

To build the docker image, run:
```bash
docker compose build
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

The [Makefile] should ease the development process.

## Git Workflow

Please refer to our [git workflow documentation] to know how to manage branches in this repository.

## Setup the environment

Create a virtual environment:
```shell
make venv
. .venv/bin/activate
```

Authenticate to google cloud and set up project (not necessary if you already did it on this machine):
```shell
make gcp
```

Install dependencies:
```shell
make install
```

Run unit tests:
```shell
make test
```

Run unit tests & integration tests (uses [bigquery-emulator]):
```shell
make testintegration
```

Run unit tests and integration inside docker container:
```shell
make testindocker
```

## Updating dependencies

The [requirements.txt] contains all transitive dependencies pinned to specific versions.
This file is compiled automatically with [pip-tools], based on [requirements/prod.in].

Use [requirements/prod.in] to specify high-level dependencies with restrictions.
Do not modify [requirements.txt] manually.

To re-compile dependencies, just run
```shell
make requirements
```

If you want to upgrade all dependencies to latest available versions
(compatible with restrictions declared), just run:
```shell
make upgrade-requirements
```

## Schema

To get the schema for an existing bigquery table - use something like this
```shell
bq show --format=prettyjson world-fishing-827:pipeline_measures_p_p516_daily.20170923 | jq '.schema'`
```