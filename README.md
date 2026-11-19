<h1 align="center" style="border-bottom: none;"> pipe-segment </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-segment">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-segment/branch/main/graph/badge.svg?token=OO2L9SXVG0">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.12%20%7C%203.13-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-segment">
  </a>
</p>

This repository contains the segment pipeline,
a dataflow pipeline which divides vessel tracks into contiguous "segments",
separating out noise and signals that may come from two or more vessels
which are broadcasting using the same MMSI at the same time.

[contributing guide]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md
[preparing the development environment]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md#preparing-the-development-environment
[git workflow]: https://github.com/GlobalFishingWatch/python-app-template/blob/main/CONTRIBUTING.md#git-workflow
[python-app-template]: https://github.com/GlobalFishingWatch/python-app-template
[config]: config/
[Makefile]: Makefile

# Development

First, clone the repository:

```shell
git clone https://github.com/GlobalFishingWatch/pipe-segment.git
cd pipe-segment
```

Then, from the cloned repository, follow the [contributing guide]
shared by the repositories created from [python-app-template]:
[preparing the development environment] (Docker setup, installing dependencies)
and the [git workflow].

The [Makefile] exposes the usual targets:
`make docker-build`, `make docker-gcp`, `make docker-shell`, `make test`, `make format`, `make lint`,
among others -- run `make help` for the full list.
No repository-specific system dependency is required.

# Usage

The [config] folder has a sample config file for each command, to run it with `-c`.

### CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs.

With `docker compose run --rm --entrypoint pipe-segment dev --help` you can see the available processes:
```bash
$ docker compose run --rm --entrypoint pipe-segment dev --help
Available Commands
  messages-segmented          segment the messages in dataflow: writes the segmented
                              messages, the fragments and the segments
  segment-identity-daily      generate daily summary of identity messages
                              per segment
  segment-vessel-daily        generate daily vessel_ids per segment
  segment-info                create a segment_info table with one row
                              per segment
  vessel-info                 create a vessel_info table with one row
                              per vessel_id
  segment-vessel              Create a many-to-many table mapping between
                              segment_id, vessel_id and ssvid
```

If you want to know the parameters of one of the processes, run for example:
```shell
docker compose run pipeline messages-segmented --help
```

The commands take kebab-case options, `--bq-in-*` and `--bq-out-*` for the tables, and a required
`--project` (the project that runs the queries and the jobs, and is billed for them) and `--labels`.
The date range is `--start-date` and `--end-date`, and the end date is exclusive. Every command
also accepts its options in a config file with `-c`, and the options in the command line take
precedence. For the Beam commands (`messages-segmented` and `segment-identity-daily`), the config file or the
command line can also have any Apache Beam pipeline option:
```shell
docker compose run pipeline messages-segmented -c config/messages-segmented/example.yaml
docker compose run pipeline messages-segmented -c config/messages-segmented/incremental.yaml
docker compose run pipeline segment-identity-daily -c config/segment-identity-daily/example.yaml
docker compose run pipeline segment-vessel-daily -c config/segment-vessel-daily/example.yaml
docker compose run pipeline segment-vessel -c config/segment-vessel/example.yaml
docker compose run pipeline segment-info -c config/segment-info/example.yaml
docker compose run pipeline vessel-info -c config/vessel-info/example.yaml
```

## Schema

To get the schema for an existing bigquery table - use something like this
```shell
bq show --format=prettyjson world-fishing-827:pipeline_measures_p_p516_daily.20170923 | jq '.schema'`
```
