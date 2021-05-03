# Segment pipeline

This repository contains the segment pipeline, a dataflow pipeline which
 divides vessel tracks into contiguous "segments", separating
out noise and signals that may come from two or more vessels which are
broadcasting using hte same mmsi at the same time

# Running

## Dependencies

You just need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) in your machine to run the
pipeline. No other dependency is required.

## Setup

The pipeline reads it's input from BigQuery, so you need to first authenticate
with your google cloud account inside the docker images. To do that, you need
to run this command and follow the instructions:

```
docker-compose run gcloud auth application-default login
```

## CLI

The pipeline includes a CLI that can be used to start both local test runs and
remote full runs. Just run `docker-compose run pipeline --help` and follow the
instructions there.

## Development and Testing

Run unit tests
  Quick run
  `docker-compose run test tests`

  Run with all tests including ones that hit some GCP API
  `docker-compose run test tests --runslow`

Re-build the docker environment (needed if you modify setup.py or other environmental change)
  `docker-compose build`

You can run the unit tests outside of docker like this
  ` py.test tests`
which may be convenient when debugging stuff.  If you do this then you will need
to clear out the `__pycache__` with
    `rm -rf tests/__pycache__/`

or else you will get an error like this
`ImportMismatchError: ('conftest', '/opt/project/tests/conftest.py',
local('/Users/paul/github/pipe-segment/tests/conftest.py'))`

You can do a local run using a query from BQ in order to get more data to run through it.
Use the second command below to help view the output in sorted order

```console
./scripts/local.sh
cat local-output-00000-of-00001 | jq -s '. | sort_by(.mmsi + .timestamp)'
```

## Schema

To get the schema for an existing bigquery table - use something like this

  `bq show --format=prettyjson world-fishing-827:pipeline_measures_p_p516_daily.20170923 | jq '.schema'`

## Note on the gpsdio-segment dependency

This library depends on the python package [gpsdio-segment](https://github.com/SkyTruth/gpsdio-segment)

We would like to just specify the dependency in setup.py (see the comment in
that file). However, this does not work when installing in the remote worker
in dataflow because there is no git executable on the remote workers.

So instead we download the package tarball in setup.sh and then for local
execution we just pip install from that package, and for remote install we pass
the tarball along via the extra_packages option in parser.py

# License

Copyright 2017 Global Fishing Watch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
