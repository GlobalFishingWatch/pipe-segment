# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [UNRELEASED]

## v3.4.1 - 2024-04-10

* [PIPELINE-1875](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1875): Ensure 
  to set all fields mode None as NULLABLE

## v3.4.0 - 2024-04-08

* [PIPELINE-1875](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1875): Ensure 
  destination tables of pipe_segment steps are created even without data.

## v3.3.0 - 2023-07-26

### Added

* [PIPELINE-1319](https://globalfishingwatch.atlassian.net/browse/PIPELINE-1319): Adds
  support for runnig the code with `python-3.8` and updated the google sdk from
  `2.40.0` to [2.49.0](https://github.com/apache/beam/releases/tag/v2.49.0).

## v3.2.3 - 2022-08-02

### Changed

* [PIPELINE-946](https://globalfishingwatch.atlassian.net/browse/PIPELINE-946): Changes
  how the `segment_identity_daily_` table was being saved. Issue detected when
  running in back-fill mode, only saves one shard per backfill. Fix implies
  saving one shard per day, priorities `last_timestamp` > `to_ts` > `from_ts`.

## v3.2.2 - 2022-07-21

### Changed

* [PIPELINE-914](https://globalfishingwatch.atlassian.net/browse/PIPELINE-914): Changes
  version of `Apache Beam` from `2.35.0` to [2.40.0](https://beam.apache.org/blog/beam-2.40.0/).

## v3.2.1 - 2022-04-19

### Removed

* [PIPELINE-869](https://globalfishingwatch.atlassian.net/browse/PIPELINE-869): Removes
  deprecated code and type check parameter in segment_identity_daily that let
  read in EXPORT mode.

## v3.2.0 - 2022-03-24

### Changed

* [PIPELINE-807](https://globalfishingwatch.atlassian.net/browse/PIPELINE-807): Changes
  to support Beam `2.35.0`.
  Points to last shipdataprocess SHA commit that supports python3.
  Separates Dockerfile scheduler and worker and create separate images.


## v3.1.1 - 2021-06-23

### Changed

* [PIPELINE-431](https://globalfishingwatch.atlassian.net/browse/PIPELINE-431): Removes
  Travis and its references and uses cloudbuild instead to run the tests.
  Uses [gfw-pipeline](https://github.com/GlobalFishingWatch/gfw-pipeline) as Docker base image.
  Updates `pipe-tools` with update in beam reference when reading schema from json.
  Removes 4 warnings from tests.

## v3.1.0 - 2021-04-29

### Added

* [Data Pipeline/PIPELINE-84](https://globalfishingwatch.atlassian.net/browse/PIPELINE-84):
  Adds support of Apache Beam `2.28.0`.
  Increments Google SDK version to `338.0.0`.
  Fixes tests after update of Beam.

## v3.0.8 - 2020-11-13

### Added

* [Data Pipeline/PIPELINE-155](https://globalfishingwatch.atlassian.net/browse/PIPELINE-155):
  Adds a new data quality step for segment identity metrics.

## v3.0.7 - 2020-10-26

### Added

* [Data Pipeline/PIPELINE-129](https://globalfishingwatch.atlassian.net/browse/PIPELINE-129):
  Adds `segmenter_params` to segment airflow step and `max_gap_size_value` to Airflow variable.

### Changed

* [Data Pipeline/PIPELINE-144](https://globalfishingwatch.atlassian.net/browse/PIPELINE-144):
  Changes `gpsdio-segment` version to use the latest fixed version `0.20.2`.

## v3.0.6 - 2020-10-06

### Changed

* [Data Pipeline/PIPELINE-139](https://globalfishingwatch.atlassian.net/browse/PIPELINE-139):
  Changes the `gpsdio-segment` version to use the latest fixed version, `0.20.1`.

## v3.0.5 - 2020-07-15

### Added

* [GlobalFishingWatch/gfw-eng-task#129](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/129): Added
  * flag to enable or disable the run of the aggregation tables,
  * `segment_info`, `segment_vessel`, `vessel_info`.

## v3.0.4 - 2020-06-18

### Changed

* [GlobalFishingWatch/gfw-eng-task#56](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/56): Changes
    the use of the Airflow Variables `PIPELINE_START_DATE` to the value
    that is stored in `defaults_args` as `start_date`.

## v3.0.3 - 2020-06-11

### Changed

* [GlobalFishingWatch/gfw-eng-task#111](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/111): Changes
  * the version of the `pipe-tools:v3.1.2`.

## v3.0.2 - 2020-04-07

### Changed

* [GlobalFishingWatch/gfw-eng-task#48](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/48): Changes
    Bash Operator to flexible operator.
    version to gpsdio-segment:0.20

## v3.0.1 - 2020-04-07

### Changed

* [GlobalFishingWatch/gfw-eng-tasks#49](https://github.com/GlobalFishingWatch/gfw-eng-tasks/issues/49): Changes
    Pin version of gpsdio-segment to v0.19 non-dev version.

## v3.0.0 - 2020-03-12

### Added

* [GlobalFishingWatch/pipe-segment/pull/101](https://github.com/GlobalFishingWatch/pipe-segment/pull/101): Adds
  - Support new version of gpsdio-segment, but continue to emit old style segments as well for backwards
    compatibility. See PR for details
  - Improve memory usage significantly by cogrouping messages rather than passing as side arguments.
    Also, filter out noise segments before grouping and use more temporary shards on output.
  - Update to pipe-tools 3.1.1 and support Python 3.
  - Upgrade Google SDK to 268 from 232
  - Remove the fixed version of pip to 9

## v1.2.1 - 2019-05-15

### Changed

* [GlobalFishingWatch/GFW-Tasks#1030](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1030): Changes
  - the way we pass the machine type to dataflow so it re-allow us to send the custom machine type.


## v1.2.0 - 2019-05-03

### Changed

* [GlobalFishingWatch/GFW-Tasks#1015](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1015): Changes
  - Updated version of gpsdio-segment in order to include the fix of A vs B messages.

## v1.1.0 - 2019-04-24

### Changed

* [GlobalFishingWatch/GFW-Tasks#1000](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1000): Changes
  - Forces ordering when serializing and deserializing segmenter state each day so that the segmenter state timestamp is correctly calculated.

## v1.0.0 - 2019-03-28

### Added

* [GlobalFishingWatch/GFW-Tasks#991](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/991): Adds
  - version 2.0.0 to pipe-tools that split airflow dependencies from dataflow dependencies. Check the [repo](https://github.com/GlobalFishingWatch/airflow-gfw/tree/develop)

## 0.3.3 - 2019-03-11

### Added

* [GlobalFishingWatch/GFW-Tasks#992](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/992): Adds
  - Fixed issue with a dependency in gpsdio-segment

## 0.3.2 - 2019-03-07

### Added

* [#80](https://github.com/GlobalFishingWatch/pipe-segment/issues/80) and  [#77](https://github.com/GlobalFishingWatch/pipe-segment/issues/77) Take into account if the message is of type A or B to generate the segment. Uses the change done in [GPSDIO version 0.12](https://github.com/SkyTruth/gpsdio-segment/pull/60)
* [#83](https://github.com/GlobalFishingWatch/pipe-segment/pull/83)
  Add vessel_id field to segment_info table
* [#87](https://github.com/GlobalFishingWatch/pipe-segment/issues/87)
  Increase the noise threshold for determination of spoofing, and parameterize
* [GlobalFishingWatch/GFW-Tasks#982](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/982)
  Include width and length of vessels in the segment_info, vessel_info,
  vessel_identity_daily and segment_identity_daily tables
* [GlobalFishingWatch/GFW-Tasks#979](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/979)
  Include the Yearly run mode.
* **DEPRECATED** segment_identity and identity_messages_monthly.


## 0.3.1 - 2018-12-10

* [#66](https://github.com/GlobalFishingWatch/pipe-segment/pull/66)
  Refactor Segment Identity
* [#71](https://github.com/GlobalFishingWatch/pipe-segment/pull/71)
  Add param MOST_COMMON_MIN_FREQ which is used to filter noise values 
  when determinig the most commonly occuring identity value used to
  assign vessel_id
* [#76](https://github.com/GlobalFishingWatch/pipe-segment/pull/76)
  Ranked vessel_id per segment in segment_vessel table

## 0.2.3 - 2018-09-03

* [#61](https://github.com/GlobalFishingWatch/pipe-segment/pull/61)
  Include additional noise and message count fields in segment_info table 
* [#68](https://github.com/GlobalFishingWatch/pipe-segment/pull/68)
  Bump version of pipe-tools to 0.1.7

## 0.2.2 - 2018-07-06

* [#53](https://github.com/GlobalFishingWatch/pipe-segment/pull/53)
  Improved Vessel ID creation scheme
  vessel_info table

## 0.2.1 - 2018-06-17

* [#50](https://github.com/GlobalFishingWatch/pipe-segment/pull/50)
  Force SSVID to string before segmenting

## 0.2.0 - 2018-05-14

* [#44](https://github.com/GlobalFishingWatch/pipe-segment/pull/44)
  pin pip version to 9.0.3
* [#45](https://github.com/GlobalFishingWatch/pipe-segment/pull/45)
  Change dataflow machine type to increase memory
* [#47](https://github.com/GlobalFishingWatch/pipe-segment/pull/47)
  Update to pipe-tools v0.1.6 

## 0.1.2 - 2018-03-25

* [#40](https://github.com/GlobalFishingWatch/pipe-segment/pull/40)
  Update to pipe-tools v0.1.5
* [#42](https://github.com/GlobalFishingWatch/pipe-segment/pull/42)
  Change ssvid data type to STRING in segment schema

## 0.1.1 - 2018-03-12

* [#35](https://github.com/GlobalFishingWatch/pipe-segment/pull/35)
  Importable Dags.  Update to pipe-tools v0.1.4

## 0.0.1

* Initial release.
