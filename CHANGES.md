# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

* [GlobalFishingWatch/GFW-Tasks#1015](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1015) Updated version of gpsdio-segment in order to include the fix of A vs B messages.

## v1.1.0 - 2019-04-24

### Changed
* [GlobalFishingWatch/GFW-Tasks#1000](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/1000) Forces ordering when serializing and deserializing segmenter state each day so that the segmenter state timestamp is correctly calculated.

## v1.0.0 - 2019-03-28

### Added

* [GlobalFishingWatch/GFW-Tasks#991](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/991)
  Moved to pipe-tools version 2.0.0 that split airflow dependencies from dataflow dependencies. Check the [repo](https://github.com/GlobalFishingWatch/airflow-gfw/tree/develop)

## 0.3.3 - 2019-03-11

* [GlobalFishingWatch/GFW-Tasks#992](https://github.com/GlobalFishingWatch/GFW-Tasks/issues/992) Fixed issue with a dependency in gpsdio-segment

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
