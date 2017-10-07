SELECT
  mmsi,
  TIMESTAMP_TO_USEC(messages.timestamp) / 1000000.0  AS timestamp,
  lat,
  lon,
  speed,
  course,
  heading,
  tagblock_station,
  type,
  imo,
  shipname,
  shiptype,
  shiptype_text,
  callsign,
  distance_from_shore,
  distance_from_port,
  gridcode
FROM
  TABLE_DATE_RANGE(
    [world-fishing-827:pipeline_measures_p_p516_daily.],
    TIMESTAMP('2015-01-01'),
    TIMESTAMP('2015-02-01')
    ) AS messages
