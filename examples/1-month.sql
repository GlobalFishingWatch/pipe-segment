SELECT
  type,
  mmsi,
  imo,
  shipname,
  shiptype,
  shiptype_text,
  callsign,
  timestamp,
  lon,
  lat,
  speed,
  course,
  heading,
  distance_from_shore,
  distance_from_port,
  tagblock_station,
  gridcode

FROM
  TABLE_DATE_RANGE(
    [world-fishing-827:pipeline_measures_p_p516_daily.],
    TIMESTAMP('2015-01-01'),
    TIMESTAMP('2015-02-01')
    ) AS messages

