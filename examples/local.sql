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
  [world-fishing-827:pipeline_measures_p_p516_daily.20170923] messages
where mmsi in (538006717, 567532000, 477301500)
order by mmsi, timestamp
LIMIT
  1000
