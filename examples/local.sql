SELECT
  mmsi,
  TIMESTAMP_TO_USEC(messages.timestamp) / 1000000.0  AS timestamp,
  lat,
  lon,
  speed,
  tagblock_station

FROM
  [world-fishing-827:pipeline_measures_p_p516_daily.20170923] messages
where mmsi in (538006717, 567532000, 477301500)
order by mmsi, timestamp
LIMIT
  1000
