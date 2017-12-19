SELECT
  type,
  mmsi as ssvid,
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
  [world-fishing-827:pipeline_measures_p_p516_daily.20170923] messages
where mmsi in (538006717, 567532000, 477301500)
order by mmsi, timestamp
LIMIT
  1000
