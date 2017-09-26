SELECT
  messages.lat        AS lat,
  messages.lon        AS lon,
  messages.speed      AS speed,
  TIMESTAMP_TO_USEC(messages.timestamp)  AS timestamp,
  messages.mmsi       AS mmsi
FROM
  [world-fishing-827:pipeline_measures_p_p516_daily.20170923] messages
LIMIT
  1000
