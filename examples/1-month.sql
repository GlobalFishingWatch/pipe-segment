SELECT
  mmsi,
  TIMESTAMP_TO_USEC(timestamp) / 1000000.0 AS timestamp,
  lat,
  lon,
  speed,
FROM
  TABLE_DATE_RANGE(
    [world-fishing-827:pipeline_measures_p_p516_daily.],
    TIMESTAMP('2015-01-01'),
    TIMESTAMP('2015-02-01')
    ) AS messages

