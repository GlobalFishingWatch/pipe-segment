# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.5.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
from google.cloud import bigquery
from datetime import date
import matplotlib.pyplot as plt

# %matplotlib inline
# -

start_date = date(2018, 8, 1)
end_date = date(2018, 8, 7)
query = f"""
        WITH

        position_messages as (
          SELECT *,
                 ABS(TIMESTAMP_DIFF(LAG(timestamp) OVER
                     (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) next_dt,
                 ABS(TIMESTAMP_DIFF(LEAD(timestamp) OVER
                     (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) prev_dt,
                 TIMESTAMP_TRUNC(timestamp, HOUR) hour,
                 ROW_NUMBER() OVER (PARTITION BY ssvid, receiver, EXTRACT(MINUTE FROM timestamp)
                                    ORDER by ABS(EXTRACT(SECOND FROM timestamp) - 30)) rn
          FROM `pipe_ais_sources_v20190222.normalized_spire_*`
          WHERE _table_suffix BETWEEN "{start_date:%Y%m%d}" AND "{end_date:%Y%m%d}"
            AND lat IS NOT NULL AND lon IS NOT NULL
            AND ABS(lat) <= 90 AND ABS(lon) <= 180
         ),


         distance_from_satellite_table as (
            SELECT
              a.msgid,
              TIMESTAMP_TRUNC(a.timestamp, HOUR) hour,
              st_distance(st_geogpoint(a.lon,a.lat),
              st_geogpoint(c.lon,c.lat))/1000 distance_from_sat_km,
              altitude/1000 as sat_altitude_km,
              a.receiver receiver,
              c.lat as sat_lat,
              c.lon as sat_lon
            FROM
              position_messages a
            LEFT JOIN (
              SELECT
                norad_id,
                receiver
              FROM
                `world-fishing-827.gfw_research_precursors.norad_to_receiver_v20200127` ) b
            ON a.receiver = b.receiver
            LEFT JOIN (
              SELECT
                avg(lat) lat,
                avg(lon) lon,
                avg(altitude) altitude,
                timestamp,
                norad_id
              FROM
                `satellite_positions_v20190208.satellite_positions_one_second_resolution_*`
             WHERE _table_suffix BETWEEN "{start_date:%Y%m%d}" AND "{end_date:%Y%m%d}"
              GROUP BY
                norad_id, timestamp) c
            ON a.timestamp = c.timestamp
            AND b.norad_id = c.norad_id
            ),

        median_dist_from_sat as

        (
        select hour, receiver, avg(distance_from_sat_km) avg_distance_from_sat_km,
        med_dist_from_sat_km from
        (select hour, receiver, distance_from_sat_km,
        percentile_cont(distance_from_sat_km, 0.5)
        over (partition by receiver, hour) AS med_dist_from_sat_km
        from distance_from_satellite_table)
        group by hour, receiver, med_dist_from_sat_km
        ),


        base AS (
          SELECT ssvid,
                 hour,
                 timestamp,
                 receiver,
                 lat,
                 lon,
                 speed,
                 course
          FROM position_messages
          WHERE speed between 5 AND 10
            AND lat is not null
            AND lon is not null
            AND course is not null
            AND course != 360
            AND speed  < 102.3
            AND abs(lat) <= 90
            AND abs(lon) <= 180
            AND receiver_type = 'satellite'
            AND type != 'AIS.27'
            -- AND source = 'spire'
            AND rn = 1 -- only 1 point per ssvid, receiver pair per minute
        ),
        hours as (
          SELECT receiver, hour, COUNT(*) pings
          FROM base
          GROUP BY receiver, hour
        ),
        pairs AS (
          SELECT
            ssvid,
            hour,
            timestamp_diff(b.timestamp,
            a.timestamp, millisecond) / 1000.0 AS dt, -- dt is in seconds
            a.lat AS lat1, b.lat AS lat2,
            a.lon AS lon1, b.lon AS lon2,
            0.5 * (a.speed + b.speed) AS speed,
            0.5 * (a.course + b.course) AS course,
            a.receiver AS receiver1, b.receiver AS receiver2,
            ROW_NUMBER() OVER (PARTITION BY ssvid, a.receiver, b.receiver, hour
                               ORDER BY timestamp_diff(b.timestamp, a.timestamp, millisecond)) rn
          FROM base AS a
          JOIN base AS b
          USING (hour, ssvid) -- Join using hour limits the range and chops off offsets at edges
          WHERE a.receiver != b.receiver
           AND ABS(timestamp_diff(b.timestamp, a.timestamp, millisecond) / 1000.0) < 600
           AND cos(3.14159 / 180 * (a.course - b.course)) > 0.8 -- very little difference in course
        ),
        -- This collects ping pairs with timestamp withing 10 minutes of each other.
        -- The two pairs also have similar course (from above), so we expect that
        -- boats are "well behaved" over this period.
        close_pairs AS (
            SELECT *,
                   (lon2 - lon1) * cos(0.5 * (lat1 + lat2) * 3.14159 / 180) * 60 AS dx,
                   (lat2 - lat1) * 60 AS dy,
                   SUM(IF(rn = 1, 1, 0)) over(partition by receiver1, receiver2) AS pair_count
            FROM pairs
            WHERE abs(dt) < 600
              AND rn = 1  -- one ping per hour for each (ssvid, receiver1, receiver2) combo
        ),

        _distances_1 AS (
          SELECT * except (pair_count, dx, dy),
                 SQRT(dx * dx + dy * dy) AS distance,
                 ATAN2(dx, dy) AS implied_course_rads
          FROM close_pairs
          WHERE pair_count >= 10
        ),
        _distances_2 AS (
          SELECT *,
                 COS(course  * 3.14159 / 180 - implied_course_rads) AS cos_delta
          FROM _distances_1
        ),
        distances AS (
            SELECT * except(distance),
                   -- `sign` here: case where boats implied course and course are ~180 deg apart
                   SIGN(cos_delta) * distance AS signed_distance
            FROM _distances_2
            -- only use cases where implied course ~agree or are ~opposite
            WHERE ABS(cos_delta) > 0.8
        ),
        -- Compute the expected dts
        delta_ts AS (
            SELECT *, signed_distance / speed * 60 * 60 AS expected_dt,
            FROM distances
        ),
        grouped AS (
          SELECT *
          FROM (
            SELECT hour,
                   percentile_cont(dt - expected_dt, 0.5)
                   over (partition by receiver1, receiver2, hour) AS dt,
                   receiver1,
                   receiver2
            FROM delta_ts
          )
        GROUP BY receiver1, receiver2, dt, hour
        ),
        time_offset_by_hour_by_satellite AS (
          SELECT *
          FROM (
            SELECT receiver1 AS receiver,
                   hour,
                   percentile_cont(dt, 0.5) over (partition by receiver1, hour) AS dt
            FROM grouped
          )
          GROUP BY receiver, hour, dt
        ),
        safe_time_offset_by_hour_by_satellite AS (
            SELECT receiver, hour,
                   GREATEST(dt,
                            IFNULL(LAG(dt) OVER(PARTITION BY receiver ORDER BY hour), 0),
                            IFNULL(LEAD(dt) OVER(PARTITION BY receiver ORDER BY hour), 0)), dt
            FROM time_offset_by_hour_by_satellite
        )


        SELECT *
        FROM time_offset_by_hour_by_satellite
        LEFT JOIN hours
        USING (hour, receiver)
        left join median_dist_from_sat
        using(hour, receiver)
        ORDER BY receiver, hour
    """

df = bigquery.Client().query(query).to_dataframe()

# +
plt.figure(figsize=(12, 12))

rcvrs = sorted(set(df.receiver))
for x in rcvrs:
    mask = (x == df.receiver)
    plt.plot(df[mask].hour.values, df[mask].dt)

plt.ylim(-60, 60)
plt.xlim(date(2018, 8, 1), date(2018, 8, 2))
# -

df1 = df[df.hour.dt.day <= 8]
df1[abs(df1.dt) > 30][['receiver', 'pings']].groupby(
    'receiver').sum().sort_values(by='pings', ascending=False)

df.columns
