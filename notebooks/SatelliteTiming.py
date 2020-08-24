# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import datetime as dt_
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import gridspec
import matplotlib.dates as mdates
import numpy as np
import pyseas
from pyseas import maps, styles
from pyseas.contrib import plot_tracks
from skimage import morphology

# %matplotlib inline
# -

# # Estimating Satellite Timing Error
#
# We estimate the satellite timing error by looking at pings from the same vessel
# that are close in time and received by different satellites. The expected time
# difference ($\Delta t$) is computed by looking at the distance between the two point divided
# by the reported speed. The sign of $\Delta t$ is determing by looking at the relationship
# between the reported course and the course implied by the pair of pings. This gives us 
# a mutiples estimates of the relative clock error between pairs of satellites and take
# a median to arrive at an estimated relative clock error for each pair of satellites.
#
# We then take the median clock error between any single satellite and all other satellites
# as the clock error for that satellite. Effectively assuming that the median clock value of
# all the satellites is correct. 
#
# This only is applicable to the set of Spire data as we don't have the time
# tagblock information to disambiguate the Orbcom satellites.

# +
core_template = '''
WITH

position_messages as (
  SELECT *,
         ABS(TIMESTAMP_DIFF(LAG(timestamp) OVER 
             (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) next_dt,
         ABS(TIMESTAMP_DIFF(LEAD(timestamp) OVER 
             (PARTITION BY ssvid ORDER BY timestamp), timestamp, SECOND)) prev_dt,
         TIMESTAMP_TRUNC(timestamp, HOUR) hour,
         ROW_NUMBER() OVER (PARTITION BY ssvid, receiver, EXTRACT(MINUTE FROM timestamp)) rn
  FROM `pipe_ais_sources_v20190222.normalized_spire_*`
  WHERE _table_suffix BETWEEN '{start_date:%Y%m%d}' AND '{end_date:%Y%m%d}'
    AND lat IS NOT NULL AND lon IS NOT NULL 
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
    AND source = 'spire' 
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
    timestamp_diff(b.timestamp, a.timestamp, millisecond) / 1000.0 AS dt, -- dt is in seconds
    a.lat AS lat1, b.lat AS lat2,
    a.lon AS lon1, b.lon AS lon2,
    0.5 * (a.speed + b.speed) AS speed,
    0.5 * (a.course + b.course) AS course,
    a.receiver AS receiver1, b.receiver AS receiver2,
    ROW_NUMBER() OVER (PARTITION BY ssvid, a.receiver, b.receiver, hour 
                       ORDER BY timestamp_diff(b.timestamp, a.timestamp, millisecond)) rn
  FROM base AS a
  JOIN base AS b
  USING (hour, ssvid) -- Joining using hour limits the range and chops off some offsets at edges
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
      AND rn = 1  -- only use one ping per hour for each (ssvid, receiver1, receiver2) combo
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
           -- `sign` here takes care of case where boats implied course and course are ~180 deg apart
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
           percentile_cont(dt - expected_dt, 0.5) over (partition by receiver1, receiver2, hour) AS dt,
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


'''

offset_template = core_template + '''
SELECT * 
FROM time_offset_by_hour_by_satellite
LEFT JOIN hours 
USING (hour, receiver)
ORDER BY receiver, hour
'''

bad_id_template = core_template + '''
select msgid  
FROM position_messages 
JOIN time_offset_by_hour_by_satellite
USING (hour, receiver)
-- bad msgids are returned when:
-- satellite is off by at least 10s AND
-- satellite is off by over 5 minutes OR more than half the gap to the prev/next point
WHERE NOT ABS(dt) < 30 AND (ABS(dt) > 300 OR ABS(dt) > prev_dt / 2 OR ABS(dt) > next_dt / 2)
'''

# +
start_date = dt_.date(2020, 1, 1)
end_date = start_date + dt_.timedelta(days=10)
query = offset_template.format(start_date=start_date, end_date=end_date)
offsets_1 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

start_date = dt_.date(2019, 7, 1)
end_date = start_date + dt_.timedelta(days=10)
query = offset_template.format(start_date=start_date, end_date=end_date)
offsets_2 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

start_date = dt_.date(2019, 1, 1)
end_date = start_date + dt_.timedelta(days=10)
query = offset_template.format(start_date=start_date, end_date=end_date)
offsets_3 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

start_date = dt_.date(2018, 7, 1)
end_date = start_date + dt_.timedelta(days=10)
query = offset_template.format(start_date=start_date, end_date=end_date)
offsets_4 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')
# -

offsets = pd.concat([offsets_1, offsets_2, offsets_3, offsets_4])

# ## How Does Threshold Affect Message Counts
#
# We look at how many messages we keep versus clock error threshold in the plot below.
# The main takeaway here is that above ~20s, we keep nearly all messages (>99%), but
# below that the fraction of messages kept falls off rapidly. 
#
# We would like the threshold to be below one minute,
# thin points to 1 per minute later in the pipeline. Any cutoff below ~30s should eliminate
# virtually all points getting reversed in time. If necessary a cutoff between 30 and ~40
# seconds would be acceptable, since it would eliminate most reversed time points since
# two satellites with unlikely clock errors would need to be involved. Above that the
# amount of reversed points would likely start to climb. Note that reversed points are not
# the sole problem, they are just the most extreme example. Unrealistic implied speeds
# can creep in without point reversal.

pings = offsets.pings.values
pings[np.isnan(pings)] = 0
plt.figure(figsize=(12, 8))
_ = plt.hist(abs(offsets.dt), bins=100, weights=pings,
             density=True, range=(0, 60), cumulative=True)
plt.hlines(0.99, 0, 60, linewidth=0.5)
plt.grid(axis='x', linewidth=0.5)
plt.ylim(0, 1)
plt.xlim(0, 60)
plt.xlabel('clock error')
plt.ylabel('cumulative pings')

# Another way to visualize this is to look at the time series for the various satellites. 
# The clock errors cluster between about -10s and 10s, but there are excursions well outside
# this range, some of them prolonged.

receivers = sorted(set(offsets_4.receiver))
fig = plt.figure(figsize=(20, 12))
x = mdates.date2num(offsets_4.hour.values)
for r in receivers:
    mask = (offsets_4.receiver == r)
    df = offsets_4[mask]
    plt.plot(x[mask], df.dt.values)
    plt.ylim(-120, 120)
plt.grid(axis='y')
plt.xlabel('hour')
plt.ylabel('clock error (s)')
plt.gca().xaxis_date()
plt.title('estimated clock error for one 11 day period')
plt.xlim(min(x), max(x))

# ## Example Tracks with Clock Offset Effects

# +
template = core_template + '''
,

ssvid_list AS (
  SELECT ssvid FROM (
    SELECT ssvid, AVG(IF(abs(dt) > 1000, 1, 0)) frac, count(*) pings
    FROM time_offset_by_hour_by_satellite
    JOIN position_messages
    USING (hour, receiver)
    GROUP BY ssvid
  )
  WHERE -- (frac > 0.05
    --AND pings > 100)
    -- OR 
     ssvid IN ('303272000', '503702600', '503752700')
  ORDER BY FARM_FINGERPRINT(ssvid)
  LIMIT 10
)

SELECT ssvid, timestamp, lat, lon, dt, prev_dt, next_dt
FROM safe_time_offset_by_hour_by_satellite
LEFT JOIN position_messages 
USING (hour, receiver)
WHERE ssvid IN (SELECT * FROM ssvid_list)
ORDER BY ssvid, timestamp
'''

start_date = dt_.date(2018, 12, 15)
end_date = start_date + dt_.timedelta(days=30)

query = template.format(start_date=start_date, end_date=end_date)
extreme_df = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')


# -

def plot_example(df, ssvid, t_range=None):
    fig = plt.figure(figsize=(20, 20))
    with pyseas.context(styles.light):
        data = df[df.ssvid == ssvid]
        good = abs(data.dt) < 30
        
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 1, 1])

        projection_info = plot_tracks.find_projection(data.lon, data.lat, abs_delta=0.01)
        ax = maps.create_map(gs[0], projection=projection_info.projection, 
                                    extent=projection_info.extent)
        maps.add_land()
        ax.plot(data.lon.values[:], data.lat.values[:], 'k.-', linewidth=0.2, transform=maps.identity)
        ax.plot(data.lon.values[good], data.lat.values[good], 'b.-', linewidth=0.5, transform=maps.identity)
        ax.plot(data.lon.values[~good], data.lat.values[~good], 'r.', transform=maps.identity)
        ax.set_title(ssvid)
        
        x = mdates.date2num(data.timestamp.values)
        xc = mdates.date2num([t + np.timedelta64(int(round(dt)), 's') 
                              for (t, dt) in zip(data.timestamp.values, data.dt.values)])

        ax = plt.subplot(gs[1])
        ax.plot(x[good], data.lon.values[good], 'b.-')
        ax.plot(x[~good], data.lon.values[~good], 'r.')
#         ax.plot(xc[~good], data.lon.values[~good], 'g.')
        if t_range:
            ax.set_xlim(*t_range)
        ax.set_xticks([]) 
        ax.set_ylabel('longitude')
        
        ax = plt.subplot(gs[2])
        ax.plot(x[good], data.lat.values[good], 'b.-')
        ax.plot(x[~good], data.lat.values[~good], 'r.')
#         ax.plot(xc[~good], data.lat.values[~good], 'g.')
        if t_range:
            ax.set_xlim(*t_range)  
        ax.xaxis_date()
        ax.set_ylabel('latitude')


# Here we show some examples with offsets on the order of an hour. The raw track is shown with light 
# black lines and you can see where offset causes unrealistic tracks. The corrected track is shown in 
# blue and the dropped points are shown in red.
#
# The offset points appear as a characteristic parallel track when viewed in a lat/lon versus timestamp
# plot.

plot_example(extreme_df, '303272000', t_range=(dt_.datetime(2019, 1, 8, 17), dt_.datetime(2019, 1, 8, 20)))  

plot_example(extreme_df, '503702600', t_range=(dt_.datetime(2019, 1, 8, 23), dt_.datetime(2019, 1, 9, 3)))

plot_example(extreme_df, '503752700', t_range=(dt_.datetime(2019, 1, 7, 23), dt_.datetime(2019, 1, 8, 1)))

# ### Why not just correct the results?
# 1. Offset is sometimes a jump, so there would be edge cases.
# 2. Correction starts to break down for large offsets.

# ## So what do offsets between 30 seconds and 1 minute look like?
#
# For these shorter intervals, the results are not apparent in the lat/lon versus timestamp plot
# so we turn to looking at implied speed instead. Even then, it's difficult to find examples, but
# here is one where we see some extra jumps in the implied speed versus the uncorrected data.

# +
template = core_template + '''
,

ssvid_list AS (
  SELECT ssvid FROM (
    SELECT ssvid, AVG(IF(abs(dt) BETWEEN 30 AND 60, 1, 0)) frac, 
                  AVG(IF(abs(dt) > 60, 1, 0)) frac2, count(*) pings
    FROM time_offset_by_hour_by_satellite
    JOIN position_messages
    USING (hour, receiver)
    GROUP BY ssvid
  )
  WHERE (frac > 0.05 and frac2 = 0
    AND pings > 100)
  ORDER BY FARM_FINGERPRINT(ssvid)
  LIMIT 10
)

SELECT ssvid, timestamp, lat, lon, dt, prev_dt, next_dt
FROM safe_time_offset_by_hour_by_satellite
LEFT JOIN position_messages 
USING (hour, receiver)
WHERE ssvid IN (SELECT * FROM ssvid_list)
ORDER BY ssvid, timestamp
'''

start_date = dt_.date(2018, 12, 15)
end_date = start_date + dt_.timedelta(days=30)

query = template.format(start_date=start_date, end_date=end_date)
moderate_df = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')


# -

def plot_moderate_example(df, ssvid, t_range=None):
    fig = plt.figure(figsize=(20, 20))
    with pyseas.context(styles.light):
        data = df[df.ssvid == ssvid]
        good1 = abs(data.dt) < 30
        good2 = abs(data.dt) < 60
        last = data.timestamp.values[0]
        thin_mask = [True]
        for v in data.timestamp.values[1:]:
            delta = (v - last) / np.timedelta64(1, 's')
            if delta >= 60:
                thin_mask.append(True)
                last = v
            else:
                thin_mask.append(False)
        thin_mask = np.array(thin_mask)
        thin_mask &= good2
        
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 1, 1])

        projection_info = plot_tracks.find_projection(data.lon, data.lat, abs_delta=0.01)
        ax = maps.create_map(gs[0], projection=projection_info.projection, 
                                    extent=projection_info.extent)
        maps.add_land()
        ax.plot(data.lon.values[thin_mask], data.lat.values[thin_mask], 'k.-', linewidth=0.2, 
                transform=maps.identity)
        ax.plot(data.lon.values[good1 & thin_mask], data.lat.values[good1 & thin_mask], 'b.-', 
                linewidth=0.5, transform=maps.identity)
        ax.plot(data.lon.values[~good1 & thin_mask], data.lat.values[~good1 & thin_mask], 'r.',
                transform=maps.identity)
        ax.set_title(ssvid)
        
        x = mdates.date2num(data.timestamp.values )
        
        tc = np.array([t + np.timedelta64(int(round(dt)), 's') 
                              for (t, dt) in zip(data.timestamp.values, data.dt.values)])
        xc = mdates.date2num(tc)


        
        def implied_speed(x, df):
            dist = np.hypot(df.lat[1:].values - df.lat[:-1].values,
                            np.cos(np.deg2rad(df.lat[1:].values)) * 
                            (df.lon[1:].values - df.lon[:-1].values)) # lots of room for imp
            return 60 * dist / (1e-10 + (x[1:] - x[:-1]) / 
                           np.timedelta64(1, 's') / (60 * 60))

        ax = plt.subplot(gs[1])
        ax.plot(x[thin_mask][1:], implied_speed(data.timestamp.values[thin_mask], data[thin_mask]), 'r.-')
        if t_range:
            ax.set_xlim(*t_range)
        ax.set_xticks([]) 
        ax.set_ylabel('implied speed')
        ax.set_ylim(0, 20)
        ax.set_title('cutoff of 60s')
        
        ax = plt.subplot(gs[2])
        ax.plot(x[good1 & thin_mask][1:], implied_speed(data.timestamp.values[good1 & thin_mask], 
                                                        data[good1 & thin_mask]), 'b.-')
        if t_range:
            ax.set_xlim(*t_range)  
        ax.xaxis_date()
        ax.set_ylabel('implied speed')
        ax.set_ylim(0, 20)
        ax.set_title('cutoff of 30s')

# It is difficult to find examples where a cutoff of 30 is different versus a cutoff of 60. Here's one
# however.

plot_moderate_example(moderate_df, '232012919')

# ## Can we examine implied histogram with different cutoffs for removal?

# +
template = core_template + '''
,

tagged_msgs AS (
SELECT ssvid, timestamp, lat, lon, dt, prev_dt, next_dt,
FROM safe_time_offset_by_hour_by_satellite
LEFT JOIN position_messages 
USING (hour, receiver)
WHERE ABS(dt) < {max_dt}
ORDER BY ssvid, timestamp
),

prethinned AS (
SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY ssvid, EXTRACT(MINUTE FROM timestamp)
                          ORDER BY ABS(EXTRACT(SECOND FROM timestamp) - 30)) rn
FROM tagged_msgs
),

w_implied_speed AS (
  SELECT *,
         60 * (60 * 60) * SQRT(
             (lat - LAG(lat) OVER (PARTITION BY ssvid ORDER BY timestamp)) * 
             (lat - LAG(lat) OVER (PARTITION BY ssvid ORDER BY timestamp)) +
             COS(3.14159 / 180 * lat) * (lon - LAG(lon) OVER (PARTITION BY ssvid ORDER BY timestamp)) * 
             COS(3.14159 / 180 * lat) * (lon - LAG(lon) OVER (PARTITION BY ssvid ORDER BY timestamp))
         ) / TIMESTAMP_DIFF(timestamp,
            LAG(timestamp) OVER (PARTITION BY ssvid ORDER BY timestamp), SECOND) implied_speed
  FROM (SELECT * FROM prethinned WHERE rn = 1)
)

SELECT implied_speed implied_speed, COUNT(*) cnt
FROM (SELECT FLOOR(LEAST(implied_speed, 100)) implied_speed FROM w_implied_speed)
WHERE implied_speed IS NOT NULL
GROUP BY implied_speed
ORDER BY implied_speed
'''

start_date = dt_.date(2018, 12, 15)
end_date = start_date + dt_.timedelta(days=30)

query = template.format(start_date=start_date, end_date=end_date, max_dt=60)
implied_speeds_60 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')
# -

query = template.format(start_date=start_date, end_date=end_date, max_dt=30)
implied_speeds_30 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

query = template.format(start_date=start_date, end_date=end_date, max_dt=5)
implied_speeds_5 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

query = template.format(start_date=start_date, end_date=end_date, max_dt=15)
implied_speeds_15 = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

query = template.format(start_date=start_date, end_date=end_date, max_dt=1e6)
implied_speeds_large = pd.read_gbq(query, project_id='world-fishing-827', dialect='standard')

# Here we show two plots showing how the number of pings associated with a given
# implied speed varies with different thresholds. We expect the number of pings
# with unrealistically high speeds to drop as the threshold is lowered, and that is
# indeed what we see. However the number does not drop to zero, likely do to
# a combination of "spoofing" (SSVID used by more than one vessel) and noise.

plt.figure(figsize=(12, 12))
plt.plot(implied_speeds_5.implied_speed, implied_speeds_5.cnt / implied_speeds_15.cnt.sum(), '-', label='15s')
plt.plot(implied_speeds_30.implied_speed, implied_speeds_30.cnt/ implied_speeds_30.cnt.sum(), '-', label='30s')
plt.plot(implied_speeds_60.implied_speed, implied_speeds_60.cnt/ implied_speeds_60.cnt.sum(), '-', label='60s')
plt.plot(implied_speeds_large.implied_speed, implied_speeds_large.cnt/ implied_speeds_large.cnt.sum(), '-', label='no threshold')
plt.yscale('log')
plt.legend()
plt.ylabel('fraction of messages')
plt.xlabel('implied speed')
plt.title('How Threshold Affects Amount of Pings with a Given Implied Speed')
plt.xlim(0, 99)


plt.figure(figsize=(12, 12))
# plt.plot(implied_speeds_30.implied_speed, implied_speeds_15.cnt / implied_speeds_15.cnt 
#          / (implied_speeds_15.cnt.sum() / implied_speeds_15.cnt.sum() ), '-', label='15s')
plt.plot(implied_speeds_30.implied_speed, implied_speeds_30.cnt / implied_speeds_15.cnt 
         / (implied_speeds_30.cnt.sum() / implied_speeds_15.cnt.sum() ), '-', label='30s')
plt.plot(implied_speeds_60.implied_speed, implied_speeds_60.cnt / implied_speeds_15.cnt
         / (implied_speeds_60.cnt.sum() / implied_speeds_15.cnt.sum() ), '-', label='60s')
plt.plot(implied_speeds_large.implied_speed, implied_speeds_large.cnt / implied_speeds_15.cnt
         / (implied_speeds_large.cnt.sum() / implied_speeds_15.cnt.sum() ), '-', 
                                             label='no threshold')
plt.ylim(0.9, 2)
plt.xlim(1, 60)
plt.xlabel('implied speed')
plt.ylabel('fraction relative to 15s')
plt.legend()
plt.title('How Threshold Affects Amount of Pings with a Given Implied Speed')

# ## Conclusion
#
# A threshold between 20s and 40s seems accceptable from both a messages kept
# and from probability of crossover or near crossover. We chose a threshold
# of 30s in the middle of this range.

# +
# import rendered
# rendered.publish_to_github('./SatelliteTiming.ipynb', 
#                            'pipe-segment/notebooks', action='push')
