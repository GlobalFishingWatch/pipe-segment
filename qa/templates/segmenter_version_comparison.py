# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3.9.6 ('rad')
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Segmenter Version Comparison
#
# This notebook calculates and visualizes key segment metrics to allow the user to compare a new pipeline to the old one for QA purposes. This was specifically built to compate pipe 3 to pipe 2.5 and is not guaranteed when using different pipeline versions. Even using a new version of pipe 3 may require some changes if column names have changed.
#
# Author: Jenn Van Osdel  
# Last Updated: August 24, 2022

# %%
import pandas as pd
import matplotlib.pyplot as plt

from config import DATASET_OLD, DATASET_NEW_MONTHLY_INT, FRAGMENTS_TABLE, SEGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, SEGMENT_INFO_TABLE

pd.set_option("max_rows", 20)


# %% [markdown]
# ## Data generation
#
# *NOTE: this query is not fully flexible on dates and still assumes all data is within 2020 as hardcoding "2020" into the table suffix decreased query costs by ~95%. You may need to modify the query and check costs if running a period outside of 2020.*

# %%
def make_daily(df):
    df_daily = df.copy().groupby(['date', 'year']).sum().reset_index()
    df_daily['avg_seg_length_h_new'] = df_daily.sum_seg_length_h_new / df_daily.num_segs_distinct_new
    df_daily['avg_seg_length_h_old'] = df_daily.sum_seg_length_h_old / df_daily.num_segs_distinct_old
    df_daily['avg_seg_length_h_diff'] = df_daily.avg_seg_length_h_new - df_daily.avg_seg_length_h_old
    return df_daily



# %%
q = f'''
CREATE TEMP FUNCTION start_date() AS (
  (SELECT DATE(PARSE_TIMESTAMP("%Y%m%d", MIN(_TABLE_SUFFIX)))
  FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`)
);

CREATE TEMP FUNCTION end_date() AS (
  (SELECT DATE(PARSE_TIMESTAMP("%Y%m%d", MAX(_TABLE_SUFFIX)))
  FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`)
);

WITH 

segment_data_new AS (
    SELECT 
    DATE(seg.timestamp) as date,
    EXTRACT(YEAR from DATE(seg.timestamp)) as year,
    seg.ssvid as ssvid,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(frag.last_msg_timestamp, seg.first_timestamp, MINUTE)/60.0) as sum_seg_length_h
    FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*` seg
    JOIN `{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}*` frag
    USING(frag_id)
    GROUP BY date, year, ssvid
),

segment_data_old AS (
      SELECT 
    DATE(timestamp) as date,
    EXTRACT(YEAR from DATE(timestamp)) as year,
    ssvid,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(last_msg_of_day_timestamp, IF(DATE(first_msg_timestamp) >= start_date(), first_msg_timestamp, TIMESTAMP(start_date())), MINUTE)/60.0) as sum_seg_length_h
    FROM `{DATASET_OLD}.{SEGMENTS_TABLE}2020*`
    WHERE ssvid IN (SELECT DISTINCT ssvid FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`)
    AND _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP("%m%d", start_date()) AND FORMAT_TIMESTAMP("%m%d", end_date())
    -- Necessary to filter out segments that have had no positions in 2020
    -- but have not yet closed because a new position isn't reported for
    -- that MMSI for days/weeks/months.
    AND first_msg_of_day_timestamp IS NOT NULL
    GROUP BY date, year, ssvid
),

segment_join AS (
    SELECT
    date,
    year,
    ssvid,
    IFNULL(segs_new.num_segs, 0) AS num_segs_new,
    IFNULL(segs_new.num_segs_distinct, 0) AS num_segs_distinct_new,
    IFNULL(segs_new.sum_seg_length_h, 0) AS sum_seg_length_h_new,
    IFNULL(segs_old.num_segs, 0) AS num_segs_old,
    IFNULL(segs_old.num_segs_distinct, 0) AS num_segs_distinct_old,
    IFNULL(segs_old.sum_seg_length_h, 0) AS sum_seg_length_h_old,
    FROM segment_data_new segs_new
    FULL OUTER JOIN segment_data_old segs_old
    USING(date, year, ssvid)
    ORDER BY ssvid, date
)

SELECT
date,
year,
ssvid,
num_segs_new,
num_segs_distinct_new,
sum_seg_length_h_new,
num_segs_old,
num_segs_distinct_old,
sum_seg_length_h_old,
(num_segs_new - num_segs_old) as num_segs_diff,
(num_segs_distinct_new - num_segs_distinct_old) as num_segs_distinct_diff,
(sum_seg_length_h_new - sum_seg_length_h_old) as sum_seg_length_h_diff,
FROM segment_join
'''

# print(q)
df_segs_daily_by_ssvid = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

df_segs_daily = make_daily(df_segs_daily_by_ssvid)

# Quick checks for duplicate seg_id
assert(df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.num_segs_new != df_segs_daily_by_ssvid.num_segs_distinct_new].shape[0] == 0)
assert(df_segs_daily[df_segs_daily.num_segs_new != df_segs_daily.num_segs_distinct_new].shape[0] == 0)


# %%
def plot_new_vs_old(df, col_prefix, title, ylabel=""):
    fig = plt.figure()
    ax = df[[f'{col_prefix}old']].plot(label='old')
    df[[f'{col_prefix}new']].plot(label='new', ax=ax)
    years = list(df.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    plt.legend()
    plt.title(title)
    plt.ylabel(ylabel)
    return fig, ax



# %%
def plot_diff(df, col_prefix, title, ylabel=""):
    fig = plt.figure()
    ax = df[[f'{col_prefix}diff']].plot(c='green', label='diff')
    years = list(df.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    plt.legend()
    plt.title(title)
    plt.ylabel(ylabel)
    return fig, ax



# %% [markdown]
# ## Daily Stats

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='num_segs_distinct_', 
                      title="Number of segments per day\nAll baby pipe MMSI",
                      ylabel="Number of active segments")


# %%
fig = plot_diff(df_segs_daily, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI",
                ylabel="Number of active segments")

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='sum_seg_length_h_', 
                      title="Total length of segments per day (hours)\nAll baby pipe MMSI",
                      ylabel="Total active segment hours")

# %%
fig = plot_diff(df_segs_daily, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI",
                ylabel="Total active segment hours")

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='avg_seg_length_h_', 
                      title="Average length of active segments per day (hours)\nAll baby pipe MMSI",
                      ylabel="Avg. length of active segments (hour)")

# %%
fig = plot_diff(df_segs_daily, col_prefix='avg_seg_length_h_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI",
                ylabel="Avg. length of active segments (hour)")

# %% [markdown]
# ## MMSI with biggest changes

# %%
q = f'''
WITH

seg_hours_new AS (
  SELECT 
    ssvid,
    seg_id,
    TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 as hours
  FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`
),

seg_hours_old AS (
  SELECT
    ssvid, 
    seg_id,
    TIMESTAMP_DIFF(
        IF(last_timestamp <= '2020-12-31 23:59:59', last_timestamp, TIMESTAMP('2020-12-31 23:59:59')),
        IF(first_timestamp >= '2020-01-01', first_timestamp, TIMESTAMP('2020-01-01'))
        , MINUTE)/60.0 as hours
  FROM `{DATASET_OLD}.{SEGMENT_INFO_TABLE}`
  WHERE (DATE(first_timestamp) <= '2020-12-31' AND DATE(last_timestamp) >= '2020-01-01')
  AND (ssvid IN (SELECT DISTINCT ssvid FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`))
),

ssvid_stats_new AS (
  SELECT 
    ssvid,
    COUNT(*) as num_segs_new,
    COUNT(DISTINCT seg_id) as num_segs_distinct_new,
    SUM(hours) as sum_seg_length_h_new,
    SUM(hours) / COUNT(*) as avg_seg_length_h_new
  FROM seg_hours_new
  GROUP BY ssvid
),

ssvid_stats_old AS (
  SELECT 
    ssvid,
    COUNT(*) as num_segs_old,
    COUNT(DISTINCT seg_id) as num_segs_distinct_old,
    SUM(hours) as sum_seg_length_h_old,
    SUM(hours) / COUNT(*) as avg_seg_length_h_old
  FROM seg_hours_old
  GROUP BY ssvid
)

SELECT *,
num_segs_new - num_segs_old AS num_segs_diff,
num_segs_distinct_new - num_segs_distinct_old AS num_segs_distinct_diff,
sum_seg_length_h_new - sum_seg_length_h_old AS sum_seg_length_h_diff,
avg_seg_length_h_new - avg_seg_length_h_old AS avg_seg_length_h_diff,
ABS(num_segs_new - num_segs_old) AS num_segs_diff_abs,
ABS(num_segs_distinct_new - num_segs_distinct_old) AS num_segs_distinct_diff_abs,
ABS(sum_seg_length_h_new - sum_seg_length_h_old) AS sum_seg_length_h_diff_abs,
ABS(avg_seg_length_h_new - avg_seg_length_h_old) AS avg_seg_length_h_diff_abs,
FROM ssvid_stats_new
FULL OUTER JOIN ssvid_stats_old
USING(ssvid)
'''
# print(q)
df_ssvid_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# %% [markdown]
# #### Number of segments
#
# The overwhelming majority of the drop in the number of segments occurs for `413000000`. The decrease in the number of segments is smaller for the rest of the MMSI but the majority of MMSI have a decrease in segments.
#
# The decrease in the segment hours is more evenly spread over the MMSI with more larger decreases in hours coming from more frequently spoofed MMSI. The difference in total segment hours for an MMSI is well correlated to its difference in number of segments but not to the change in the average segment length. This means that some segments are impacted more than others by the changes to the segmenter.

# %%
df_ssvid_stats[df_ssvid_stats.ssvid != '413000000'].plot.scatter(
        x="sum_seg_length_h_diff",
        y="num_segs_distinct_diff")
plt.xlabel("Total segment hours")
plt.ylabel("Nubmer of segments")
plt.show()

# %%
df_ssvid_stats[df_ssvid_stats.ssvid != '413000000'].plot.scatter(
        x="sum_seg_length_h_diff",
        y="avg_seg_length_h_diff")
plt.xlabel("Total segment hours")
plt.ylabel("Average segment length (hour)")
plt.show()

# %%
df_ssvid_stats.sort_values("num_segs_distinct_diff_abs", ascending=False)[:10][['ssvid', 'num_segs_distinct_new', 'num_segs_distinct_old', 'num_segs_distinct_diff', 'num_segs_distinct_diff_abs']].reset_index(drop=True)

# %% [markdown]
# #### Total segment hours

# %%
df_ssvid_stats.sort_values("sum_seg_length_h_diff_abs", ascending=False)[:20][['ssvid', 'sum_seg_length_h_new', 'sum_seg_length_h_old', 'sum_seg_length_h_diff', 'sum_seg_length_h_diff_abs', 'num_segs_distinct_new', 'num_segs_distinct_old', 'num_segs_distinct_diff', 'num_segs_distinct_diff_abs']].reset_index(drop=True)

# %% [markdown]
# #### Average segment length (hours)

# %%
df_ssvid_stats.sort_values("avg_seg_length_h_diff_abs", ascending=False)[:10][['ssvid', 'avg_seg_length_h_new', 'avg_seg_length_h_old', 'avg_seg_length_h_diff', 'avg_seg_length_h_diff_abs']].reset_index(drop=True)

# %% [markdown]
# ## Daily patterns of 413000000
#
# `413000000` generally dominates the overall pattern so it is interesting to see that and to see what the pattern looks like without `413000000`

# %% [markdown]
# ### The difference in the number of segments and hours of all MMSI vs just `413000000` 
#
# To confirm that `413000000` dominates the pattern. This may not be true for future QA and your copy of this notebook may need to be modified to look deeper.

# %%
fig, ax = plot_diff(df_segs_daily, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI",
                ylabel="Number of active segments")
df_temp = df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid == '413000000'].copy().reset_index(drop=True)
df_temp.num_segs_distinct_diff.plot(c='orange', ax=ax, label='413000000 only')
plt.legend()
plt.show()

# %%
fig, ax = plot_diff(df_segs_daily, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI",
                ylabel="Total active segment hours")
df_temp = df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid == '413000000'].copy().reset_index(drop=True)
df_temp.sum_seg_length_h_diff.plot(c='orange', ax=ax, label='413000000 only')
plt.legend()
plt.show()

# %% [markdown]
# ## Daily Stats WITHOUT `413000000`

# %%
df_segs_daily_no_413000000 = make_daily(df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid != '413000000'])



# %%
fig = plot_new_vs_old(df_segs_daily_no_413000000, col_prefix='num_segs_distinct_', 
                      title="Number of segments per day\nExcluding 413000000",
                      ylabel="Number of active segments")


# %%
fig = plot_diff(df_segs_daily_no_413000000, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nExcluding 413000000",
                ylabel="Number of active segments")


# %%
fig = plot_new_vs_old(df_segs_daily_no_413000000, col_prefix='sum_seg_length_h_', 
                      title="Total length of segments per day (hours)\nExcluding 413000000",
                      ylabel="Total active segment hours")

# %%
fig = plot_diff(df_segs_daily_no_413000000, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nExcluding 413000000",
                ylabel="Total active segment hours")


# %%
fig = plot_new_vs_old(df_segs_daily_no_413000000, col_prefix='avg_seg_length_h_', 
                      title="Average length of active segments per day\nExcluding 413000000",
                      ylabel="Avg. length of active segments (hour)")

# %%
fig = plot_diff(df_segs_daily_no_413000000, col_prefix='avg_seg_length_h_', 
                title="Difference between segmenters (new - old)\nExcluding 413000000",
                ylabel="Avg. length of active segments (hour)")

# %% [markdown]
# ## Overall segment Stats
#
# Calculated at the segment level for the set of final segments in 2020. Segment lengths for the old dataset are truncated as starting on '2020-01-01' for comparability.

# %%
q = f'''
WITH

seg_hours_new AS (
  SELECT 
    seg_id,
    TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 as hours
  FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`
),

seg_hours_no_413000000_new AS (
  SELECT 
    seg_id,
    TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 as hours
  FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`
  WHERE ssvid != '413000000'
),

seg_hours_old AS (
  SELECT 
    seg_id,
    TIMESTAMP_DIFF(
        IF(last_timestamp <= '2020-12-31 23:59:59', last_timestamp, TIMESTAMP('2020-12-31 23:59:59')),
        IF(first_timestamp >= '2020-01-01', first_timestamp, TIMESTAMP('2020-01-01'))
        , MINUTE)/60.0 as hours
  FROM `{DATASET_OLD}.{SEGMENT_INFO_TABLE}`
  WHERE (DATE(first_timestamp) <= '2020-12-31' AND DATE(last_timestamp) >= '2020-01-01')
  AND (ssvid IN (SELECT DISTINCT ssvid FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`))
),

seg_hours_no_413000000_old AS (
  SELECT 
    seg_id,
    TIMESTAMP_DIFF(
        IF(last_timestamp <= '2020-12-31 23:59:59', last_timestamp, TIMESTAMP('2020-12-31 23:59:59')),
        IF(first_timestamp >= '2020-01-01', first_timestamp, TIMESTAMP('2020-01-01'))
        , MINUTE)/60.0 as hours
  FROM `{DATASET_OLD}.{SEGMENT_INFO_TABLE}`
  WHERE (DATE(first_timestamp) <= '2020-12-31' AND DATE(last_timestamp) >= '2020-01-01')
  AND (ssvid IN (SELECT DISTINCT ssvid FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`))
  AND ssvid != '413000000'
),

all_mmsi_stats AS (
SELECT 'all mmsi' as description, * EXCEPT (jn)
FROM
(
  SELECT 
    1 as jn,   
    COUNT(*) as num_segs_new,
    COUNT(DISTINCT seg_id) as num_segs_distinct_new,
    SUM(hours) as sum_seg_length_h_new,
    SUM(hours)/COUNT(*) as avg_hours_new
  FROM seg_hours_new
) 
JOIN
(
  SELECT 
    1 as jn,   
    COUNT(*) as num_segs_old,
    COUNT(DISTINCT seg_id) as num_segs_distinct_old,
    SUM(hours) as sum_seg_length_h_old,
    SUM(hours)/COUNT(*) as avg_hours_old
  FROM seg_hours_old
) 
USING (jn)
),

no_413000000_stats AS (
  SELECT 'no 413000000' as description, * EXCEPT (jn)
FROM
(
  SELECT 
    1 as jn,   
    COUNT(*) as num_segs_new,
    COUNT(DISTINCT seg_id) as num_segs_distinct_new,
    SUM(hours) as sum_seg_length_h_new,
    SUM(hours)/COUNT(*) as avg_hours_new
  FROM seg_hours_no_413000000_new
) 
JOIN
(
  SELECT 
    1 as jn,   
    COUNT(*) as num_segs_old,
    COUNT(DISTINCT seg_id) as num_segs_distinct_old,
    SUM(hours) as sum_seg_length_h_old,
    SUM(hours)/COUNT(*) as avg_hours_old
  FROM seg_hours_no_413000000_old
) 
USING (jn)
)

(SELECT * FROM all_mmsi_stats)
UNION ALL
(SELECT * FROM no_413000000_stats)
ORDER BY description
'''

# print(q)
df_seg_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
df_seg_stats_all_mmsi = df_seg_stats[df_seg_stats.description == 'all mmsi'].copy().iloc[0]
df_seg_stats_no_413000000 = df_seg_stats[df_seg_stats.description == 'no 413000000'].copy().iloc[0]

# %%
print("STATS FOR ALL MMSI")
print("--------------------")
print(f"Num distinct segs (NEW):  {df_seg_stats_all_mmsi.num_segs_distinct_new}")
print(f"Num distinct segs (OLD):  {df_seg_stats_all_mmsi.num_segs_distinct_old}")
print(f"Num distinct segs (DIFF): {df_seg_stats_all_mmsi.num_segs_distinct_new - df_seg_stats_all_mmsi.num_segs_distinct_old}")
print()
print(f"Sum seg hours (NEW):  {round(df_seg_stats_all_mmsi.sum_seg_length_h_new):,d}")
print(f"Sum seg hours (OLD):  {round(df_seg_stats_all_mmsi.sum_seg_length_h_old):,d}")
print(f"Sum seg hours (DIFF): {round(df_seg_stats_all_mmsi.sum_seg_length_h_new - df_seg_stats_all_mmsi.sum_seg_length_h_old):,d}")
print()
print(f"Avg seg length (NEW): {df_seg_stats_all_mmsi.avg_hours_new:0.2f}")
print(f"Avg seg length (OLD): {df_seg_stats_all_mmsi.avg_hours_old:0.2f}")
print(f"Avg seg length (DIFF): {df_seg_stats_all_mmsi.avg_hours_new - df_seg_stats_all_mmsi.avg_hours_old:0.2f}")



# %%
print("STATS WITHOUT 413000000")
print("--------------------")
print(f"Num distinct segs (NEW):  {df_seg_stats_no_413000000.num_segs_distinct_new}")
print(f"Num distinct segs (OLD):  {df_seg_stats_no_413000000.num_segs_distinct_old}")
print(f"Num distinct segs (DIFF): {df_seg_stats_no_413000000.num_segs_distinct_new - df_seg_stats_no_413000000.num_segs_distinct_old}")
print()
print(f"Sum seg hours (NEW):  {round(df_seg_stats_no_413000000.sum_seg_length_h_new):,d}")
print(f"Sum seg hours (OLD):  {round(df_seg_stats_no_413000000.sum_seg_length_h_old):,d}")
print(f"Sum seg hours (DIFF): {round(df_seg_stats_no_413000000.sum_seg_length_h_new - df_seg_stats_no_413000000.sum_seg_length_h_old):,d}")
print()
print(f"Avg seg length (NEW): {df_seg_stats_no_413000000.avg_hours_new:0.2f}")
print(f"Avg seg length (OLD): {df_seg_stats_no_413000000.avg_hours_old:0.2f}")
print(f"Avg seg length (DIFF): {df_seg_stats_no_413000000.avg_hours_new - df_seg_stats_no_413000000.avg_hours_old:0.2f}")



# %%

# %%

# %%

# %% [markdown]
# # OLD CODE for pulling tracks, if needed

# %% [markdown]
# ## Pull tracks for some sample MMSI and dates for both old and new segmenters to compare in GFW map
#
# **Negative change**
# * 413000000 on 2020-04-27
# * 100900000 from 2020-01-20 to 2020-01-22
# * 244453043 from 2020-08-04 to 2020-08-06
#
#
#
# **Positive change**
# * 412440222 from 2020-02-27 to 2020-03-02
#
#

# %%
import os

data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)


# %%
def get_tracks(dataset, ssvid, start_date, end_date):

    q = f'''
    SELECT 
    ssvid, timestamp, lat, lon, seg_id
    FROM `{dataset}.{MESSAGES_SEGMENTED_TABLE}*`
    WHERE ssvid = '{ssvid}'
    AND _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
    AND seg_id IS NOT NULL
    ORDER BY timestamp
    '''

    # print(q)
    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
def get_segments(dataset, ssvid, start_date, end_date):
    q = f'''
    SELECT 
    *
    FROM `{dataset}.{SEGMENT_INFO_TABLE}*`
    WHERE ssvid = '{ssvid}'
    AND (DATE(first_timestamp) <= '{end_date}' AND DATE(last_timestamp) >= '{start_date}')
    ORDER BY seg_id
    '''

    # print(q)
    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# %%
import pyseas.maps as psm
import pyseas.contrib as psc

def plot_segmented_tracks(df):
    with psm.context(psm.styles.panel):
        fig = plt.figure(figsize=(12, 12))
        info = psc.multi_track_panel(
            df.timestamp,
            df.lon,
            df.lat,
            df.seg_id,
            plots=[{"label": "lon", "values": df.lon}, {"label": "lat", "values": df.lat}],
        )
        plt.legend(
            info.legend_handles.values(),
            [x.split("-", 1)[1].rstrip(".000000000Z") for x in info.legend_handles.keys()],
        )
        return fig



# %%
df_ssvid_stats[df_ssvid_stats.ssvid == '237352400']

# %%
tracks_237352400_new = get_tracks(DATASET_NEW_MONTHLY_INT, '237352400', '20200101', '20201231')
# tracks_237352400_new.to_csv(f'{data_folder}/tracks_237352400_new.csv')

tracks_237352400_old = get_tracks(DATASET_OLD, '237352400', '20200101', '20201231')
# tracks_237352400_old.to_csv(f'{data_folder}/tracks_237352400_old.csv')

# %%
fig = plot_segmented_tracks(tracks_237352400_old)

# %%
fig = plot_segmented_tracks(tracks_237352400_new)

# %%
df_237352400_segs_old = get_segments(DATASET_OLD,'237352400', '2020-01-01', '2020-12-31')
df_237352400_segs_new = get_segments(DATASET_NEW_MONTHLY_INT,'237352400', '2020-01-01', '2020-12-31')

# %%
df_237352400_segs_old

# %%
df_237352400_segs_new

# %%

# %%

# %%

# %%
tracks_412440222_new = get_tracks(f'{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}', '412440222', '20200227', '20200302')
tracks_412440222_new.to_csv(f'{data_folder}/tracks_412440222_new.csv')

tracks_412440222_old = get_tracks(f'{DATASET_OLD}.{MESSAGES_SEGMENTED_TABLE}', '412440222', '20200227', '20200302')
tracks_412440222_old.to_csv(f'{data_folder}/tracks_412440222_old.csv')


# %%
tracks_244453043_new = get_tracks(f'{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}', '244453043', '20200804', '20200806')
tracks_244453043_new.to_csv(f'{data_folder}/tracks_244453043_new.csv')

tracks_244453043_old = get_tracks(f'{DATASET_OLD}.{MESSAGES_SEGMENTED_TABLE}', '244453043', '20200804', '20200806')
tracks_244453043_old.to_csv(f'{data_folder}/tracks_244453043_old.csv')


# %%
tracks_100900000_new = get_tracks(f'{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}', '100900000', '20200120', '20200122')
tracks_100900000_new.to_csv(f'{data_folder}/tracks_100900000_new.csv')

tracks_100900000_old = get_tracks(f'{DATASET_OLD}.{MESSAGES_SEGMENTED_TABLE}', '100900000', '20200120', '20200122')
tracks_100900000_old.to_csv(f'{data_folder}/tracks_100900000_old.csv')


# %%
tracks_413000000_new = get_tracks(f'{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}', '413000000', '20200427', '20200427')
tracks_413000000_new.to_csv(f'{data_folder}/tracks_413000000_new.csv')

tracks_413000000_old = get_tracks(f'{DATASET_OLD}.{MESSAGES_SEGMENTED_TABLE}', '413000000', '20200427', '20200427')
tracks_413000000_old.to_csv(f'{data_folder}/tracks_413000000_old.csv')



