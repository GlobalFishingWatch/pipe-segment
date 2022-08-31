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

pd.set_option("max_rows", 20)

# %% [markdown]
# ##### Table Names

# %%
data_folder = 'data'

dataset_old = 'pipe_production_v20201001'
dataset_new = 'pipe_ais_test_20220830193000_monthly_internal'

fragments_table = 'fragments_'
segments_table = 'segments_'
messages_segmented_table = 'messages_segmented_'


# %% [markdown]
# ## Data generation

# %%
q = f'''

WITH 

segment_data_new AS (
    SELECT 
    DATE(seg.timestamp) as date,
    EXTRACT(YEAR from DATE(seg.timestamp)) as year,
    seg.ssvid as ssvid,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(frag.last_msg_timestamp, seg.first_timestamp, MINUTE)/60.0) as sum_seg_length_h
    FROM `{dataset_new}.{segments_table}2020*` seg
    JOIN `{dataset_new}.{fragments_table}2020*` frag
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
    SUM(TIMESTAMP_DIFF(last_msg_of_day_timestamp, IF(first_msg_timestamp >= '2020-01-01', first_msg_timestamp, TIMESTAMP('2020-01-01')), MINUTE)/60.0) as sum_seg_length_h
    FROM `{dataset_old}.{segments_table}2020*`
    WHERE ssvid IN (SELECT DISTINCT ssvid FROM `{dataset_new}.{segments_table}2020*`)
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

df_segs_daily = df_segs_daily_by_ssvid.copy().groupby(['date', 'year']).sum().reset_index()

# Quick checks for duplicate seg_id
assert(df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.num_segs_new != df_segs_daily_by_ssvid.num_segs_distinct_new].shape[0] == 0)
assert(df_segs_daily[df_segs_daily.num_segs_new != df_segs_daily.num_segs_distinct_new].shape[0] == 0)


# %%
def plot_new_vs_old(df, col_prefix, title, ylabel="Number of segments"):
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
def plot_diff(df, col_prefix, title, ylabel="Total segment hours"):
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
                      title="Number of segments per day\nAll baby pipe MMSI")


# %%
fig = plot_diff(df_segs_daily, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI")

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='sum_seg_length_h_', 
                      title="Total length of segments per day (hours)\nAll baby pipe MMSI")

# %%
fig = plot_diff(df_segs_daily, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI")

# %% [markdown]
# ## Daily stats without 413000000
#
# `413000000` generally dominates the overall pattern so it is interesting to see that and to see what the pattern looks like without `413000000`

# %% [markdown]
# ### The difference in the number of segments and hours of all MMSI vs just `413000000` 
#
# To confirm that `413000000` dominates the pattern. This may not be true for future QA and your copy of this notebook may need to be modified to look deeper.

# %%
fig, ax = plot_diff(df_segs_daily, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI")
df_temp = df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid == '413000000'].copy().reset_index(drop=True)
df_temp.num_segs_distinct_diff.plot(c='orange', ax=ax, label='413000000 only')
plt.legend()
plt.show()

# %%
fig, ax = plot_diff(df_segs_daily, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nAll baby pipe MMSI")
df_temp = df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid == '413000000'].copy().reset_index(drop=True)
df_temp.sum_seg_length_h_diff.plot(c='orange', ax=ax, label='413000000 only')
plt.legend()
plt.show()

# %% [markdown]
# ## Daily Stats WITHOUT `413000000`

# %%
df_segs_daily_no_413000000 = df_segs_daily_by_ssvid[df_segs_daily_by_ssvid.ssvid != '413000000'].copy().groupby(['date', 'year']).sum().reset_index()


# %%
fig = plot_new_vs_old(df_segs_daily_no_413000000, col_prefix='num_segs_distinct_', 
                      title="Number of segments per day\nExcluding 413000000")

# %%
fig = plot_diff(df_segs_daily_no_413000000, col_prefix='num_segs_distinct_', 
                title="Difference between segmenters (new - old)\nExcluding 413000000")

# %%
fig = plot_new_vs_old(df_segs_daily_no_413000000, col_prefix='sum_seg_length_h_', 
                      title="Total length of segments per day (hours)\nExcluding 413000000")

# %%
fig = plot_diff(df_segs_daily_no_413000000, col_prefix='sum_seg_length_h_', 
                title="Difference between segmenters (new - old)\nExcluding 413000000")

# %% [markdown]
# ## Overall Segment Stats

# %% [markdown]
# ##### All MMSI

# %%
print("STATS FOR ALL MMSI")
print("--------------------")
print(f"Num distinct segs (NEW):  {df_segs_daily_by_ssvid.num_segs_distinct_new.sum()}")
print(f"Num distinct segs (OLD):  {df_segs_daily_by_ssvid.num_segs_distinct_old.sum()}")
print(f"Num distinct segs (DIFF): {df_segs_daily_by_ssvid.num_segs_distinct_diff.sum()}")
print()
print(f"Sum seg hours (NEW):  {round(df_segs_daily_by_ssvid.sum_seg_length_h_new.sum()):,d}")
print(f"Sum seg hours (OLD):  {round(df_segs_daily_by_ssvid.sum_seg_length_h_old.sum()):,d}")
print(f"Sum seg hours (DIFF): {round(df_segs_daily_by_ssvid.sum_seg_length_h_diff.sum()):,d}")


# %% [markdown]
# ##### Without `413000000`

# %%
print("STATS WITHOUT 413000000")
print("--------------------")
print(f"Num distinct segs (NEW):  {df_segs_daily_no_413000000.num_segs_distinct_new.sum()}")
print(f"Num distinct segs (OLD):  {df_segs_daily_no_413000000.num_segs_distinct_old.sum()}")
print(f"Num distinct segs (DIFF): {df_segs_daily_no_413000000.num_segs_distinct_diff.sum()}")
print()
print(f"Sum seg hours (NEW):  {round(df_segs_daily_no_413000000.sum_seg_length_h_new.sum()):,d}")
print(f"Sum seg hours (OLD):  {round(df_segs_daily_no_413000000.sum_seg_length_h_old.sum()):,d}")
print(f"Sum seg hours (DIFF): {round(df_segs_daily_no_413000000.sum_seg_length_h_diff.sum()):,d}")


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
def get_tracks(table, ssvid, start_date, end_date):

    q = f'''
    SELECT 
    ssvid, timestamp, lat, lon, seg_id
    FROM `{table}*`
    WHERE ssvid = '{ssvid}'
    AND _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
    AND seg_id IS NOT NULL
    ORDER BY timestamp
    '''

    # print(q)
    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
tracks_412440222_new = get_tracks(f'{dataset_new}.{messages_segmented_table}', '412440222', '20200227', '20200302')
tracks_412440222_new.to_csv(f'{data_folder}/tracks_412440222_new.csv')

tracks_412440222_old = get_tracks(f'{dataset_old}.{messages_segmented_table}', '412440222', '20200227', '20200302')
tracks_412440222_old.to_csv(f'{data_folder}/tracks_412440222_old.csv')


# %%
tracks_244453043_new = get_tracks(f'{dataset_new}.{messages_segmented_table}', '244453043', '20200804', '20200806')
tracks_244453043_new.to_csv(f'{data_folder}/tracks_244453043_new.csv')

tracks_244453043_old = get_tracks(f'{dataset_old}.{messages_segmented_table}', '244453043', '20200804', '20200806')
tracks_244453043_old.to_csv(f'{data_folder}/tracks_244453043_old.csv')


# %%
tracks_100900000_new = get_tracks(f'{dataset_new}.{messages_segmented_table}', '100900000', '20200120', '20200122')
tracks_100900000_new.to_csv(f'{data_folder}/tracks_100900000_new.csv')

tracks_100900000_old = get_tracks(f'{dataset_old}.{messages_segmented_table}', '100900000', '20200120', '20200122')
tracks_100900000_old.to_csv(f'{data_folder}/tracks_100900000_old.csv')


# %%
tracks_413000000_new = get_tracks(f'{dataset_new}.{messages_segmented_table}', '413000000', '20200427', '20200427')
tracks_413000000_new.to_csv(f'{data_folder}/tracks_413000000_new.csv')

tracks_413000000_old = get_tracks(f'{dataset_old}.{messages_segmented_table}', '413000000', '20200427', '20200427')
tracks_413000000_old.to_csv(f'{data_folder}/tracks_413000000_old.csv')



