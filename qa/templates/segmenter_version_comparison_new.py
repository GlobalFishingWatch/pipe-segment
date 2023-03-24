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
#     display_name: rad
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
import os
import pandas as pd
import matplotlib.pyplot as plt
import pyseas.maps as psm
import pyseas.contrib as psc

from importlib import reload
import config_new
reload(config_new)
from config_new import PROJECT, DATASET_PIPE3, DATASET_PIPE25, SEGMENTS_TABLE, FRAGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, SEGMENT_INFO_TABLE, SEGMENT_IDENITY_DAILY_TABLE
# from config import DATASET_PIPE25, DATASET_PIPE3, FRAGMENTS_TABLE, SEGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, SEGMENT_INFO_TABLE

START_DATE = '20190101'
END_DATE = '20211231'
pd.set_option("max_rows", 20)

# TODO: Change once running in script form
DATA_FOLDER = "../data"
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)

# TODO: Change once running in script form
FIGURES_FOLDER = "../figures"
if not os.path.exists(FIGURES_FOLDER):
    os.makedirs(FIGURES_FOLDER)

# %% [markdown]
# ## Data generation
#
# *NOTE: this query is not fully flexible on dates and still assumes all data is within 2020 as hardcoding "2020" into the table suffix decreased query costs by ~95%. You may need to modify the query and check costs if running a period outside of 2020.*

# %%
q = f'''

WITH 

segment_data_new AS (
    SELECT 
    DATE(seg.timestamp) AS date,
    EXTRACT(YEAR from DATE(seg.timestamp)) AS year,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(frag.last_msg_timestamp, seg.first_timestamp, MINUTE)/60.0) AS sum_seg_length_h,
    SUM(TIMESTAMP_DIFF(frag.last_msg_timestamp, frag.first_msg_timestamp, MINUTE)/60.0) AS sum_seg_day_length_h,
    SUM(IF(num_idents > 0, 1, 0)) as num_segs_with_idents,
    MIN(num_idents) as min_num_idents,
    MAX(num_idents) as max_num_idents,
    AVG(num_idents) as avg_num_idents,
    SUM(daily_msg_count) as total_msg_count,
    SUM(cumulative_msg_count) as total_cumul_msg_count,
    FROM 
        (SELECT *, (SELECT COUNT(DISTINCT id.shipname) FROM UNNEST(daily_identities) AS id WHERE id.shipname IS NOT NULL) as num_idents
        FROM `{DATASET_PIPE3}.{SEGMENTS_TABLE}*`
        WHERE _TABLE_SUFFIX BETWEEN '{START_DATE}' AND '{END_DATE}')
        AS seg
    JOIN 
        (SELECT * FROM `{DATASET_PIPE3}.{FRAGMENTS_TABLE}*`
        WHERE _TABLE_SUFFIX BETWEEN '{START_DATE}' AND '{END_DATE}')
        AS frag
    USING(frag_id)
    GROUP BY date, year
),

segment_data_old AS (
      SELECT 
    DATE(timestamp) as date,
    EXTRACT(YEAR from DATE(timestamp)) as year,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(last_msg_of_day_timestamp, first_msg_timestamp, MINUTE)/60.0) as sum_seg_length_h,
    SUM(TIMESTAMP_DIFF(last_msg_of_day_timestamp, first_msg_of_day_timestamp, MINUTE)/60.0) AS sum_seg_day_length_h,
    SUM(IF(num_idents > 0, 1, 0)) as num_segs_with_idents,
    MIN(num_idents) as min_num_idents,
    MAX(num_idents) as max_num_idents,
    AVG(num_idents) as avg_num_idents,
    SUM(message_count) as total_msg_count,
    FROM 
        (SELECT *, (SELECT COUNTIF(shipname IS NOT NULL) FROM UNNEST (shipnames) AS shipname) as num_idents
        FROM `{DATASET_PIPE25}.{SEGMENTS_TABLE}*`
        WHERE _TABLE_SUFFIX BETWEEN '{START_DATE}' AND '{END_DATE}')
    GROUP BY date, year
),

segment_join AS (
    SELECT
    date,
    year,
    IFNULL(segs_new.num_segs, 0) AS num_segs_new,
    IFNULL(segs_new.num_segs_distinct, 0) AS num_segs_distinct_new,
    IFNULL(segs_new.sum_seg_length_h, 0) AS sum_seg_length_h_new,
    IFNULL(segs_new.sum_seg_day_length_h, 0) AS sum_seg_day_length_h_new,
    segs_new.num_segs_with_idents AS num_segs_with_idents_new,
    segs_new.min_num_idents AS min_num_idents_new,
    segs_new.max_num_idents AS max_num_idents_new,
    segs_new.avg_num_idents AS avg_num_idents_new,
    segs_new.total_msg_count AS total_msg_count_new,
    segs_new.total_cumul_msg_count AS total_cumul_msg_count_new,
    IFNULL(segs_old.num_segs, 0) AS num_segs_old,
    IFNULL(segs_old.num_segs_distinct, 0) AS num_segs_distinct_old,
    IFNULL(segs_old.sum_seg_length_h, 0) AS sum_seg_length_h_old,
    IFNULL(segs_old.sum_seg_day_length_h, 0) AS sum_seg_day_length_h_old,
    segs_old.min_num_idents AS min_num_idents_old,
    segs_old.max_num_idents AS max_num_idents_old,
    segs_old.avg_num_idents AS avg_num_idents_old,
    segs_old.num_segs_with_idents AS num_segs_with_idents_old,
    segs_old.total_msg_count AS total_msg_count_old,
    FROM segment_data_new segs_new
    FULL OUTER JOIN segment_data_old segs_old
    USING(date, year)
    ORDER BY date
)

SELECT
date,
year,
num_segs_new,
num_segs_distinct_new,
sum_seg_length_h_new,
sum_seg_day_length_h_new,
num_segs_with_idents_new,
min_num_idents_new,
max_num_idents_new,
avg_num_idents_new,
total_msg_count_new,
total_cumul_msg_count_new,
num_segs_old,
num_segs_distinct_old,
sum_seg_length_h_old,
sum_seg_day_length_h_old,
num_segs_with_idents_old,
min_num_idents_old,
max_num_idents_old,
avg_num_idents_old,
total_msg_count_old,
(num_segs_new - num_segs_old) as num_segs_diff,
(num_segs_distinct_new - num_segs_distinct_old) as num_segs_distinct_diff,
(sum_seg_length_h_new - sum_seg_length_h_old) as sum_seg_length_h_diff,
(sum_seg_day_length_h_new - sum_seg_day_length_h_old) as sum_seg_day_length_h_diff,
(num_segs_with_idents_new - num_segs_with_idents_old) as num_segs_with_idents_diff,
(min_num_idents_new - min_num_idents_old) as min_num_idents_diff,
(max_num_idents_new - max_num_idents_old) as max_num_idents_diff,
(avg_num_idents_new - avg_num_idents_old) as avg_num_idents_diff,
(total_msg_count_new - total_msg_count_old) as total_msg_count_diff,
FROM segment_join
'''

# print(q)
df_segs_daily = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
df_segs_daily.date = pd.to_datetime(df_segs_daily.date)

df_segs_daily['avg_seg_length_h_new'] = df_segs_daily.sum_seg_length_h_new / df_segs_daily.num_segs_distinct_new
df_segs_daily['avg_seg_length_h_old'] = df_segs_daily.sum_seg_length_h_old / df_segs_daily.num_segs_distinct_old
df_segs_daily['avg_seg_length_h_diff'] = df_segs_daily.avg_seg_length_h_new - df_segs_daily.avg_seg_length_h_old
df_segs_daily['prop_segs_with_idents_new'] = df_segs_daily.num_segs_with_idents_new / df_segs_daily.num_segs_new
df_segs_daily['prop_segs_with_idents_old'] = df_segs_daily.num_segs_with_idents_old / df_segs_daily.num_segs_old
df_segs_daily['prop_segs_with_idents_diff'] = df_segs_daily.num_segs_with_idents_new - df_segs_daily.num_segs_with_idents_old

# Quick checks for duplicate seg_id
assert(df_segs_daily[df_segs_daily.num_segs_new != df_segs_daily.num_segs_distinct_new].shape[0] == 0)

# %%
q = f'''
WITH 

segment_data_new AS (
    SELECT 
    DATE(first_timestamp) AS date,
    EXTRACT(YEAR from DATE(first_timestamp)) AS year,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(last_pos_timestamp, TIMESTAMP(SUBSTRING(seg_id, STRPOS(seg_id, '-') + 1, LENGTH(seg_id) - STRPOS(seg_id, '-') - STRPOS(REVERSE(seg_id), '-'))), MINUTE)/60.0) AS sum_seg_length_h,
    SUM(TIMESTAMP_DIFF(last_pos_timestamp, first_pos_timestamp, MINUTE)/60.0) AS sum_seg_day_length_h,
    SUM(IF(num_idents > 0, 1, 0)) as num_segs_with_idents,
    MIN(num_idents) as min_num_idents,
    MAX(num_idents) as max_num_idents,
    AVG(num_idents) as avg_num_idents,
    SUM(msg_count) as total_msg_count,
    SUM(pos_count) as total_pos_count,
    SUM(ident_count) as total_ident_count,
    FROM 
        (SELECT *, (SELECT COUNTIF(shipname IS NOT NULL) FROM UNNEST (shipname) AS shipname) as num_idents
        FROM `{DATASET_PIPE3}.{SEGMENT_IDENITY_DAILY_TABLE}*`
        WHERE _TABLE_SUFFIX BETWEEN '{START_DATE}' AND '{END_DATE}')
    GROUP BY date, year
),

segment_data_old AS (
    SELECT 
    DATE(first_timestamp) as date,
    EXTRACT(YEAR from DATE(first_timestamp)) as year,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    SUM(TIMESTAMP_DIFF(last_pos_timestamp, TIMESTAMP(RIGHT(seg_id, LENGTH(seg_id) - STRPOS(seg_id, '-'))), MINUTE)/60.0) AS sum_seg_length_h,
    SUM(TIMESTAMP_DIFF(last_pos_timestamp, first_pos_timestamp, MINUTE)/60.0) AS sum_seg_day_length_h,
    SUM(IF(num_idents > 0, 1, 0)) as num_segs_with_idents,
    MIN(num_idents) as min_num_idents,
    MAX(num_idents) as max_num_idents,
    AVG(num_idents) as avg_num_idents,
    SUM(msg_count) as total_msg_count,
    SUM(pos_count) as total_pos_count,
    SUM(ident_count) as total_ident_count,
    FROM 
        (SELECT *, (SELECT COUNTIF(shipname IS NOT NULL) FROM UNNEST (shipname) AS shipname) as num_idents
        FROM `{DATASET_PIPE25}.{SEGMENT_IDENITY_DAILY_TABLE}*`
        WHERE _TABLE_SUFFIX BETWEEN '{START_DATE}' AND '{END_DATE}')
    GROUP BY date, year
),

segment_join AS (
    SELECT
    date,
    year,
    IFNULL(segs_new.num_segs, 0) AS num_segs_new,
    IFNULL(segs_new.num_segs_distinct, 0) AS num_segs_distinct_new,
    IFNULL(segs_new.sum_seg_length_h, 0) AS sum_seg_length_h_new,
    IFNULL(segs_new.sum_seg_day_length_h, 0) AS sum_seg_day_length_h_new,
    segs_new.num_segs_with_idents AS num_segs_with_idents_new,
    segs_new.min_num_idents AS min_num_idents_new,
    segs_new.max_num_idents AS max_num_idents_new,
    segs_new.avg_num_idents AS avg_num_idents_new,
    segs_new.total_msg_count AS total_msg_count_new,
    segs_new.total_pos_count AS total_pos_count_new,
    segs_new.total_ident_count AS total_ident_count_new,
    IFNULL(segs_old.num_segs, 0) AS num_segs_old,
    IFNULL(segs_old.num_segs_distinct, 0) AS num_segs_distinct_old,
    IFNULL(segs_old.sum_seg_length_h, 0) AS sum_seg_length_h_old,
    IFNULL(segs_old.sum_seg_day_length_h, 0) AS sum_seg_day_length_h_old,
    segs_old.min_num_idents AS min_num_idents_old,
    segs_old.max_num_idents AS max_num_idents_old,
    segs_old.avg_num_idents AS avg_num_idents_old,
    segs_old.num_segs_with_idents AS num_segs_with_idents_old,
    segs_old.total_msg_count AS total_msg_count_old,
    segs_old.total_pos_count AS total_pos_count_old,
    segs_old.total_ident_count AS total_ident_count_old,
    FROM segment_data_new segs_new
    FULL OUTER JOIN segment_data_old segs_old
    USING(date, year)
    ORDER BY date
)

SELECT
date,
year,
num_segs_new,
num_segs_distinct_new,
sum_seg_length_h_new,
sum_seg_day_length_h_new,
num_segs_with_idents_new,
min_num_idents_new,
max_num_idents_new,
avg_num_idents_new,
total_msg_count_new,
total_pos_count_new,
total_ident_count_new,
num_segs_old,
num_segs_distinct_old,
sum_seg_length_h_old,
sum_seg_day_length_h_old,
num_segs_with_idents_old,
min_num_idents_old,
max_num_idents_old,
avg_num_idents_old,
total_msg_count_old,
total_pos_count_old,
total_ident_count_old,
(num_segs_new - num_segs_old) as num_segs_diff,
(num_segs_distinct_new - num_segs_distinct_old) as num_segs_distinct_diff,
(sum_seg_length_h_new - sum_seg_length_h_old) as sum_seg_length_h_diff,
(sum_seg_day_length_h_new - sum_seg_day_length_h_old) as sum_seg_day_length_h_diff,
(num_segs_with_idents_new - num_segs_with_idents_old) as num_segs_with_idents_diff,
(min_num_idents_new - min_num_idents_old) as min_num_idents_diff,
(max_num_idents_new - max_num_idents_old) as max_num_idents_diff,
(avg_num_idents_new - avg_num_idents_old) as avg_num_idents_diff,
(total_msg_count_new - total_msg_count_old) as total_msg_count_diff,
(total_pos_count_new - total_pos_count_old) as total_pos_count_diff,
(total_ident_count_new - total_ident_count_old) as total_ident_count_diff,
FROM segment_join
'''

# print(q)
df_seg_identity_daily = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
df_seg_identity_daily.drop(df_seg_identity_daily[df_seg_identity_daily.date.isnull()].index, inplace=True)
df_seg_identity_daily.date = pd.to_datetime(df_seg_identity_daily.date)

df_seg_identity_daily['avg_seg_length_h_new'] = df_seg_identity_daily.sum_seg_length_h_new / df_seg_identity_daily.num_segs_distinct_new
df_seg_identity_daily['avg_seg_length_h_old'] = df_seg_identity_daily.sum_seg_length_h_old / df_seg_identity_daily.num_segs_distinct_old
df_seg_identity_daily['avg_seg_length_h_diff'] = df_seg_identity_daily.avg_seg_length_h_new - df_seg_identity_daily.avg_seg_length_h_old
df_seg_identity_daily['prop_segs_with_idents_new'] = df_seg_identity_daily.num_segs_with_idents_new / df_seg_identity_daily.num_segs_new
df_seg_identity_daily['prop_segs_with_idents_old'] = df_seg_identity_daily.num_segs_with_idents_old / df_seg_identity_daily.num_segs_old
df_seg_identity_daily['prop_segs_with_idents_diff'] = df_seg_identity_daily.num_segs_with_idents_new - df_seg_identity_daily.num_segs_with_idents_old

# Quick checks for duplicate seg_id
assert(df_seg_identity_daily[df_seg_identity_daily.num_segs_new != df_seg_identity_daily.num_segs_distinct_new].shape[0] == 0)

# %% [markdown]
# #### Save data for reference

# %%
df_segs_daily.to_csv(f"{DATA_FOLDER}/daily_stats_segments.csv")
df_seg_identity_daily.to_csv(f"{DATA_FOLDER}/daily_stats_segment_identity_daily.csv")


# %%
def plot_new_vs_old(df, col_prefix, title, ylabel="", outfile=None):
    fig = plt.figure()
    ax = df[[f'{col_prefix}old']].plot(label='old')
    df[[f'{col_prefix}new']].plot(label='new', ax=ax)
    years = list(df.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    ax.legend(["Pipe 2.5", "Pipe 3"])
    plt.title(title)
    plt.ylabel(ylabel)

    if outfile:
        plt.savefig(f"{FIGURES_FOLDER}/{outfile}", dpi=180)
        
    return fig, ax



# %%
def plot_diff(df, col_prefix, title, ylabel="", outfile=None):
    fig = plt.figure()
    ax = df[[f'{col_prefix}diff']].plot(c='green', label='diff')
    years = list(df.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    ax.legend(["Pipe 3 - Pipe 2.5"])
    plt.title(title)
    plt.ylabel(ylabel)

    if outfile:
        plt.savefig(f"{FIGURES_FOLDER}/{outfile}", dpi=180)
        
    return fig, ax



# %% [markdown]
# ## Daily Stats

# %% [markdown]
# #### Number of segments active in each day
#
# `num_segs`
#
# Note: distinctness of `seg_id`s was checked in an assertion during data pull so we can ignore the `num_segs_distinct` column.

# %%
plot_new_vs_old(df_segs_daily, col_prefix='num_segs_', 
                title="Number of active segments per day\nsegments_",
                ylabel="num_segs", outfile="num_segs_segments.png")

plot_diff(df_segs_daily, col_prefix='num_segs_', 
          title="Difference in number of active segments per day\nsegments_",
          ylabel="num_segs", outfile="num_segs_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='num_segs_', 
                title="Number of active segments per day\nsegment_identity_daily_",
                ylabel="num_segs", outfile="num_segs_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='num_segs_', 
          title="Difference in number of active_segments per day\nsegment_identity_daily_",
          ylabel="num_segs", outfile="num_segs_diff_segment_identity_daily.png")


# %% [markdown]
# #### Total cumulative length of segments active in each day (hours)
#
# `sum_seg_length_h`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='sum_seg_length_h_', 
                      title="Total cumulative length of segments active in each day (hours)\nsegments_",
                      ylabel="Hours", outfile="sum_seg_length_h_segments.png")

plot_diff(df_segs_daily, col_prefix='sum_seg_length_h_', 
          title="Difference in total cumulative length of segments active in each day\nsegments_",
          ylabel="Hours", outfile="sum_seg_length_h_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='sum_seg_length_h_', 
                      title="Total cumulative length of segments active in each day (hours)\nsegment_identity_daily_",
                      ylabel="Hours", outfile="sum_seg_length_h_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='sum_seg_length_h_', 
          title="Difference in total cumulative length of segments active in each day\nsegment_identity_daily_",
          ylabel="Hours", outfile="sum_seg_length_h_diff_segment_identity_daily.png")


# %% [markdown]
# #### Sum of all segment lengths within each day (hours)
#
# `sum_seg_day_length_h_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='sum_seg_day_length_h_', 
                      title="Total length of segments within each day (hours)\nsegments_",
                      ylabel="Hours", outfile="sum_seg_day_length_h_segments.png")

plot_diff(df_segs_daily, col_prefix='sum_seg_day_length_h_', 
          title="Difference in total length of segments within each day\nsegments_",
          ylabel="Hours", outfile="sum_seg_day_length_h_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='sum_seg_day_length_h_', 
                      title="Total length of segments within each day (hours)\nsegment_identity_daily_",
                      ylabel="Hours", outfile="sum_seg_day_length_h_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='sum_seg_day_length_h_', 
          title="Difference in total length of segments within each day\nsegment_identity_daily_",
          ylabel="Hours", outfile="sum_seg_day_length_h_diff_segment_identity_daily.png")


# %% [markdown]
# #### Average segment length per day (hours)
#
# `sum_seg_length_h_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='avg_seg_length_h_', 
                      title="Average length of segments per day (hours)\nsegments_",
                      ylabel="Hours", outfile="avg_seg_length_h_segments.png")

plot_diff(df_segs_daily, col_prefix='avg_seg_length_h_', 
          title="Difference in average length of segment per day\nsegments_",
          ylabel="Hours", outfile="avg_seg_length_h_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='avg_seg_length_h_', 
                      title="Average length of segments per day (hours)\nsegment_identity_daily_",
                      ylabel="Hours", outfile="avg_seg_length_h_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='avg_seg_length_h_', 
          title="Difference in average length of segment per day\nsegment_identity_daily_",
          ylabel="Hours", outfile="avg_seg_length_h_diff_segment_identity_daily.png")


# %%

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='num_segs_with_idents_', 
                      title="Number of segments with at least one valid shipname\nAll MMSI",
                      ylabel="Number of segments")


# %% [markdown]
# #### Number of segment with at least one valid shipname
#
# `num_segs_with_idents_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='num_segs_with_idents_', 
                      title="Number of segments with at least one valid shipname per day\nsegments_",
                      ylabel="num_segs_with_idents_", outfile="num_segs_with_idents_segments.png")

plot_diff(df_segs_daily, col_prefix='num_segs_with_idents_', 
          title="Difference in number of segments with at least one valid shipname per day\nsegments_",
          ylabel="num_segs_with_idents_", outfile="num_segs_with_idents_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='num_segs_with_idents_', 
                      title="Number of segments with at least one valid shipname per day\nsegment_identity_daily_",
                      ylabel="num_segs_with_idents_", outfile="num_segs_with_idents_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='num_segs_with_idents_', 
          title="Difference in number of segments with at least one valid shipname per day\nsegment_identity_daily_",
          ylabel="num_segs_with_idents_", outfile="num_segs_with_idents_diff_segment_identity_daily.png")


# %% [markdown]
# #### Proportion of segments with at least one valid shipname
#
# `prop_segs_with_idents_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='prop_segs_with_idents_', 
                      title="Proportion of segments with at least one valid shipname per day\nsegments_",
                      ylabel="prop_segs_with_idents_", outfile="prop_segs_with_idents_segments.png")

plot_diff(df_segs_daily, col_prefix='prop_segs_with_idents_', 
          title="Difference in proportion of segments with at least one valid shipname per day\nsegments_",
          ylabel="prop_segs_with_idents_", outfile="prop_segs_with_idents_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='prop_segs_with_idents_', 
                      title="Proportion of segments with at least one valid shipname per day\nsegment_identity_daily_",
                      ylabel="prop_segs_with_idents_", outfile="prop_segs_with_idents_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='prop_segs_with_idents_', 
          title="Difference in proportion of segments with at least one valid shipname per day\nsegment_identity_daily_",
          ylabel="prop_segs_with_idents_", outfile="prop_segs_with_idents_diff_segment_identity_daily.png")


# %% [markdown]
# #### Average number of distinct shipnames in a segment per day
#
# `avg_num_idents_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='avg_num_idents_', 
                      title="Average number of distinct shipnames in a segment per day\nsegments_",
                      ylabel="avg_num_idents_", outfile="avg_num_idents_segments.png")

plot_diff(df_segs_daily, col_prefix='avg_num_idents_', 
          title="Difference in average number of distinct shipnames in a segment per day\nsegments_",
          ylabel="avg_num_idents_", outfile="avg_num_idents_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='avg_num_idents_', 
                      title="Average number of distinct shipnames in a segment per day\nsegment_identity_daily_",
                      ylabel="avg_num_idents_", outfile="avg_num_idents_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='avg_num_idents_', 
          title="Difference in average number of distinct shipnames in a segment per day\nsegment_identity_daily_",
          ylabel="avg_num_idents_", outfile="avg_num_idents_diff_segment_identity_daily.png")


# %% [markdown]
# #### Maximum number of distinct shipnames in a segment per day
#
# `max_num_idents_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='max_num_idents_', 
                      title="Maximum number of distinct shipnames in a segment per day\nsegments_",
                      ylabel="max_num_idents_", outfile="max_num_idents_segments.png")

plot_diff(df_segs_daily, col_prefix='max_num_idents_', 
          title="Difference in maximum number of distinct shipnames in a segment per day\nsegments_",
          ylabel="max_num_idents_", outfile="max_num_idents_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='max_num_idents_', 
                      title="Maximum number of distinct shipnames in a segment per day\nsegment_identity_daily_",
                      ylabel="max_num_idents_", outfile="max_num_idents_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='max_num_idents_', 
          title="Difference in maximum number of distinct shipnames in a segment per day\nsegment_identity_daily_",
          ylabel="max_num_idents_", outfile="max_num_idents_diff_segment_identity_daily.png")


# %% [markdown]
# #### Total message count per day
#
# `total_msg_count_`

# %%
plot_new_vs_old(df_segs_daily, col_prefix='total_msg_count_', 
                      title="Total message count per day\nsegments_",
                      ylabel="total_msg_count_", outfile="total_msg_count_segments.png")

plot_diff(df_segs_daily, col_prefix='total_msg_count_', 
          title="Difference in total message count per day\nsegments_",
          ylabel="total_msg_count_", outfile="total_msg_count_diff_segments.png")

plot_new_vs_old(df_seg_identity_daily, col_prefix='total_msg_count_', 
                      title="Total message count per day\nsegment_identity_daily_",
                      ylabel="total_msg_count_", outfile="total_msg_count_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='total_msg_count_', 
          title="Difference in total message count per day\nsegment_identity_daily_",
          ylabel="total_msg_count_", outfile="total_msg_count_diff_segment_identity_daily.png")


# %% [markdown]
# #### Discrepancies in message count between pipe 2.5 and pipe 3
#
# In both the `segments_` and `segments_identity_daily_` tables, pipe 2.5 message counts are _cumulative_. In Pipe 3, the `segments_` table now has two separate columns, `daily_msg_count` and `cumulative_msg_count`. The `segment_identity_table` only uses `daily_msg_count` to set its `message_count` field meaning there is no cumulative field to compare to that table in pipe 2.5.
#
# All of the figures in the previous section are calculated with the cumulative message count in pipe 2.5 (which is the only one available) and the daily message count in pipe 3. But in the `segments_` table we can still compare cumulative message counts for each day so we do that below. 

# %%
fig = plt.figure()
ax = df_segs_daily[[f'total_msg_count_old']].plot(label='old')
df_segs_daily[[f'total_cumul_msg_count_new']].plot(label='new', ax=ax)
years = list(df_segs_daily.year.sort_values().unique())
ax.set_xticks([t*365 for t in range(len(years))])
ax.set_xticklabels(years)
fig.patch.set_facecolor('white')
ax.legend(["Pipe 2.5", "Pipe 3"])
plt.title("Total cumulative messages counts per day\nsegments_")

plt.savefig(f"{FIGURES_FOLDER}/total_cumul_msg_count_segments.png", dpi=180)

# %%
df_segs_daily['total_cumul_msg_count_diff'] = df_segs_daily.total_cumul_msg_count_new - df_segs_daily.total_msg_count_old

fig = plt.figure()
ax = df_segs_daily[[f'total_cumul_msg_count_diff']].plot(c='green', label='diff')
years = list(df_segs_daily.year.sort_values().unique())
ax.set_xticks([t*365 for t in range(len(years))])
ax.set_xticklabels(years)
fig.patch.set_facecolor('white')
ax.legend(["Pipe 3 - Pipe 2.5"])
plt.title("Difference in total cumulative messages counts per day\nsegments_")

plt.savefig(f"{FIGURES_FOLDER}/total_cumul_msg_count_diff_segments.png", dpi=180)

# %% [markdown]
# #### Total position message count per day
#
# `total_pos_count_`

# %%
plot_new_vs_old(df_seg_identity_daily, col_prefix='total_pos_count_', 
                      title="Total position message count per day\nsegment_identity_daily_",
                      ylabel="total_pos_count_", outfile="total_pos_count_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='total_pos_count_', 
          title="Difference in total position message count per day\nsegment_identity_daily_",
          ylabel="total_pos_count_", outfile="total_pos_count_diff_segment_identity_daily.png")


# %% [markdown]
# #### Total identity message count per day
#
# `total_ident_count_`

# %%
plot_new_vs_old(df_seg_identity_daily, col_prefix='total_ident_count_', 
                      title="Total identity message count per day\nsegment_identity_daily_",
                      ylabel="total_ident_count_", outfile="total_ident_count_segment_identity_daily.png")

plot_diff(df_seg_identity_daily, col_prefix='total_ident_count_', 
          title="Difference in total identity message count per day\nsegment_identity_daily_",
          ylabel="total_ident_count_", outfile="total_ident_count_diff_segment_identity_daily.png")


# %% [markdown]
# # Comparing segment_info between 2.5 and 3

# %%
fig = plt.figure()
ax = df_segs_daily[[f'total_msg_count_old']].plot(label='old')
df_segs_daily[[f'total_cumul_msg_count_new']].plot(label='new', ax=ax)
years = list(df_segs_daily.year.sort_values().unique())
ax.set_xticks([t*365 for t in range(len(years))])
ax.set_xticklabels(years)
fig.patch.set_facecolor('white')
ax.legend(["Pipe 2.5", "Pipe 3"])
plt.title("Total cumulative messages counts per day\nsegments_")

plt.savefig(f"{FIGURES_FOLDER}/total_cumul_msg_count_segments.png", dpi=180)

# %%
df_segs_daily['total_cumul_msg_count_diff'] = df_segs_daily.total_cumul_msg_count_new - df_segs_daily.total_msg_count_old

fig = plt.figure()
ax = df_segs_daily[[f'total_cumul_msg_count_diff']].plot(c='green', label='diff')
years = list(df_segs_daily.year.sort_values().unique())
ax.set_xticks([t*365 for t in range(len(years))])
ax.set_xticklabels(years)
fig.patch.set_facecolor('white')
ax.legend(["Pipe 3 - Pipe 2.5"])
plt.title("Difference in total cumulative messages counts per day\nsegments_")

plt.savefig(f"{FIGURES_FOLDER}/total_cumul_msg_count_diff_segments.png", dpi=180)

# %% [markdown]
# # Comparing segment_info between 2.5 and 3

# %% [markdown]
# ### Segment length
#
# Notes:
# * The histogram for this is developed in BigQuery as the rows are too numerous to pull into Python.
# * Segment lengths in pipe 2.5 are capped at the end of 2022 because at the time of coding, pipe 3 was only run through 2022. This is currently hardcorded.

# %%
x_cutoff = 100
num_bins = 10

# %%

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='avg_num_idents_', 
                      title="Average number of distinct shipnames in a segment\nAll MMSI",
                      ylabel="Number of identities")


# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='max_num_idents_', 
                      title="Maximum number of distinct shipnames per segment\nAll MMSI",
                      ylabel="Number of shipnames")


# %%
fig = plot_diff(df_segs_daily, col_prefix='max_num_idents_', 
                title="Difference between segmenters (new - old)\nAll MMSI",
                ylabel="Max number of distinct shipnames in a segment")

# %%
fig = plot_new_vs_old(df_segs_daily, col_prefix='total_msg_count_', 
                      title="Total daily message count for all segments in a day\nAll MMSI",
                      ylabel="Message count")


# %%
fig = plot_diff(df_segs_daily, col_prefix='total_msg_count_', 
                title="Difference between segmenters (new - old)\nAll MMSI",
                ylabel="Max number of distinct shipnames in a segment")

# %% [markdown]
# # Comparing segment_info between 2.5 and 3

# %% [markdown]
# ### Segment length
#
# Notes:
# * The histogram for this is developed in BigQuery as the rows are too numerous to pull into Python.
# * Segment lengths in pipe 2.5 are capped at the end of 2022 because at the time of coding, pipe 3 was only run through 2022. This is currently hardcorded.

# %%
x_cutoff = 100
num_bins = 10

# %%
q = f'''
WITH

segs_new AS (
  SELECT
  seg_id,
  TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE3}.{SEGMENT_INFO_TABLE}`
),

segs_old AS (
  SELECT
  seg_id,
  TIMESTAMP_DIFF(LEAST(last_timestamp, TIMESTAMP("2023-01-01")), first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE25}.{SEGMENT_INFO_TABLE}`
  WHERE first_timestamp < '2023-01-01'
),

bins AS (
  SELECT min+step*i AS min, min+step*(i+1) AS max
  FROM (
    SELECT max-min diff, min, max, (max-min)/{num_bins} step, GENERATE_ARRAY(0, {num_bins-1}, 1) i
    FROM (
      SELECT 0 AS min, {x_cutoff} AS max
    )
  ), UNNEST(i) i
  UNION ALL
  SELECT {x_cutoff} as min, MAX(max_seg_hours)+1 as max
  FROM
  (SELECT MAX(seg_hours) AS max_seg_hours FROM segs_new
  UNION ALL
  SELECT MAX(seg_hours) AS max_seg_hours FROM segs_old)
),

hist_new AS (
  SELECT COUNT(*) num_segs, MIN(min) as bin_min, MAX(max) as bin_max
  FROM segs_new 
  JOIN bins
  ON segs_new.seg_hours >= bins.min AND segs_new.seg_hours < bins.max
  GROUP BY min, max
),

hist_old AS (
  SELECT COUNT(*) num_segs, MIN(min) as bin_min, MAX(max) as bin_max
  FROM segs_old 
  JOIN bins
  ON segs_old.seg_hours >= bins.min AND segs_old.seg_hours < bins.max
  GROUP BY min, max
)

SELECT 
  bin_min, 
  bin_max, 
  hist_old.num_segs as num_segs_pipe25,
  hist_new.num_segs AS num_segs_pipe3,
FROM hist_new
JOIN hist_old
USING (bin_min, bin_max)
ORDER BY bin_min, bin_max
'''

# print(q)
df_seg_hours_hist = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
df_seg_hours_hist.plot.bar(x="bin_max", y=["num_segs_pipe25", "num_segs_pipe3"])


# %% [markdown]
# ### Number of segments per SSVID
#
# Notes:
# * The majority of SSVID has the same number of segments in both pipelines. Since there were over 17 million unique SSVID, a filter has been put in to only pull stats for SSVID that have a difference or don't have a match in the other pipeline.

# %%
q = f'''
WITH

segs_new AS (
  SELECT
  seg_id,
  ssvid,
  TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE3}.{SEGMENT_INFO_TABLE}`
),

segs_old AS (
  SELECT
  seg_id,
  ssvid,
  TIMESTAMP_DIFF(LEAST(last_timestamp, TIMESTAMP("2023-01-01")), first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE25}.{SEGMENT_INFO_TABLE}`
  WHERE first_timestamp < '2023-01-01'
),

ssvid_stats_new AS (
  SELECT
  ssvid,
  COUNT(*) as num_segs,
  SUM(seg_hours) as sum_seg_hours,
  FROM segs_new
  GROUP BY ssvid
),

ssvid_stats_old AS (
  SELECT
  ssvid,
  COUNT(*) as num_segs,
  SUM(seg_hours) as sum_seg_hours,
  FROM segs_old
  GROUP BY ssvid
)

SELECT
ssvid,
ssvid_stats_old.num_segs as num_segs_pipe25,
ssvid_stats_new.num_segs as num_segs_pipe3,
(ssvid_stats_new.num_segs - ssvid_stats_old.num_segs) as num_segs_diff,
ssvid_stats_old.sum_seg_hours as sum_seg_hours_pipe25,
ssvid_stats_new.sum_seg_hours as sum_seg_hours_pipe3,
(ssvid_stats_new.sum_seg_hours - ssvid_stats_old.sum_seg_hours) as sum_seg_hours_diff,
FROM ssvid_stats_old
FULL OUTER JOIN ssvid_stats_new
USING (ssvid)
-- WHERE (ssvid_stats_new.num_segs - ssvid_stats_old.num_segs) != 0
ORDER BY num_segs_diff
'''

# print(q)
df_ssvid_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# %%
df_ssvid_stats[df_ssvid_stats.num_segs_diff.abs() > 1000].num_segs_diff.hist(bins=100)

# %%
q = f'''
WITH

segs_new AS (
  SELECT
  seg_id,
  ssvid,
  TIMESTAMP_DIFF(last_timestamp, first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE3}.{SEGMENT_INFO_TABLE}`
),

segs_old AS (
  SELECT
  seg_id,
  ssvid,
  TIMESTAMP_DIFF(LEAST(last_timestamp, TIMESTAMP("2023-01-01")), first_timestamp, MINUTE)/60.0 AS seg_hours,
  FROM `{DATASET_PIPE25}.{SEGMENT_INFO_TABLE}`
  WHERE first_timestamp < '2023-01-01'
),

ssvid_stats_new AS (
  SELECT
  ssvid,
  COUNT(*) as num_segs,
  SUM(seg_hours) as sum_seg_hours,
  FROM segs_new
  GROUP BY ssvid
),

ssvid_stats_old AS (
  SELECT
  ssvid,
  COUNT(*) as num_segs,
  SUM(seg_hours) as sum_seg_hours,
  FROM segs_old
  GROUP BY ssvid
),

ssvid_comparison AS (
  SELECT
  ssvid,
  ssvid_stats_old.num_segs as num_segs_pipe25,
  ssvid_stats_new.num_segs as num_segs_pipe3,
  (ssvid_stats_new.num_segs - ssvid_stats_old.num_segs) as num_segs_diff,
  ssvid_stats_old.sum_seg_hours as sum_seg_hours_pipe25,
  ssvid_stats_new.sum_seg_hours as sum_seg_hours_pipe3,
  (ssvid_stats_new.sum_seg_hours - ssvid_stats_old.sum_seg_hours) as sum_seg_hours_diff,
  FROM ssvid_stats_old
  FULL OUTER JOIN ssvid_stats_new
  USING (ssvid)
)

SELECT
COUNT(*) as num_ssvid,
COUNTIF(num_segs_diff = 0) as ssvid_same_num_segs,
COUNTIF(num_segs_diff < 0) as ssvid_less_segs_pipe3,
COUNTIF(num_segs_diff > 0) as ssvid_more_segs_pipe3,
COUNTIF(sum_seg_hours_diff = 0) as ssvid_same_hours,
COUNTIF(sum_seg_hours_diff < 0) as ssvid_less_hours_pipe3,
COUNTIF(sum_seg_hours_diff > 0) as ssvid_more_hours_pipe3,
-- Normalized by number of total SSVID
COUNTIF(num_segs_diff = 0)/COUNT(*) as prop_ssvid_same_num_segs,
COUNTIF(num_segs_diff < 0)/COUNT(*) as prop_ssvid_less_segs_pipe3,
COUNTIF(num_segs_diff > 0)/COUNT(*) as prop_ssvid_more_segs_pipe3,
COUNTIF(sum_seg_hours_diff = 0)/COUNT(*) as prop_ssvid_same_hours,
COUNTIF(sum_seg_hours_diff < 0)/COUNT(*) as prop_ssvid_less_hours_pipe3,
COUNTIF(sum_seg_hours_diff > 0)/COUNT(*) as prop_ssvid_more_hours_pipe3,
-- Average negative and positive changes
SUM(IF(num_segs_diff < 0, num_segs_diff, 0)) / COUNTIF(num_segs_diff < 0) AS avg_less_segs,
SUM(IF(num_segs_diff > 0, num_segs_diff, 0)) / COUNTIF(num_segs_diff > 0) AS avg_more_segs,
SUM(IF(sum_seg_hours_diff < 0, sum_seg_hours_diff, 0)) / COUNTIF(sum_seg_hours_diff < 0) AS avg_less_seg_hours,
SUM(IF(sum_seg_hours_diff > 0, sum_seg_hours_diff, 0)) / COUNTIF(sum_seg_hours_diff > 0) AS avg_more_seg_hours,
FROM ssvid_comparison
'''

df_ssvid_summary_stats = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# %%
df_ssvid_summary_stats


# %%

# %% [markdown]
# # Code for pulling tracks, as needed
#
# If you need to investigate particular MMSI, you can plot in this notebook using pyseas and/or you can download a CSV of tracks to upload to the Global Fishing Watch online map. For very spoofy MMSI or for long periods of time, the map may be a better option as pyseas will struggle or fail to render too many segments. Also if there are a lot of segments, be sure to set `plot_legend` to False as rendering a long list of segment names will cause it to slow down significantly or fail.

# %% [markdown]
# #### Plot with `pyseas`

# %%
def get_tracks(dataset, ssvid, start_date, end_date):
    '''
    Retrieve the tracks for a given MMSI over a given date range in
    the form of AIS messages with lat, lon, timestamp, and seg_id to 
    allow for plotting.

    Args:
        dataset: string representing BigQuery dataset containing messages_segmented table
        ssvid: MMSI for the vessel of interest
        start_date: first date to consider for tracks in format "YYYYMMDD"
        end_date: last date to consider for tracks in format "YYYYMMDD"

    Returns:
        dataframe of tracks
    '''
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
    '''
    Retrieve the segments for a given MMSI betwee a given date range.

    Args:
        dataset: string representing BigQuery dataset containing segment_info table
        ssvid: MMSI for the vessel of interest
        start_date: first date to consider for tracks in format "YYYY-MM-DD"
        end_date: last date to consider for tracks in format "YYYY-MM-DD"

    Returns:
        dataframe of segments
    '''
    q = f'''
    SELECT 
    *
    FROM `{dataset}.{SEGMENT_INFO_TABLE}`
    WHERE ssvid = '{ssvid}'
    AND (DATE(first_timestamp) <= '{end_date}' AND DATE(last_timestamp) >= '{start_date}')
    ORDER BY seg_id
    '''

    # print(q)
    return pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')


# %%
def plot_segmented_tracks(df, plot_legend=True):
    '''
    Plot tracks on a map with different segments divided by color.

    Args:
        df: dataframe containing tracks to plot, requires lat, lon, timestamp, and seg_id
        plot_legend: if legend should be added (default=True)

    Returns:
        matplotlib figure
    '''
    with psm.context(psm.styles.panel):
        fig = plt.figure(figsize=(12, 12))
        info = psc.multi_track_panel(
            df.timestamp,
            df.lon,
            df.lat,
            df.seg_id,
            plots=[{"label": "lon", "values": df.lon}, {"label": "lat", "values": df.lat}],
        )
        if plot_legend:
            plt.legend(
                info.legend_handles.values(),
                [x.split("-", 1)[1].rstrip(".000000000Z") for x in info.legend_handles.keys()],
            )
        return fig



# %%
df_ssvid_stats

# %% [markdown]
# ##### Step 1: Explore segments to chose dates to pull tracks

# %%
ssvid_1 = '412000000'
segs_1 = get_segments(DATASET_PIPE3, ssvid_1, '2020-01-15', '2020-01-15')

# %%
segs_1

# %% [markdown]
# ##### Pull and plot tracks

# %%
start_date_1 = '20200115'
end_date_1 = '20200115'
tracks1_new = get_tracks(DATASET_PIPE3, ssvid_1, start_date_1, end_date_1)
tracks1_old = get_tracks(DATASET_PIPE25, ssvid_1, start_date_1, end_date_1)


# %%
fig = plot_segmented_tracks(tracks1_new, plot_legend=False)

# %%
fig = plot_segmented_tracks(tracks1_old, plot_legend=False)

# %% [markdown]
# ##### `413000000` example of tracks

# %%
tracks_413000000_new = get_tracks(DATASET_PIPE3, '413000000', '20200102', '20200102')
tracks_413000000_old = get_tracks(DATASET_PIPE25, '413000000', '20200102', '20200102')

# %%
fig = plot_segmented_tracks(tracks_413000000_new, plot_legend=False)

# %%
fig = plot_segmented_tracks(tracks_413000000_old, plot_legend=False)

# %% [markdown]
# #### Output CSV for use with GFW map
#
# These CSV can be uploaded in the GFW map (https://globalfishingwatch.org/map) in the `Environments` section.

# %%
data_folder = 'data'
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# %%
tracks1_new.to_csv(f'{data_folder}/tracks_{ssvid_1}_new.csv')
tracks1_old.to_csv(f'{data_folder}/tracks_{ssvid_1}_old.csv')

