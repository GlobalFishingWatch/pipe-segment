# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     formats: ipynb,py:percent
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
# # Segmenter Basic Checks
#
# Sanity checks for `segments_` and `messages_segmented_`.
#
# Will eventually be automated as part of the pipeline and may no longer be a necessary part of manual QA so long as the checks were run as part of the pipeline you will to check.
#
# Author: Jenn Van Osdel  
# Last Updated: August 24, 2022

# %%
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
from datetime import datetime

from config import DATASET_OLD, DATASET_NEW_MONTHLY_INT, FRAGMENTS_TABLE, SEGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, NUM_SEGS_DIFF_THRESHOLD

mpl.rcParams['figure.facecolor'] = 'white'
pd.set_option("max_rows", 40)

# %% [markdown]
# # FINAL QUERIES FOR AUTOMATED QA
#
# This notebook runs the queries on every date shard in the table, but these queries can be used directly in Airflow for a given day instead.

# %%
q = f'''
SELECT DISTINCT _table_suffix as shard_date
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`
ORDER BY shard_date
'''

shard_dates_segments = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
q = f'''
SELECT DISTINCT _table_suffix as shard_date
FROM `{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}*`
ORDER BY shard_date
'''

shard_dates_messages_segmented = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
q = f'''
SELECT DISTINCT _table_suffix as shard_date
FROM `{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}*`
ORDER BY shard_date
'''

shard_dates_fragments = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
try:
    assert(shard_dates_segments.shard_date.to_list() == shard_dates_messages_segmented.shard_date.to_list() == shard_dates_fragments.shard_date.to_list())
except:
    print("ERROR: Shard dates do not match between tables. DO NOT CONTINUE QA BEFORE RECONCILING!")

# %% [markdown]
# ## Check num_segs against 30-day rolling average for a given day
#
# Cost: 0MB

# %%
def check_rolling_average(shard_date):
    # Query to get the 30-day rolling average for a particular day
    q = f'''
    WITH 

    calc AS (
    SELECT COUNT(*)/30 rolling_average
    FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', DATE_SUB(PARSE_DATE("%Y%m%d", "{shard_date}"), INTERVAL 30 DAY)) AND FORMAT_TIMESTAMP('%Y%m%d', DATE_SUB(PARSE_DATE("%Y%m%d", "{shard_date}"), INTERVAL 1 DAY))
    )

    SELECT 
    IF(ABS((SELECT rolling_average FROM calc) - COUNT(*))/(SELECT rolling_average FROM calc) < {NUM_SEGS_DIFF_THRESHOLD}, 1, 0) as good_num_segs,
    FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}{shard_date}`
    '''

    num_segs_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in num_segs_check.columns:
            assert(num_segs_check.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED: {shard_date}")
        return False

# %%
# Cannot perform the check for the first 30 days reliably.
all_passed = True
failed_dates = []
for date in shard_dates_segments.shard_date[30:]:
    outcome = check_rolling_average(date)
    if not outcome:
        all_passed = False
        failed_dates.append(date)

failed_dates = [datetime.strptime(date, "%Y%m%d").strftime('%Y-%m-%d') for date in failed_dates]

if all_passed:
    print("ALL PASSED")

# %%
if not all_passed:
    q = f'''
    WITH 

    segment_data AS (
        SELECT DATE(timestamp) as date, seg_id
        FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`
    )

    SELECT 
    date,
    EXTRACT(YEAR from date) as year,
    COUNT(*) AS num_segs,
    COUNT(DISTINCT seg_id) AS num_segs_distinct,
    FROM segment_data
    GROUP BY date
    ORDER BY date
    '''

    # print(q)
    df_segs_daily = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    assert(df_segs_daily[df_segs_daily.num_segs != df_segs_daily.num_segs_distinct].shape[0] == 0)

    df_segs_daily['num_segs_30day_avg'] = df_segs_daily.rolling(window=30, closed='left').num_segs.mean()
    df_segs_daily['num_segs_30day_avg_diff'] = df_segs_daily.num_segs - df_segs_daily.num_segs_30day_avg
    df_segs_daily['num_segs_30day_avg_diff_perc'] = (df_segs_daily.num_segs - df_segs_daily.num_segs_30day_avg)/df_segs_daily.num_segs_30day_avg
    df_segs_daily = df_segs_daily.set_index(pd.to_datetime(df_segs_daily.date))

# %%
if not all_passed:
    fig = plt.figure(figsize=(10, 5))
    ax = df_segs_daily.num_segs.plot(label="Number of segments")
    df_segs_daily.num_segs_30day_avg.plot(label="Rolling 30-day avg.")
    (df_segs_daily.num_segs_30day_avg * 1.2).plot(label="Threshold - upper/lower bound", c="orange", ls="--", lw=1)
    (df_segs_daily.num_segs_30day_avg * 0.8).plot(label="_nolegend_", c="orange", ls="--", lw=1)
    for date in failed_dates:
        plt.axvline(date, c="red", ls='--', lw=0.7)
    plt.legend()
    plt.show()

# %% [markdown]
# ## Sanity checks

# %% [markdown]
# ### `segments_`
# * `daily_message_count` > 0
# * `cumulative_msg_count` > 0
# * ssvid portion of `frag_id` == `ssvid`
# * DATE(`frag_id`) >= DATE(`seg_id`)
# * No duplicate `seg_id` (still broken in `20220329` version)
#
# **NOTE: duplicate `seg_ids` have not been fixed yet so the below query fails the assertion**

# %% [markdown]
# ##### Validity checks
# *Cost: ~18KB depending on size of day*

# %%
def check_segments_validity(shard_date):
    q = f'''
    CREATE TEMP FUNCTION timestamp_from_id(frag_id STRING) AS
    ((
        SELECT 
        TIMESTAMP(STRING_AGG(arr, '-'))
        FROM UNNEST(SPLIT(frag_id, '-')) AS arr WITH OFFSET as offset
        WHERE offset BETWEEN 1 and 3
    ));

    SELECT 
    IF(SUM(IF(daily_msg_count > 0, 1, 0)) = COUNT(*), 1, 0) as valid_daily_msg_ct,
    IF(SUM(IF(cumulative_msg_count > 0, 1, 0)) = COUNT(*), 1, 0) as valid_cumul_msg_ct,
    IF(SUM(IF(LEFT(frag_id, STRPOS(frag_id, "-")-1) = ssvid, 1, 0)) = COUNT(*), 1, 0) as valid_frag_id_ssvid,
    IF(SUM(IF(timestamp_from_id(frag_id) >= timestamp_from_id(seg_id), 1, 0)) = COUNT(*), 1, 0) as valid_frag_seg_pair,
    FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}{shard_date}`
    '''
    # print(q)
    segment_validity_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in segment_validity_check.columns:
            assert(segment_validity_check.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED {shard_date}")
        return False

# %%
all_passed = True
for date in shard_dates_segments.shard_date:
    outcome = check_segments_validity(date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %% [markdown]
# ### `messages_segmented_`
# * DATE(`frag_id`) == DATE(`timestamp`)
# * DATE(`frag_id`) >= DATE(`seg_id`)
# * `source` is one of [`spire` , `orbcomm`]
# * `type` starts with `AIS.`
# * `receiver_type` is one of [`satellite` , `terrestrial`]
# * Check that we never get invalid values for certain columns: `lon`, `lat`, `course`, `heading`, `speed`, `shipname`, `callsign`, `destination`
#
# *Cost: ~5-6MB depending on size of day*

# %%
def check_messages_segmented_validity(shard_date):
    q = f'''
    CREATE TEMP FUNCTION time_from_segid_array(arr ARRAY<STRING>) AS
    ((
        SELECT TIMESTAMP(CONCAT(arr[OFFSET(1)], '-', arr[OFFSET(2)], '-', arr[OFFSET(3)]))
    ));

    SELECT
    IF(SUM(IF(DATE(time_from_segid_array(SPLIT(frag_id, '-'))) = DATE(timestamp), 1, 0)) = COUNTIF(frag_id IS NOT NULL), 1, 0) as valid_frag_id_timestamp,
    IF(SUM(IF(LEFT(frag_id, STRPOS(frag_id, "-")-1) = ssvid, 1, 0)) = COUNTIF(frag_id IS NOT NULL), 1, 0) as valid_frag_id_ssvid,
    IF(SUM(IF(TIMESTAMP(time_from_segid_array(SPLIT(frag_id, '-'))) >= TIMESTAMP(time_from_segid_array(SPLIT(seg_id, '-'))), 1, 0)) = COUNTIF((frag_id IS NOT NULL) AND (seg_id IS NOT NULL)), 1, 0) as valid_frag_seg_pair,
    IF(SUM(IF(source IN ("spire", "orbcomm"), 1, 0)) = COUNT(*), 1, 0) as valid_source,
    IF(SUM(IF(STARTS_WITH(type, "AIS"), 1, 0)) = COUNT(*), 1, 0) as valid_type,
    IF(SUM(IF(receiver_type IN ("satellite", "terrestrial"), 1, 0)) = COUNT(*), 1, 0) as valid_receiver_type,
    IF(COUNTIF((lon IS NOT NULL) AND (lon <= -181 OR lon >= 181)) = 0, 1, 0) AS valid_lon,
    IF(COUNTIF((lat IS NOT NULL) AND (lat <= -91 OR lat >= 91)) = 0, 1, 0) AS valid_lat,
    IF(COUNTIF((course IS NOT NULL) AND (course < 0 OR course >= 360)) = 0, 1, 0) AS valid_course,
    IF(COUNTIF((heading IS NOT NULL) AND (heading < 0 OR heading >= 360)) = 0, 1, 0) AS valid_heading,
    IF(COUNTIF((speed IS NOT NULL) AND (speed < 0 OR speed >= 102.3)) = 0, 1, 0) AS valid_speed_most,
    IF(COUNTIF((type = 'AIS.27') AND (speed IS NOT NULL) AND (speed < 0 OR speed >= 63)) = 0, 1, 0) AS valid_speed_AIS27,
    IF(COUNTIF((type IN ('AIS.5', 'AIS.19', 'AIS.21', 'AIS.24')) AND (shipname = '@@@@@@@@@@@@@@@@@@@@')) = 0, 1, 0) AS valid_shipname,
    IF(COUNTIF((type IN ('AIS.5', 'AIS.24')) AND (shipname = '@@@@@@@')) = 0, 1, 0) AS valid_callsign,
    IF(COUNTIF((type = 'AIS.5') AND (destination = '@@@@@@@@@@@@@@@@@@@@')) = 0, 1, 0) AS valid_destination,
    FROM `{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}{shard_date}`
    '''

    messages_segmented_check_1 = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in messages_segmented_check_1.columns:
            assert(messages_segmented_check_1.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED: {shard_date}")
        return False

# %%
all_passed = True
for date in shard_dates_messages_segmented.shard_date:
    outcome = check_messages_segmented_validity(date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %% [markdown]
# ### `fragments_`
# * `msg_count` > 0
# * timestamp portion of `frag_id` == `first_msg_timestamp`
# * date portion of `frag_id` == date portion of `timestamp` and `last_msg_timestamp`
# * ssvid portion of `frag_id` == `ssvid`
# * No duplicate `frag_id`

# %%
def check_fragments_validity(shard_date):
    q = f'''
    CREATE TEMP FUNCTION timestamp_from_id(frag_id STRING) AS
    ((
        SELECT 
        TIMESTAMP(STRING_AGG(arr, '-'))
        FROM UNNEST(SPLIT(frag_id, '-')) AS arr WITH OFFSET as offset
        WHERE offset BETWEEN 1 and 3
    ));

    SELECT 
    IF(SUM(IF(msg_count > 0, 1, 0)) = COUNT(*), 1, 0) as valid_msg_count,
    IF(SUM(IF(timestamp_from_id(frag_id) = first_msg_timestamp, 1, 0)) = COUNT(*), 1, 0) as valid_first_msg_timestamp,
    IF(SUM(IF(DATE(timestamp_from_id(frag_id)) = DATE(timestamp), 1, 0)) = COUNT(*), 1, 0) as valid_timestamp,
    IF(SUM(IF(DATE(timestamp_from_id(frag_id)) = DATE(last_msg_timestamp), 1, 0)) = COUNT(*), 1, 0) as valid_last_msg_timestamp,
    IF(SUM(IF(LEFT(frag_id, STRPOS(frag_id, "-")-1) = ssvid, 1, 0)) = COUNT(*), 1, 0) as valid_frag_id_ssvid,
    FROM `{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}{shard_date}`
    '''
    # print(q)
    fragment_validity_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in fragment_validity_check.columns:
            assert(fragment_validity_check.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED {shard_date}")
        return False

# %%
all_passed = True
for date in shard_dates_fragments.shard_date:
    outcome = check_fragments_validity(date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %% [markdown]
# ### Duplicates check for `segments_` and `fragments_`
# *Cost: ~6KB depending on size of day*

# %%
def check_duplicate_ids(table, id_col, shard_date):
    q = f'''
    WITH

    duplicate_ids AS (
    SELECT
    {id_col}, COUNT(*)
    FROM `{table}{shard_date}`
    GROUP BY {id_col}
    HAVING COUNT(*) > 1
    )

    SELECT
    IF(COUNT(*) = 0, 1, 0) as dupe_check
    FROM duplicate_ids
    '''
    # print(q)
    duplicate_ids_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in duplicate_ids_check.columns:
            assert(duplicate_ids_check.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED: {shard_date}")
        return False

# %%
all_passed = True
for date in shard_dates_segments.shard_date:
    outcome = check_duplicate_ids(f"{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}", "seg_id", date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %%
all_passed = True
for date in shard_dates_segments.shard_date:
    outcome = check_duplicate_ids(f"{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}", "frag_id", date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %% [markdown]
# ### Fragment/segment join checks
#
# Make sure there is a one-to-one relationship between `segments_` and `framents_` on `frag_id` on every shard date. There should be a one-to-one relationship across all dates as well, but this is designed to be run on a daily basis. The construction of the `frag_id` should keep `frag_id` from having a duplicate on any other shard date by definition, but that is still an assumption we make here to keep checks at the daily level.

# %%
def check_seg_frag_relationship(seg_table, frag_table, shard_date):
    q = f'''
    SELECT
        IF(COUNTIF((seg.frag_id IS NULL) OR (frag.frag_id IS NULL)) = 0, 1, 0) as count_bad,
    FROM `{seg_table}{shard_date}` seg
    FULL OUTER JOIN `{frag_table}{shard_date}` frag
    USING (frag_id)
    '''
    # print(q)
    seg_frag_relationship_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

    try:
        for col in seg_frag_relationship_check.columns:
            assert(seg_frag_relationship_check.iloc[0][col] == 1)
        return True
    except:
        print(f"FAILED: {shard_date}")
        return False

# %%
all_passed = True
for date in shard_dates_segments.shard_date:
    outcome = check_seg_frag_relationship(f"{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}", f"{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}", date)
    if all_passed and not outcome:
        all_passed = False

if all_passed:
    print("ALL PASSED")

# %%


# %% [markdown]
# ---
# # EXPLORATORY ANALYSIS FOR `num_segs` METRIC
#
# ### !!! YOU CAN IGNORE THIS IF YOU JUST WANT TO DO THE QA QUERIES !!!
# Finding a metric that flags when the number of segments in a day deviates far enough from the norm to cause concern. This query will not stop the pipeline but instead raise a flag that the day's data should be inspected against historical segment numbers.
#
# *NOTE: If you would like to run, you will need to uncomment the cells below.*

# %%
# q = f'''
# WITH 

# segment_data AS (
#     SELECT DATE(timestamp) as date, seg_id, ssvid, closed, message_count,
#     TIMESTAMP_DIFF(last_msg_timestamp, first_msg_timestamp, SECOND)/3600 as seg_hours,
#     (SELECT COUNT(*) FROM UNNEST(shipnames) WHERE value IS NOT NULL) > 0 AS valid_shipname
#     FROM `{DATASET_OLD}.{SEGMENTS_TABLE}*`
# )

# SELECT 
# date,
# EXTRACT(YEAR from date) as year,
# COUNT(*) AS num_segs,
# COUNT(DISTINCT seg_id) AS num_segs_distinct,
# COUNTIF(closed) AS num_closed,
# COUNTIF(closed)/COUNT(*) AS ratio_closed,
# COUNTIF(valid_shipname) AS num_valid_shipname,
# COUNTIF(valid_shipname)/COUNT(*) AS ratio_valid_shipname,
# AVG(IF(seg_hours = 0, NULL, LOG10(seg_hours))) AS avg_log_length,
# COUNTIF(seg_hours = 0) AS num_zero_length,
# COUNTIF(seg_hours = 0)/COUNT(*) as ratio_zero_length
# FROM segment_data
# GROUP BY date
# ORDER BY date
# '''

# # print(q)
# df_segs_old_daily = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

# %%
# df_segs_old_daily_aug1 = df_segs_old_daily[df_segs_old_daily.date >= '2020-08-01'].reset_index(drop=True).copy()
# df_segs_old_daily_aug1['num_segs_7day_avg'] = df_segs_old_daily_aug1.rolling(window=7, closed='left').num_segs.mean()
# df_segs_old_daily_aug1['num_segs_7day_avg_diff'] = df_segs_old_daily_aug1.num_segs - df_segs_old_daily_aug1.num_segs_7day_avg
# df_segs_old_daily_aug1['num_segs_7day_avg_diff_perc'] = (df_segs_old_daily_aug1.num_segs - df_segs_old_daily_aug1.num_segs_7day_avg)/df_segs_old_daily_aug1.num_segs_7day_avg
# df_segs_old_daily_aug1['num_segs_30day_avg'] = df_segs_old_daily_aug1.rolling(window=30, closed='left').num_segs.mean()
# df_segs_old_daily_aug1['num_segs_30day_avg_diff'] = df_segs_old_daily_aug1.num_segs - df_segs_old_daily_aug1.num_segs_30day_avg
# df_segs_old_daily_aug1['num_segs_30day_avg_diff_perc'] = (df_segs_old_daily_aug1.num_segs - df_segs_old_daily_aug1.num_segs_30day_avg)/df_segs_old_daily_aug1.num_segs_30day_avg
# df_segs_old_daily_aug1

# %%
# ax = df_segs_old_daily_aug1.plot(x='date', y='num_segs', ylim=(125000, 400000))
# df_segs_old_daily_aug1.plot(ax=ax, x='date', y='num_segs_7day_avg', ylim=(125000, 400000))
# df_segs_old_daily_aug1.plot(ax=ax, x='date', y='num_segs_30day_avg', ylim=(125000, 400000))

# plt.title("Number of segments per day")
# plt.show()

# %%
# fig, (ax0, ax1) = plt.subplots(nrows=1, ncols=2, figsize=(15,5))
# df_segs_old_daily_aug1.num_segs_7day_avg_diff_perc.hist(ax=ax0, bins=100)
# df_segs_old_daily_aug1.num_segs_30day_avg_diff_perc.hist(ax=ax1, bins=100)
# ax0.set_title("Percent difference\nfrom 7-day rolling average")
# ax1.set_title("Percent difference\nfrom 30-day rolling average")
# plt.show()

# %%
# ax = df_segs_old_daily_aug1.plot(x='date', y='num_segs_7day_avg_diff_perc', label="7-day")#, ylim=(0, 400000))
# df_segs_old_daily_aug1.plot(ax=ax, x='date', y='num_segs_30day_avg_diff_perc', label="30-day")#, ylim=(0, 400000))

# diff_10perc_7day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_7day_avg_diff_perc.abs() > 0.1].copy().reset_index(drop=True)
# diff_10perc_30day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_30day_avg_diff_perc.abs() > 0.1].copy().reset_index(drop=True)
# diff_20perc_7day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_7day_avg_diff_perc.abs() > 0.2].copy().reset_index(drop=True)
# diff_20perc_30day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_30day_avg_diff_perc.abs() > 0.2].copy().reset_index(drop=True)
# diff_30perc_7day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_7day_avg_diff_perc.abs() > 0.3].copy().reset_index(drop=True)
# diff_30perc_30day = df_segs_old_daily_aug1[df_segs_old_daily_aug1.num_segs_30day_avg_diff_perc.abs() > 0.3].copy().reset_index(drop=True)

# plt.axhline(0.1, color='gray', ls='--')
# plt.axhline(-0.1, color='gray', ls='--')
# plt.axhline(0.2, color='black', ls='--')
# plt.axhline(-0.2, color='black', ls='--')
# plt.axhline(0.3, color='red', ls='--')
# plt.axhline(-0.3, color='red', ls='--')

# plt.title("Percent change in segments\nwith respect to 7 or 30 day rolling average")
# plt.show()

# %% [markdown]
#
# ### Assessing percent change methodology
#
# Seems like a gradual decline into the holidays with a sharp drop at the Lunar New Year in both years. The large drop in November is currently unexplained. These should all probably throw a flag as they are a sharp deviation that warrants some investigation. If a similar deviation happened unrealted to the winter holidays, it would be cause for concern. This methodology tends to trigger both when going into a sudden decrease/increase but also on the way out of that decrease/increase. This means that flags will cluster with long periods of time inbetween flags being thrown. 
#
# **I suggest using the 30-day rolling average methodology with a threshold of 0.2 as a first pass at this QA.**

# %%
# fig, ((ax0, ax1), (ax2, ax3), (ax4, ax5)) = plt.subplots(nrows=3, ncols=2, figsize=(15,10))

# # 10% from 7-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax0, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_10perc_7day.date.to_list():
#     ax0.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# # 10% from 30-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax1, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_10perc_30day.date.to_list():
#     ax1.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# # 20% from 7-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax2, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_20perc_7day.date.to_list():
#     ax2.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# # 20% from 30-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax3, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_20perc_30day.date.to_list():
#     ax3.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# # 30% from 7-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax4, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_30perc_7day.date.to_list():
#     ax4.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# # 30% from 30-day rolling average
# df_segs_old_daily_aug1.plot(ax=ax5, x='date', y='num_segs', ylim=(125000, 400000), legend=False)
# for date in diff_30perc_30day.date.to_list():
#     ax5.scatter(x=date, y=df_segs_old_daily_aug1[df_segs_old_daily_aug1.date == date].num_segs,
#                 color='black', s=15, zorder=10)

# ax0.set_ylabel("10% cutoff")
# ax2.set_ylabel("20% cutoff")
# ax4.set_ylabel("30% cutoff")
# ax0.set_title("7-day rolling average")
# ax1.set_title("30-day rolling average")

# plt.show()

# %% [markdown]
# #### Could also consider using the stdev for this clean time period as the bar for the future
#
# This also somewhat justifies the use of 0.2 or 0.3 depending on if we want to flag 2 or 3 standard deviations.

# %%
# print("2 stdev\n--------")
# print(f"7-day: {df_segs_old_daily_aug1[df_segs_old_daily_aug1.date >= '2020-08-01'].num_segs_7day_avg_diff_perc.std() * 2:0.3f}")
# print(f"30-day: {df_segs_old_daily_aug1[df_segs_old_daily_aug1.date >= '2020-08-01'].num_segs_30day_avg_diff_perc.std() * 2:0.3f}")
# print()
# print("3 stdev\n--------")
# print(f"7-day: {df_segs_old_daily_aug1[df_segs_old_daily_aug1.date >= '2020-08-01'].num_segs_7day_avg_diff_perc.std() * 3:0.3f}")
# print(f"30-day: {df_segs_old_daily_aug1[df_segs_old_daily_aug1.date >= '2020-08-01'].num_segs_30day_avg_diff_perc.std() * 3:0.3f}")


