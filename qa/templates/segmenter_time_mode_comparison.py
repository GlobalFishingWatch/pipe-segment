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
# # Segmenter Time Mode Comparison
#
# This notebook compares pipelines run over the same time period but in different time modes (i.e. daily, monthly, yearly) to make sure the segmenter is stable across modes.
#
# Author: Jenn Van Osdel  
# Last Modified: August 24, 2022

# %%
import pandas as pd
pd.set_option("max_rows", 20)

from config import DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, DATASET_NEW_BACKFILL_PUB, DATASET_NEW_MONTHLY_PUB, SAT_OFFSETS_TABLE, FRAGMENTS_TABLE, SEGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, SEGMENT_IDENTITY_DAILY_TABLE, SEGMENT_VESSEL_DAILY_TABLE, SEGMENT_INFO_TABLE, SEGMENT_VESSEL_TABLE, VESSEL_INFO_TABLE


# %%
def unnest_checks(dataset_backfill, dataset_monthly, table_name, unnest_columns):
    dfs = []
    for col in unnest_columns:
        q = f'''
            SELECT * EXCEPT ({', '.join(unnest_columns)})
            FROM `{dataset_backfill}.{table_name}*`, UNNEST({col})
            EXCEPT DISTINCT
            SELECT * EXCEPT ({', '.join(unnest_columns)})
            FROM `{DATASET_NEW_MONTHLY_INT}.{table_name}*`, UNNEST({col})
            '''
    
        df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
        dfs.append((col, df))
    return dfs


# %%
def struct_checks(dataset_backfill, dataset_monthly, table_name, struct_columns):
    dfs = []
    for col in struct_columns:
        q = f'''
            SELECT * EXCEPT ({', '.join(struct_columns)}), {col}.*
            FROM `{dataset_backfill}.{table_name}*`
            EXCEPT DISTINCT
            SELECT * EXCEPT ({', '.join(struct_columns)}), {col}.*
            FROM `{dataset_monthly}.{table_name}*`
            '''
    
        df = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')
        dfs.append((col, df))
    return dfs


# %% [markdown]
# ### `sat_time_offsets`

# %%
q = f'''
SELECT * EXCEPT (avg_distance_from_sat_km), TRUNC(CAST(avg_distance_from_sat_km AS NUMERIC), 5) AS avg_distance_from_sat_km
FROM `{DATASET_NEW_BACKFILL_INT}.{SAT_OFFSETS_TABLE}*`
EXCEPT DISTINCT
SELECT * EXCEPT (avg_distance_from_sat_km), TRUNC(CAST(avg_distance_from_sat_km AS NUMERIC), 5) AS avg_distance_from_sat_km
FROM `{DATASET_NEW_MONTHLY_INT}.{SAT_OFFSETS_TABLE}*`
'''

df_sat_time_offsets_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_sat_time_offsets_check.shape[0] == 0)
print(f"PASS: {SAT_OFFSETS_TABLE}")

# %% [markdown]
# ### `messages_segmented`

# %%
q = f'''
SELECT *
FROM `{DATASET_NEW_BACKFILL_INT}.{MESSAGES_SEGMENTED_TABLE}*`
EXCEPT DISTINCT
SELECT *
FROM `{DATASET_NEW_MONTHLY_INT}.{MESSAGES_SEGMENTED_TABLE}*`
'''

df_messages_segmented_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_messages_segmented_check.shape[0] == 0)
print(f"PASS: {MESSAGES_SEGMENTED_TABLE}")


# %% [markdown]
# ### `fragments`

# %%
q = f'''
SELECT * EXCEPT (identities, destinations)
FROM `{DATASET_NEW_BACKFILL_INT}.{FRAGMENTS_TABLE}*`
EXCEPT DISTINCT
SELECT * EXCEPT (identities, destinations)
FROM `{DATASET_NEW_MONTHLY_INT}.{FRAGMENTS_TABLE}*`
'''

df_fragments_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_fragments_check_basic.shape[0] == 0)
print(f"PASS: {FRAGMENTS_TABLE} basic check")


# %%
fragments_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, FRAGMENTS_TABLE, ['identities', 'destinations'])

for (col, df) in fragments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {FRAGMENTS_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {FRAGMENTS_TABLE} advanced check - {col}")

# %% [markdown]
# ### `segments`

# %%
q = f'''
SELECT * EXCEPT (daily_identities, daily_destinations, cumulative_identities, cumulative_destinations)
FROM `{DATASET_NEW_BACKFILL_INT}.{SEGMENTS_TABLE}*`
EXCEPT DISTINCT
SELECT * EXCEPT (daily_identities, daily_destinations, cumulative_identities, cumulative_destinations)
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENTS_TABLE}*`
'''

df_segments_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segments_check_basic.shape[0] == 0)
print(f"PASS: {SEGMENTS_TABLE} basic check")


# %%
segments_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENTS_TABLE, ['daily_identities', 'daily_destinations', 'cumulative_identities', 'cumulative_destinations'])

for (col, df) in segments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENTS_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENTS_TABLE} advanced check - {col}")



# %% [markdown]
# ### `segment_identity_daily`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_BACKFILL_INT}.{SEGMENT_IDENTITY_DAILY_TABLE}*`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_IDENTITY_DAILY_TABLE}*`
'''

df_segment_identity_daily_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_identity_daily_check_basic.shape[0] == 0)
print(f"PASS: {SEGMENT_IDENTITY_DAILY_TABLE} basic check")


# %%
segment_identity_daily_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_IDENTITY_DAILY_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_identity_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_IDENTITY_DAILY_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_IDENTITY_DAILY_TABLE} advanced check - {col}")



# %% [markdown]
# ### `segment_vessel_daily`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_BACKFILL_INT}.{SEGMENT_VESSEL_DAILY_TABLE}*`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_VESSEL_DAILY_TABLE}*`
'''

df_segment_vessel_daily_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_vessel_daily_check_basic.shape[0] == 0)
print(f"PASS: {SEGMENT_VESSEL_DAILY_TABLE} basic check")


# %%
segment_vessel_daily_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_VESSEL_DAILY_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_vessel_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_VESSEL_DAILY_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_VESSEL_DAILY_TABLE} advanced check - {col}")



# %% [markdown]
# ### `segment_info`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_BACKFILL_INT}.{SEGMENT_INFO_TABLE}`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_INFO_TABLE}`
'''

df_segment_info_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_info_check_basic.shape[0] == 0)
print(f"PASS: {SEGMENT_INFO_TABLE} basic check")


# %%
segment_info_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_INFO_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_info_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_INFO_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_INFO_TABLE} advanced check - {col}")



# %% [markdown]
# ### `segment_vessel`

# %%
q = f'''
SELECT * 
FROM `{DATASET_NEW_BACKFILL_INT}.{SEGMENT_VESSEL_TABLE}`
EXCEPT DISTINCT
SELECT * 
FROM `{DATASET_NEW_MONTHLY_INT}.{SEGMENT_VESSEL_TABLE}`
'''

df_segment_vessel_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_vessel_check_basic.shape[0] == 0)
print(f"PASS: {SEGMENT_VESSEL_TABLE} basic check")


# %% [markdown]
# ### `vessel_info`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_BACKFILL_PUB}.{VESSEL_INFO_TABLE}`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{DATASET_NEW_MONTHLY_PUB}.{VESSEL_INFO_TABLE}`
'''

df_vessel_info_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_vessel_info_check_basic.shape[0] == 0)
print(f"PASS: {VESSEL_INFO_TABLE} basic check")


# %%
vessel_info_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_PUB, DATASET_NEW_MONTHLY_PUB, VESSEL_INFO_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in vessel_info_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {VESSEL_INFO_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {VESSEL_INFO_TABLE} advanced check - {col}")



# %%
