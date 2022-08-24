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

# %%
dataset_backfill = 'pipe_ais_test_20220821_backfill_internal'
dataset_monthly = 'pipe_ais_test_20220821_monthly_internal'

sat_offsets_table = 'sat_time_offsets_'
messages_segmented_table = 'messages_segmented_'
fragments_table = 'fragments_'
segments_table = 'segments_'
segment_identity_daily_table = 'segment_identity_daily_'
segment_vessel_daily_table = 'segment_vessel_daily_'
segment_info_table = 'segment_info'
segment_vessel_table = 'segment_vessel'


# %%
def unnest_checks(dataset_backfill, dataset_monthly, table_name, unnest_columns):
    dfs = []
    for col in unnest_columns:
        q = f'''
            SELECT * EXCEPT ({', '.join(unnest_columns)})
            FROM `{dataset_backfill}.{table_name}*`, UNNEST({col})
            EXCEPT DISTINCT
            SELECT * EXCEPT ({', '.join(unnest_columns)})
            FROM `{dataset_monthly}.{table_name}*`, UNNEST({col})
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
FROM `{dataset_backfill}.{sat_offsets_table}*`
EXCEPT DISTINCT
SELECT * EXCEPT (avg_distance_from_sat_km), TRUNC(CAST(avg_distance_from_sat_km AS NUMERIC), 5) AS avg_distance_from_sat_km
FROM `{dataset_monthly}.{sat_offsets_table}*`
'''

df_sat_time_offsets_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_sat_time_offsets_check.shape[0] == 0)
print(f"PASS: {sat_offsets_table}")

# %% [markdown]
# ### `messages_segmented`

# %%
q = f'''
SELECT *
FROM `{dataset_backfill}.{messages_segmented_table}*`
EXCEPT DISTINCT
SELECT *
FROM `{dataset_monthly}.{messages_segmented_table}*`
'''

df_messages_segmented_check = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_messages_segmented_check.shape[0] == 0)
print(f"PASS: {messages_segmented_table}")


# %% [markdown]
# ### `fragments`

# %%
q = f'''
SELECT * EXCEPT (identities, destinations)
FROM `{dataset_backfill}.{fragments_table}*`
EXCEPT DISTINCT
SELECT * EXCEPT (identities, destinations)
FROM `{dataset_monthly}.{fragments_table}*`
'''

df_fragments_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_fragments_check_basic.shape[0] == 0)
print(f"PASS: {fragments_table} basic check")


# %%
fragments_advanced_checks = unnest_checks(dataset_backfill, dataset_monthly, fragments_table, ['identities', 'destinations'])

for (col, df) in fragments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {fragments_table} advanced check - {col}")
    except:
        print(f"FAIL: {fragments_table} advanced check - {col}")

# %% [markdown]
# ### `segments`

# %%
q = f'''
SELECT * EXCEPT (daily_identities, daily_destinations, cumulative_identities, cumulative_destinations)
FROM `{dataset_backfill}.{segments_table}*`
EXCEPT DISTINCT
SELECT * EXCEPT (daily_identities, daily_destinations, cumulative_identities, cumulative_destinations)
FROM `{dataset_monthly}.{segments_table}*`
'''

df_segments_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segments_check_basic.shape[0] == 0)
print(f"PASS: {segments_table} basic check")


# %%
segments_advanced_checks = unnest_checks(dataset_backfill, dataset_monthly, segments_table, ['daily_identities', 'daily_destinations', 'cumulative_identities', 'cumulative_destinations'])

for (col, df) in segments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {segments_table} advanced check - {col}")
    except:
        print(f"FAIL: {segments_table} advanced check - {col}")



# %% [markdown]
# ### `segment_identity_daily`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_backfill}.{segment_identity_daily_table}*`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_monthly}.{segment_identity_daily_table}*`
'''

df_segment_identity_daily_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_identity_daily_check_basic.shape[0] == 0)
print(f"PASS: {segment_identity_daily_table} basic check")


# %%
segment_identity_daily_advanced_checks = unnest_checks(dataset_backfill, dataset_monthly, segment_identity_daily_table, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_identity_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {segment_identity_daily_table} advanced check - {col}")
    except:
        print(f"FAIL: {segment_identity_daily_table} advanced check - {col}")



# %% [markdown]
# ### `segment_vessel_daily`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_backfill}.{segment_vessel_daily_table}*`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_monthly}.{segment_vessel_daily_table}*`
'''

df_segment_vessel_daily_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_vessel_daily_check_basic.shape[0] == 0)
print(f"PASS: {segment_vessel_daily_table} basic check")


# %%
segment_vessel_daily_advanced_checks = struct_checks(dataset_backfill, dataset_monthly, segment_vessel_daily_table, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_vessel_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {segment_vessel_daily_table} advanced check - {col}")
    except:
        print(f"FAIL: {segment_vessel_daily_table} advanced check - {col}")



# %% [markdown]
# ### `segment_info`

# %%
q = f'''
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_backfill}.{segment_info_table}`
EXCEPT DISTINCT
SELECT * EXCEPT (shipname, callsign, imo, n_shipname, n_callsign, n_imo, shiptype, length, width)
FROM `{dataset_monthly}.{segment_info_table}`
'''

df_segment_info_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_info_check_basic.shape[0] == 0)
print(f"PASS: {segment_info_table} basic check")


# %%
segment_info_advanced_checks = struct_checks(dataset_backfill, dataset_monthly, segment_info_table, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_info_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {segment_info_table} advanced check - {col}")
    except:
        print(f"FAIL: {segment_info_table} advanced check - {col}")



# %% [markdown]
# ### `segment_vessel`

# %%
q = f'''
SELECT * 
FROM `{dataset_backfill}.{segment_vessel_table}`
EXCEPT DISTINCT
SELECT * 
FROM `{dataset_monthly}.{segment_vessel_table}`
'''

df_segment_vessel_check_basic = pd.read_gbq(q, project_id='world-fishing-827', dialect='standard')

assert(df_segment_vessel_check_basic.shape[0] == 0)
print(f"PASS: {segment_vessel_table} basic check")


# %%
