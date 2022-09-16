# Segmenter Time Mode Comparison

This notebook compares pipelines run over the same time period but in different time modes (i.e. daily, monthly, yearly) to make sure the segmenter is stable across modes.

Author: Jenn Van Osdel  
Last Modified: August 24, 2022


```python
import pandas as pd
pd.set_option("max_rows", 20)

from config import DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, DATASET_NEW_BACKFILL_PUB, DATASET_NEW_MONTHLY_PUB, SAT_OFFSETS_TABLE, FRAGMENTS_TABLE, SEGMENTS_TABLE, MESSAGES_SEGMENTED_TABLE, SEGMENT_IDENTITY_DAILY_TABLE, SEGMENT_VESSEL_DAILY_TABLE, SEGMENT_INFO_TABLE, SEGMENT_VESSEL_TABLE, VESSEL_INFO_TABLE
```


```python
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
```


```python
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
```

### `sat_time_offsets`


```python
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
```

    /Users/jennifervanosdel/miniconda3/envs/rad/lib/python3.9/site-packages/google/auth/_default.py:81: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. We recommend you rerun `gcloud auth application-default login` and make sure a quota project is added. Or you can use service accounts instead. For more information about service accounts, see https://cloud.google.com/docs/authentication/
      warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)


    PASS: sat_time_offsets_


### `messages_segmented`


```python
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
```

    PASS: messages_segmented_


### `fragments`


```python
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
```

    PASS: fragments_ basic check



```python
fragments_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, FRAGMENTS_TABLE, ['identities', 'destinations'])

for (col, df) in fragments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {FRAGMENTS_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {FRAGMENTS_TABLE} advanced check - {col}")
```

    PASS: fragments_ advanced check - identities
    PASS: fragments_ advanced check - destinations


### `segments`


```python
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
```

    PASS: segments_ basic check



```python
segments_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENTS_TABLE, ['daily_identities', 'daily_destinations', 'cumulative_identities', 'cumulative_destinations'])

for (col, df) in segments_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENTS_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENTS_TABLE} advanced check - {col}")


```

    PASS: segments_ advanced check - daily_identities
    PASS: segments_ advanced check - daily_destinations
    PASS: segments_ advanced check - cumulative_identities
    PASS: segments_ advanced check - cumulative_destinations


### `segment_identity_daily`


```python
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
```

    PASS: segment_identity_daily_ basic check



```python
segment_identity_daily_advanced_checks = unnest_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_IDENTITY_DAILY_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_identity_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_IDENTITY_DAILY_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_IDENTITY_DAILY_TABLE} advanced check - {col}")


```

    PASS: segment_identity_daily_ advanced check - shipname
    PASS: segment_identity_daily_ advanced check - callsign
    PASS: segment_identity_daily_ advanced check - imo
    PASS: segment_identity_daily_ advanced check - n_shipname
    PASS: segment_identity_daily_ advanced check - n_callsign
    PASS: segment_identity_daily_ advanced check - n_imo
    PASS: segment_identity_daily_ advanced check - shiptype
    PASS: segment_identity_daily_ advanced check - length
    PASS: segment_identity_daily_ advanced check - width


### `segment_vessel_daily`


```python
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
```

    PASS: segment_vessel_daily_ basic check



```python
segment_vessel_daily_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_VESSEL_DAILY_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_vessel_daily_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_VESSEL_DAILY_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_VESSEL_DAILY_TABLE} advanced check - {col}")


```

    PASS: segment_vessel_daily_ advanced check - shipname
    PASS: segment_vessel_daily_ advanced check - callsign
    PASS: segment_vessel_daily_ advanced check - imo
    PASS: segment_vessel_daily_ advanced check - n_shipname
    PASS: segment_vessel_daily_ advanced check - n_callsign
    PASS: segment_vessel_daily_ advanced check - n_imo
    PASS: segment_vessel_daily_ advanced check - shiptype
    PASS: segment_vessel_daily_ advanced check - length
    PASS: segment_vessel_daily_ advanced check - width


### `segment_info`


```python
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
```

    PASS: segment_info basic check



```python
segment_info_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_INT, DATASET_NEW_MONTHLY_INT, SEGMENT_INFO_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in segment_info_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {SEGMENT_INFO_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {SEGMENT_INFO_TABLE} advanced check - {col}")


```

    PASS: segment_info advanced check - shipname
    PASS: segment_info advanced check - callsign
    PASS: segment_info advanced check - imo
    PASS: segment_info advanced check - n_shipname
    PASS: segment_info advanced check - n_callsign
    PASS: segment_info advanced check - n_imo
    PASS: segment_info advanced check - shiptype
    PASS: segment_info advanced check - length
    PASS: segment_info advanced check - width


### `segment_vessel`


```python
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
```

    PASS: segment_vessel basic check


### `vessel_info`


```python
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
```

    PASS: vessel_info basic check



```python
vessel_info_advanced_checks = struct_checks(DATASET_NEW_BACKFILL_PUB, DATASET_NEW_MONTHLY_PUB, VESSEL_INFO_TABLE, ['shipname', 'callsign', 'imo', 'n_shipname', 'n_callsign', 'n_imo', 'shiptype', 'length', 'width'])

for (col, df) in vessel_info_advanced_checks:
    try:
        assert(df.shape[0] == 0)
        print(f"PASS: {VESSEL_INFO_TABLE} advanced check - {col}")
    except:
        print(f"FAIL: {VESSEL_INFO_TABLE} advanced check - {col}")
```

    PASS: vessel_info advanced check - shipname
    PASS: vessel_info advanced check - callsign
    PASS: vessel_info advanced check - imo
    PASS: vessel_info advanced check - n_shipname
    PASS: vessel_info advanced check - n_callsign
    PASS: vessel_info advanced check - n_imo
    PASS: vessel_info advanced check - shiptype
    PASS: vessel_info advanced check - length
    PASS: vessel_info advanced check - width



```python

```
