# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.6.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
from collections import Counter
from datetime import timedelta
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import pyseas.maps as psm

# %matplotlib inline
# -

# ## Look at Satellite Noise vs Earlier, Daily Run

query = """
SELECT receiver, dt, date(_partitiontime) as date
FROM `world-fishing-827.gfw_research_precursors.satellite_timing` 
WHERE DATE(_PARTITIONTIME) BETWEEN "2018-07-01" AND  "2018-07-31" """
daily_offsets = pd.read_gbq(query, project_id='world-fishing-827')


# +
def plot_daily(df, n=2):
    bad_offsets = (abs(df.dt) > 30)
    bad_cnts = Counter(df[bad_offsets].receiver)
    rcvrs = [name for (name, cnt) in bad_cnts.most_common()[:n]]
    figure = plt.figure(figsize=(18, 9), facecolor='white')
    width = timedelta(minutes=24 * 60 / (n + 1))
    for i, rcvr in enumerate(rcvrs):
        mask = (df.receiver == rcvr)
        offsets = df[mask].copy()
        offsets.sort_values(by='date', inplace=True)
        times = offsets.date + i * width
        plt.bar(times, offsets.dt, width=width, label=rcvr)
    plt.ylabel('seconds')
    plt.title('Satellite Clock Offset by Date')
    plt.legend()
    return rcvrs
    
rcvrs = plot_daily(daily_offsets, 5)
# -

query = """
select receiver, dt, hour
from `machine_learning_dev_ttl_120d.sat_offsets_v20210630_201807*`
"""
hourly_offsets = pd.read_gbq(query, project_id='world-fishing-827')


# +
def plot_hourly(df, rcvrs):
    n = len(rcvrs)
    figure = plt.figure(figsize=(18, 9), facecolor='white')
    width = timedelta(minutes=60 / (n + 1))
    for i, rcvr in enumerate(rcvrs):
        mask = (df.receiver == rcvr)
        offsets = df[mask].copy()
        offsets.sort_values(by='hour', inplace=True)
        times = offsets.hour + i * width
        plt.bar(times, offsets.dt, width=width, label=rcvr)
    plt.ylabel('seconds')
    plt.title('Satellite Clock Offset by Hour')
    plt.legend()
    
plot_hourly(hourly_offsets, rcvrs)
plt.ylim(-30, 90)


# +
def plot_rcvr(daily, hourly, rcvr):
    figure = plt.figure(figsize=(18, 9), facecolor='white', dpi=300)
    # Plot daily first, it's wide and will show up better underneath
    width = timedelta(minutes=18 * 60)
    mask = (daily.receiver == rcvr)
    offsets = daily[mask].copy()
    plt.bar(offsets.date, offsets.dt, width=width, label=rcvr)
    plt.ylabel('seconds')
    plt.title(f'Satellite Clock Offset For {rcvr}')
    # Now plot hourly on top, leave enough space that we can see hourly
    width = timedelta(minutes=30)
    mask = (hourly.receiver == rcvr)
    offsets = hourly[mask].copy()
    plt.bar(offsets.hour, offsets.dt, width=width, label=rcvr)
    plt.ylabel('seconds')
    plt.title(f'Satellite Clock Offset For {rcvr}')
    plt.hlines([-30, 30], offsets.hour.min(), offsets.hour.max(), linewidth=0.5, color='k')
    plt.ylim(-40, 90)
    
plot_rcvr(daily_offsets, hourly_offsets, rcvrs[0])

# -

plot_rcvr(daily_offsets, hourly_offsets, rcvrs[1])

plot_rcvr(daily_offsets, hourly_offsets, rcvrs[2])

plot_rcvr(daily_offsets, hourly_offsets, rcvrs[3])

plot_rcvr(daily_offsets, hourly_offsets, rcvrs[4])

dropped_mask_60 = (abs(hourly_offsets.dt) > 60)
dropped_mask_60[1:] |= dropped_mask_60[:-1]
dropped_mask_60[:-1] |= dropped_mask_60[1:]
# We also drop the two adjacent hours
dropped_mask_30 = (abs(hourly_offsets.dt) > 30)
dropped_mask_30[1:] |= dropped_mask_30[:-1]
dropped_mask_30[:-1] |= dropped_mask_30[1:]
dropped_60 = dropped_mask_60.mean()
dropped_30 = dropped_mask_30.mean()
print(f'{100 * dropped_60:.1f}% dropped with 60s threshold')
print(f'{100 * dropped_30:.1f}% dropped with 30s threshold')

# ### Are Messages Getting Dropped As they Should?

# +
dropped_mask = (abs(hourly_offsets.dt) > 30)
# We also drop the two adjacent hours
dropped_mask[1:] |= dropped_mask[:-1]
dropped_mask[:-1] |= dropped_mask[1:]

examples = hourly_offsets.hour[dropped_mask & 
                               (hourly_offsets.receiver == rcvrs[0]) &
                               (hourly_offsets.hour.dt.month == 7) &
                               (hourly_offsets.hour.dt.year == 2018)
                              ]

exstr = ', '.join(f'timestamp("{x}")' for x in examples)

q = f"""
select * 
from `machine_learning_dev_ttl_120d.messages_segmented_v20210630_201807*`
where receiver = "{rcvrs[0]}"
  and timestamp_trunc(timestamp, hour) in ({exstr})
limit 100
"""

should_be_dropped = pd.read_gbq(q, project_id='world-fishing-827')
should_be_dropped # should be empty()

# +
not_dropped_mask = ~dropped_mask

examples = hourly_offsets.hour[not_dropped_mask & 
                               (hourly_offsets.receiver == rcvrs[0]) &
                               (hourly_offsets.hour.dt.month == 7) &
                               (hourly_offsets.hour.dt.year == 2018)
                              ]

exstr = ', '.join(f'timestamp("{x}")' for x in examples)

q = f"""
select * 
from `machine_learning_dev_ttl_120d.messages_segmented_v20210630_201807*`
where receiver = "{rcvrs[0]}"
  and timestamp_trunc(timestamp, hour) in ({exstr})
"""

should_not_be_dropped = pd.read_gbq(q, project_id='world-fishing-827')
len(should_not_be_dropped), len(should_not_be_dropped) # Should be the same
# -

# ## Check that Null Value Code replacing Invalid Values with Null

# ### Type 1, 2, 3, 18 & 19

query = """

select 'base',  count(*) num_invalid
from `pipe_ais_sources_v20190222.normalized_spire_20180716` 
where type in ('AIS.1', 'AIS.2', 'AIS.3', 'AIS.18', 'AIS.19')
  and (lon = 181
    or lat = 91
    or speed = 1023
    or course = 360
    or heading = 511
  )
union all
select 'segmented',  count(*) num_invalid
from `machine_learning_dev_ttl_120d.messages_segmented_v20210630_20180716` 
where type in ('AIS.1', 'AIS.2', 'AIS.3', 'AIS.18', 'AIS.19')
  and (lon = 181
    or lat = 91
    or speed = 1023
    or course = 360
    or heading = 511
  )
"""
df = pd.read_gbq(query, project_id='world-fishing-827')
df.head()

# ## Type 27

query = """

select 'base',  count(*) num_invalid
from `pipe_ais_sources_v20190222.normalized_spire_20180716` 
where type in ('AIS.27')
  and (lon = 181
    or lat = 91
    or speed = 63
    or course = 511
  )
union all
select 'segmented',  count(*) num_invalid
from `machine_learning_dev_ttl_120d.messages_segmented_v20210630_20180716` 
where type in ('AIS.27')
  and (lon = 181
    or lat = 91
    or speed = 63
    or course = 511
  )
"""
df = pd.read_gbq(query, project_id='world-fishing-827')
df.head()

# This appears to remove all of the invalid values as expected.

query = """

select 'base',  sum(if(a.course = 360 and b.course is null, 1, 0)) replaced_courses, 
                sum(if(a.course = 360 and b.course is not null, 1, 0)) nonreplaced_courses
from `pipe_ais_sources_v20190222.normalized_spire_20180716` a
join  `machine_learning_dev_ttl_120d.messages_segmented_v20210630_20180716` b
using (msgid)
where a.type in ('AIS.1', 'AIS.2', 'AIS.3', 'AIS.18', 'AIS.19')

"""
df = pd.read_gbq(query, project_id='world-fishing-827')
df.head()

# Huh? Why is one non-replaced.

# Hmmm. this is related to duplicate message ids with different messages!

query = """

select *
from `pipe_ais_sources_v20190222.normalized_spire_20180716`
where msgid = '8ee2f3eb-0563-0a77-559d-b2e18b516a91'
"""
df = pd.read_gbq(query, project_id='world-fishing-827')
df.head()


