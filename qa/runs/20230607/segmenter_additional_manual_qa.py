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
#     name: rad
# ---

# %% [markdown]
# ## Additional Manual QA
# Author: Jenn Van Osdel  
# Date: June 2023
# 
# This notebook is additional exploration that I did after running `segmenter_version_comparison.py`. I have not cleaned it up but am persisting here for reference as some of the figures were used in Jira tickets and will potentially be used in the documentation for changes in pipe 3.

# %%
import pandas as pd
import matplotlib.pyplot as plt

# %%
file_loc = "data/daily_stats_segment_identity_daily.csv"
df_daily_stats = pd.read_csv(file_loc)
df_daily_stats

# %%


# %%
def plot_diff(df, col_prefix, title, ylabel=""):
    fig = plt.figure()
    ax = df[[f'{col_prefix}diff']].plot(c='green', label='diff')
    years = list(df.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    ax.legend(["Pipe 3 - Pipe 2.5"])
    plt.title(title)
    plt.ylabel(ylabel)
        
    return fig, ax

# %%
plot_diff(df_daily_stats, col_prefix='num_segs_', 
          title="Difference in number of active_segments per day\nsegment_identity_daily_",
          ylabel="num_segs")


# %%
df_daily_stats[(df_daily_stats.year == 2018) & (df_daily_stats.num_segs_diff > 100)][["date", "num_segs_new", "num_segs_old", "num_segs_diff"]]

# %% [markdown]
# # OLD RESULTS

# %%
file_loc_old = "../20230329/data/daily_stats_segment_identity_daily.csv"
df_daily_stats_old = pd.read_csv(file_loc_old)
df_daily_stats_old

# %%
plot_diff(df_daily_stats_old, col_prefix='total_pos_count_', 
          title="Difference in total position message count per day\nsegment_identity_daily_",
          ylabel="total_pos_count_")


# %%
df_daily_stats_old[df_daily_stats_old.total_pos_count_diff < -10000][["date", "total_pos_count_new", "total_pos_count_old", "total_pos_count_diff"]]

# %%
df_daily_stats_old.sort_values("total_pos_count_diff")[["date", "total_pos_count_new", "total_pos_count_old", "total_pos_count_diff"]]

# %%


# %% [markdown]
# # Number of segments with at least one valid shipname

# %%
def plot_diff_pipe3(df_prev, df_curr, col, title, ylabel=""):
    '''
    Plots df_curr - df_prev for col. Assumes df1 and df2 
    have same length and date ranges.
    '''
    fig = plt.figure()
    ax = (df_curr[col] - df_prev[col]).plot(c='green', label='diff')
    years = list(df_curr.year.sort_values().unique())
    ax.set_xticks([t*365 for t in range(len(years))])
    ax.set_xticklabels(years)
    fig.patch.set_facecolor('white')
    ax.legend(["Pipe 3: current - previous"])
    plt.title(title)
    plt.ylabel(ylabel)
        
    return fig, ax

# %% [markdown]
# ### Number of segments in each pipe 3 run

# %%
plot_diff_pipe3(df_daily_stats_old, df_daily_stats, 'num_segs_new', 'Number of segments')

# %%
plot_diff_pipe3(df_daily_stats_old, df_daily_stats, 'sum_seg_length_h_new', 'Total length of segments')

# %%
diff = df_daily_stats[["date"]].copy()
diff["num_segs_diff"] = df_daily_stats.num_segs_new - df_daily_stats_old.num_segs_new
diff[diff.num_segs_diff != 0]

# %% [markdown]
# ### Number of segments with at least one valid shipname

# %%
df_daily_stats_old.num_segs_with_idents_new.plot()
df_daily_stats.num_segs_with_idents_new.plot()


# %%
plot_diff_pipe3(df_daily_stats_old, df_daily_stats, 'num_segs_with_idents_new', 'Number of segments with at least one valid shipname')

# %%
df_daily_stats[df_daily_stats.date == '2018-01-01'].num_segs_with_idents_new

# %%
df_daily_stats_old[df_daily_stats_old.date == '2018-01-01'].num_segs_with_idents_new

# %% [markdown]
# ## Average number of shipnames per segment by day

# %%
plot_diff_pipe3(df_daily_stats_old, df_daily_stats, 'avg_num_idents_new', 'Average number of shipnames per segment')

# %%
df_daily_stats[df_daily_stats.avg_num_idents_diff < -0.6][:40][['date', 'avg_num_idents_new', 'avg_num_idents_old', 'avg_num_idents_diff']]

# %%
data = df_daily_stats[(df_daily_stats.date >= '2018-04-14') & (df_daily_stats.date <= '2018-04-17')]
for col in df_daily_stats.columns:
    print(f"{col} --> {data.iloc[0][col]} -- {data.iloc[1][col]} -- {data.iloc[2][col]}  -- {data.iloc[3][col]}")


# %% [markdown]
# Starts on 20230416
# 
# max_num_idents higher as well
# 


