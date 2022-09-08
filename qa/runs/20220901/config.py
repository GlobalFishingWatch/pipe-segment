DATASET_OLD = 'world-fishing-827.pipe_production_v20201001'
DATASET_NEW_MONTHLY_INT = 'pipe_ais_test_20220901_monthly_internal'
DATASET_NEW_BACKFILL_INT = 'pipe_ais_test_20220901_backfill_internal'
DATASET_NEW_MONTHLY_PUB = 'pipe_ais_test_20220901_monthly_published'
DATASET_NEW_BACKFILL_PUB = 'pipe_ais_test_20220901_backfill_published'

SAT_OFFSETS_TABLE = 'sat_time_offsets_'
MESSAGES_SEGMENTED_TABLE = 'messages_segmented_'
FRAGMENTS_TABLE = 'fragments_'
SEGMENTS_TABLE = 'segments_'
SEGMENT_IDENTITY_DAILY_TABLE = 'segment_identity_daily_'
SEGMENT_VESSEL_DAILY_TABLE = 'segment_vessel_daily_'
SEGMENT_INFO_TABLE = 'segment_info'
SEGMENT_VESSEL_TABLE = 'segment_vessel'
VESSEL_INFO_TABLE = 'vessel_info'

# Threshold used for the difference between the
# number of segments in a given day compared to
# the rolling 30-day average.
# Used in `segmenter_version_comparison.py`.
NUM_SEGS_DIFF_THRESHOLD = 0.2