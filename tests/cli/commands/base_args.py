"""Arguments of each command with only its required options, for the CLI tests."""


MESSAGES_SEGMENTED = [
    "messages-segmented",
    "--bq-in-normalized-messages",
    "project.dataset.messages",
    "--bq-out-segmented-messages",
    "project.dataset.messages_segmented",
    "--bq-out-segments",
    "project.dataset.segments",
    "--bq-out-fragments",
    "project.dataset.fragments",
    "--start-date",
    "2024-01-01",
    "--end-date",
    "2024-01-08",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


SEGMENT_IDENTITY_DAILY = [
    "segment-identity-daily",
    "--bq-in-segments",
    "project.dataset.segments",
    "--bq-in-fragments",
    "project.dataset.fragments",
    "--bq-out-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--start-date",
    "2024-01-01",
    "--end-date",
    "2024-01-08",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


SEGMENT_VESSEL_DAILY = [
    "segment-vessel-daily",
    "--bq-in-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--bq-out-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--start-date",
    "2024-01-01",
    "--end-date",
    "2024-01-08",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


SEGMENT_VESSEL = [
    "segment-vessel",
    "--bq-in-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--bq-out-segment-vessel",
    "project.dataset.segment_vessel",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


SEGMENT_INFO = [
    "segment-info",
    "--bq-in-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--bq-in-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--bq-out-segment-info",
    "project.dataset.segment_info",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]


VESSEL_INFO = [
    "vessel-info",
    "--bq-in-segment-identity-daily",
    "project.dataset.segment_identity_daily",
    "--bq-in-segment-vessel-daily",
    "project.dataset.segment_vessel_daily",
    "--bq-out-vessel-info",
    "project.dataset.vessel_info",
    "--project",
    "test-project",
    "--labels",
    "environment=test",
]
