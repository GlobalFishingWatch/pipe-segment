"""Tests that the pipelines that run queries on BigQuery have in common."""

import pytest

from pipe_segment.pipelines.segment_info import main as segment_info
from pipe_segment.pipelines.segment_vessel import main as segment_vessel
from pipe_segment.pipelines.vessel_info import main as vessel_info
from . import query_helpers
from .test_segment_info import make_config as segment_info_config
from .test_segment_vessel import make_config as segment_vessel_config
from .test_vessel_info import make_config as vessel_info_config


PIPELINES = [
    pytest.param(segment_info, segment_info_config, ["seg_id"], id="segment-info"),
    pytest.param(segment_vessel, segment_vessel_config, None, id="segment-vessel"),
    pytest.param(vessel_info, vessel_info_config, None, id="vessel-info"),
]


@pytest.mark.parametrize("main, make_config, clustering_fields", PIPELINES)
@pytest.mark.parametrize("dry_run", [True, False])
def test_run_executes_the_query_in_dry_run_mode_if_asked(
    main, make_config, clustering_fields, dry_run
) -> None:
    """Checks that the dry run option is passed to the BigQuery job."""
    client = query_helpers.run_with_client(
        main, make_config(dry_run=dry_run), clustering_fields=clustering_fields
    )

    assert client.default_query_job_config.dry_run is dry_run
