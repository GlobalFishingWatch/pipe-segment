from argparse import Namespace

import pytest

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_segment import pipeline as pipeline_module


TABLE = "project.dataset.{}"


def make_pipeline(mocker, **options):
    mocker.patch.object(pipeline_module.bigquery, "Client")
    defaults = {
        "date_range": "2024-01-02,2024-01-03",
        "fragments_table": TABLE.format("fragments"),
        "out_fragments_table": None,
        "out_segments_table": TABLE.format("segments"),
        "in_segments_table": None,
        "in_fragments_table": None,
    }
    args = Namespace(**{**defaults, **options})
    return pipeline_module.SegmentPipeline(args, PipelineOptions(["--project", "project", "--labels", "a=b"]))


def test_prior_tables_default_to_the_output_tables(mocker):
    pipe = make_pipeline(mocker, out_fragments_table=TABLE.format("out_fragments"))

    assert pipe.prior_segments_table == TABLE.format("segments")
    assert pipe.prior_fragments_table == TABLE.format("out_fragments")


def test_prior_fragments_fall_back_to_fragments_table(mocker):
    pipe = make_pipeline(mocker)

    assert pipe.prior_fragments_table == TABLE.format("fragments")


def test_prior_tables_can_be_read_from_other_tables(mocker):
    pipe = make_pipeline(
        mocker,
        out_fragments_table=TABLE.format("out_fragments"),
        in_segments_table=TABLE.format("in_segments"),
        in_fragments_table=TABLE.format("in_fragments"),
    )

    assert pipe.prior_segments_table == TABLE.format("in_segments")
    assert pipe.prior_fragments_table == TABLE.format("in_fragments")
