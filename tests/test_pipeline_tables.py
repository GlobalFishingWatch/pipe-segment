from argparse import Namespace
from datetime import date

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
        "incremental": False,
        "in_normalized_messages_table": "project.dataset.messages",
        "ssvid_filter_query": None,
    }
    args = Namespace(**{**defaults, **options})
    beam_options = PipelineOptions(["--project", "project", "--labels", "a=b"])
    return pipeline_module.SegmentPipeline(args, beam_options)


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


def test_prepare_output_tables_ensures_runs_the_checks_and_clears_last(mocker):
    pipe = make_pipeline(mocker)
    calls = mocker.Mock()
    mocker.patch.object(pipe, "ensure_output_tables", calls.ensure)
    mocker.patch.object(pipe, "precondition_hooks", return_value=[calls.check])
    mocker.patch.object(pipe, "clear_output_tables", calls.clear)
    start, end = date(2024, 1, 2), date(2024, 1, 3)

    pipe.prepare_output_tables(start, end)

    assert calls.mock_calls == [
        mocker.call.ensure(),
        mocker.call.check(pipe),
        mocker.call.clear(start, end),
    ]


def test_prepare_output_tables_does_not_clear_when_a_check_fails(mocker):
    pipe = make_pipeline(mocker)
    failing_check = mocker.Mock(side_effect=ValueError("no prior segments"))
    mocker.patch.object(pipe, "ensure_output_tables")
    mocker.patch.object(pipe, "precondition_hooks", return_value=[failing_check])
    clear = mocker.patch.object(pipe, "clear_output_tables")

    with pytest.raises(ValueError, match="no prior segments"):
        pipe.prepare_output_tables(date(2024, 1, 2), date(2024, 1, 3))

    clear.assert_not_called()


def test_no_checks_run_without_incremental(mocker):
    pipe = make_pipeline(mocker, incremental=False)

    assert pipe.precondition_hooks(date(2024, 1, 2), date(2024, 1, 3)) == []


def test_incremental_runs_the_messages_and_prior_segments_checks(mocker):
    pipe = make_pipeline(mocker, incremental=True, in_normalized_messages_table="p.d.messages")

    hooks = pipe.precondition_hooks(date(2024, 1, 2), date(2024, 1, 3))

    assert len(hooks) == 2
