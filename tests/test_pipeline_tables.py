from datetime import date

import pytest

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_segment import pipeline as pipeline_module
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


TABLE = "project.dataset.{}"
START = date(2024, 1, 2)
END = date(2024, 1, 4)


def make_config(**kwargs):
    defaults = {
        "project": "project",
        "bq_in_normalized_messages": TABLE.format("messages"),
        "bq_out_segmented_messages": TABLE.format("messages_segmented"),
        "bq_out_segments": TABLE.format("segments"),
        "bq_out_fragments": TABLE.format("fragments"),
        "start_date": START,
        "end_date": END,
        "labels": {"a": "b"},
    }
    return MessagesSegmentedConfig(**{**defaults, **kwargs})


def make_pipeline(mocker, **kwargs):
    mocker.patch.object(pipeline_module.bigquery, "Client")
    beam_options = PipelineOptions(["--project", "project"])
    return pipeline_module.SegmentPipeline(make_config(**kwargs), beam_options)


def test_the_input_tables_default_to_the_output_tables(mocker):
    pipe = make_pipeline(mocker)

    assert pipe.in_segments_table == TABLE.format("segments")
    assert pipe.in_fragments_table == TABLE.format("fragments")


def test_the_input_tables_can_be_read_from_other_tables(mocker):
    pipe = make_pipeline(
        mocker,
        bq_in_segments=TABLE.format("in_segments"),
        bq_in_fragments=TABLE.format("in_fragments"),
    )

    assert pipe.in_segments_table == TABLE.format("in_segments")
    assert pipe.in_fragments_table == TABLE.format("in_fragments")
    assert pipe.fragments_output_table.table_id == TABLE.format("fragments")
    assert pipe.segments_output_table.table_id == TABLE.format("segments")


def test_the_last_day_is_the_one_before_the_end_date(mocker):
    pipe = make_pipeline(mocker)

    assert pipe.last_day == date(2024, 1, 3)


def test_the_messages_are_read_from_all_the_sources(mocker):
    pipe = make_pipeline(mocker, bq_in_normalized_messages="p.d.a,p.d.b")

    assert pipe.source_tables == ["p.d.a", "p.d.b"]


def test_prepare_output_tables_ensures_runs_the_checks_and_clears_last(mocker):
    pipe = make_pipeline(mocker)
    calls = mocker.Mock()
    mocker.patch.object(pipe, "ensure_output_tables", calls.ensure)
    mocker.patch.object(pipe, "precondition_hooks", return_value=[calls.check])
    mocker.patch.object(pipe, "clear_output_tables", calls.clear)

    pipe.prepare_output_tables(START, pipe.last_day)

    assert calls.mock_calls == [
        mocker.call.ensure(),
        mocker.call.check(pipe),
        mocker.call.clear(START, pipe.last_day),
    ]


def test_prepare_output_tables_does_not_clear_when_a_check_fails(mocker):
    pipe = make_pipeline(mocker)
    failing_check = mocker.Mock(side_effect=ValueError("no prior segments"))
    mocker.patch.object(pipe, "ensure_output_tables")
    mocker.patch.object(pipe, "precondition_hooks", return_value=[failing_check])
    clear = mocker.patch.object(pipe, "clear_output_tables")

    with pytest.raises(ValueError, match="no prior segments"):
        pipe.prepare_output_tables(START, pipe.last_day)

    clear.assert_not_called()


def test_no_checks_run_without_incremental(mocker):
    pipe = make_pipeline(mocker, incremental=False)

    assert pipe.precondition_hooks(START, pipe.last_day) == []


def test_incremental_runs_the_messages_and_prior_segments_checks(mocker):
    pipe = make_pipeline(mocker, incremental=True)

    hooks = pipe.precondition_hooks(START, pipe.last_day)

    assert len(hooks) == 2


def test_the_output_tables_are_cleared_without_the_end_date(mocker):
    pipe = make_pipeline(mocker)
    client = pipe.bq_helper.client

    pipe.clear_output_tables(START, pipe.last_day)

    clears = [call.args[0] for call in client.query_and_wait.call_args_list]
    assert len(clears) == 3
    for clear in clears:
        assert ">= '2024-01-02'" in clear
        assert "<= '2024-01-03'" in clear


def test_the_satellite_offsets_table_is_cleared_without_the_end_date(mocker):
    pipe = make_pipeline(mocker, bq_out_sat_offsets=TABLE.format("sat_offsets"))

    pipe.clear_output_tables(START, pipe.last_day)

    # The pipeline and the satellite offsets share the mocked BigQuery client class.
    client = pipeline_module.bigquery.Client.return_value
    clear = client.query_and_wait.call_args.args[0]
    assert "DELETE FROM `project.dataset.sat_offsets`" in clear
    assert "<= '2024-01-03'" in clear
