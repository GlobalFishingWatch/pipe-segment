"""Tests for the entry point of the messages-segmented pipeline."""

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from pytest_mock import MockerFixture

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.bigquery.table_description import TO_BE_COMPLETED
from pipe_segment.pipelines.messages_segmented import main
from pipe_segment.pipelines.messages_segmented.config import MessagesSegmentedConfig


def make_config(**kwargs: Any) -> MessagesSegmentedConfig:
    """Returns a config with the required fields."""
    defaults = {
        "project": "test-project",
        "bq_in_normalized_messages": "project.dataset.messages",
        "bq_out_segmented_messages": "project.dataset.messages_segmented",
        "bq_out_segments": "project.dataset.segments",
        "bq_out_fragments": "project.dataset.fragments",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 3),
        "labels": {"environment": "test"},
        "mock_bq_clients": True,
    }
    return MessagesSegmentedConfig(**{**defaults, **kwargs})


SAT_OFFSETS = {
    "bq_in_sat_offset_messages": "project.dataset.sat_messages",
    "bq_in_norad_to_receiver": "project.dataset.norad",
    "bq_in_sat_positions": "project.dataset.sat_positions",
    "bq_out_sat_offsets": "project.dataset.sat_offsets",
}


def run_main(
    mocker: MockerFixture, config: MessagesSegmentedConfig, **factories: Any
) -> MagicMock:
    """Runs the pipeline with a mocked Pipeline, and returns the mock of its class."""
    pipeline_class = mocker.patch.object(main, "Pipeline")
    main.run(config, **factories)
    return pipeline_class


def written_tables(mocker: MockerFixture, config: MessagesSegmentedConfig) -> list[dict[str, Any]]:
    """Returns the arguments of the sinks that the pipeline builds, in order."""
    writes: list[dict[str, Any]] = []

    def write_factory(**kwargs: Any) -> MagicMock:
        writes.append(kwargs)
        return MagicMock()

    run_main(mocker, config, write_to_bigquery_factory=write_factory)
    return writes


def test_the_tables_are_the_three_outputs(mocker: MockerFixture) -> None:
    """Checks that the sinks write the messages, the segments and the fragments."""
    writes = written_tables(mocker, make_config())

    assert [write["table"] for write in writes] == [
        "project.dataset.messages_segmented",
        "project.dataset.segments",
        "project.dataset.fragments",
    ]


def test_a_sink_appends_to_an_existing_table_with_its_schema(mocker: MockerFixture) -> None:
    """Checks that each sink writes the schema of its table, never creating it."""
    writes = written_tables(mocker, make_config())

    for write, fields in zip(writes, [22, 11, 16]):
        assert len(write["schema"]["fields"]) == fields
        assert write["create_disposition"] == "CREATE_NEVER"


def test_the_satellite_offsets_are_a_table_if_they_are_written(mocker: MockerFixture) -> None:
    """Checks that the offsets table is the last one, and only when it is given."""
    writes = written_tables(mocker, make_config(**SAT_OFFSETS))

    assert len(writes) == 4
    assert writes[-1]["table"] == "project.dataset.sat_offsets"
    assert len(writes[-1]["schema"]["fields"]) == 6


def test_the_descriptions_have_caveats_and_the_parameters(mocker: MockerFixture) -> None:
    """Checks that each table has real caveats, and the parameters that built it."""
    create_table_hook = mocker.patch.object(main, "create_table_hook")

    run_main(mocker, make_config(**SAT_OFFSETS))

    tables = [call.args[0] for call in create_table_hook.call_args_list]
    assert len(tables) == 4
    for table in tables:
        assert table.description.caveats != TO_BE_COMPLETED
        assert table.description.relevant_params["start_date"] == date(2024, 1, 1)
        assert table.description.version


def run_hooks(
    mocker: MockerFixture, config: MessagesSegmentedConfig
) -> list[tuple[str, tuple, dict]]:
    """Runs the pre-hooks on mocked clients, and returns the calls that they made, in order."""
    mocked_factory = BigQueryHelper.get_client_factory(mocked=True)
    calls: list[tuple[str, tuple, dict]] = []

    def recorder(name: str) -> Any:
        def record(*args: Any, **kwargs: Any) -> MagicMock:
            calls.append((name, args, kwargs))
            return MagicMock()

        return record

    def bq_client_factory(**kwargs: Any) -> MagicMock:
        client = mocked_factory(**kwargs)
        client.create_table.side_effect = recorder("create_table")
        client.query.side_effect = recorder("query")
        return client

    pipeline_class = run_main(mocker, config, bq_client_factory=bq_client_factory)

    pipeline = Pipeline(project=config.project, labels=config.labels)
    for hook in pipeline_class.call_args.kwargs["pre_hooks"]:
        hook(pipeline)

    return calls


def test_the_tables_are_created_before_the_range_is_deleted(mocker: MockerFixture) -> None:
    """Checks that every table is created first, and then its range is deleted."""
    calls = run_hooks(mocker, make_config())

    assert [name for name, _, _ in calls] == ["create_table"] * 3 + ["query"] * 3


def test_the_range_is_deleted_without_the_end_date(mocker: MockerFixture) -> None:
    """Checks that each table's range is deleted before the end date, with the labels."""
    calls = run_hooks(mocker, make_config(**SAT_OFFSETS))

    deletes = [(args, kwargs) for name, args, kwargs in calls if name == "query"]
    assert len(deletes) == 4
    for args, kwargs in deletes:
        assert "< '2024-01-03'" in args[0]
        assert kwargs["job_config"].labels == {"environment": "test"}


def test_the_checks_run_only_in_an_incremental_run_and_before_the_delete(
    mocker: MockerFixture,
) -> None:
    """Checks that the incremental checks are between the creation and the deletes."""
    verify_messages = mocker.patch.object(main, "verify_messages_exist_hook")
    verify_segments = mocker.patch.object(main, "verify_prior_segments_hook")

    pipeline_class = run_main(mocker, make_config(incremental=True))

    hooks = pipeline_class.call_args.kwargs["pre_hooks"]
    assert hooks[3] is verify_messages.return_value
    assert hooks[4] is verify_segments.return_value
    assert len(hooks) == 3 + 2 + 3
    assert verify_messages.call_args.args[:3] == (
        "project.dataset.messages",
        date(2024, 1, 1),
        date(2024, 1, 2),
    )
    assert verify_segments.call_args.args == (
        "project.dataset.segments",
        "project.dataset.fragments",
        date(2023, 12, 31),
    )


def test_a_full_history_run_has_no_checks(mocker: MockerFixture) -> None:
    """Checks that only an incremental run is checked."""
    verify_messages = mocker.patch.object(main, "verify_messages_exist_hook")

    pipeline_class = run_main(mocker, make_config())

    assert len(pipeline_class.call_args.kwargs["pre_hooks"]) == 3 + 3
    verify_messages.assert_not_called()


def test_run_resolves_the_beam_options_from_the_config(mocker: MockerFixture) -> None:
    """Checks that the project, the labels and the other Beam options reach the pipeline."""
    config = make_config(
        unknown_unparsed_args=("--max_num_workers=7",),
        unknown_parsed_args={"region": "us-central1"},
    )

    pipeline_class = run_main(mocker, config)

    kwargs = pipeline_class.call_args.kwargs
    assert kwargs["project"] == "test-project"
    assert kwargs["region"] == "us-central1"
    assert kwargs["labels"] == {"environment": "test"}
    assert kwargs["unparsed_args"] == ("--max_num_workers=7",)
    pipeline_class.return_value.run.assert_called_once()


@pytest.mark.parametrize("wait_for_job", [True, False])
def test_run_passes_wait_for_job_to_the_pipeline(
    mocker: MockerFixture, wait_for_job: bool
) -> None:
    """Checks that the pipeline waits as the config says, with no argument to run."""
    pipeline_class = run_main(mocker, make_config(wait_for_job=wait_for_job))

    assert pipeline_class.call_args.kwargs["wait_for_job"] is wait_for_job
    pipeline_class.return_value.run.assert_called_once_with()
