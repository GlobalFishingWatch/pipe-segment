import os
import pytest

from pipe_segment.cli import main
from pipe_segment import pipeline


def test_cli_incremental_flag(monkeypatch):
    options = {}
    monkeypatch.setattr(pipeline, "run", lambda args, extra_args: options.update(vars(args)))
    dummy_table = 'dummy_proj.dummy_dataset.dummy_table'
    required = [
        'segment',
        '--in_normalized_messages_table', dummy_table,
        '--out_segmented_messages_table', dummy_table,
        '--fragments_table', dummy_table,
        '--out_segments_table', dummy_table,
    ]

    main.run(required)
    assert options["incremental"] is False

    assert options["in_segments_table"] is None
    assert options["in_fragments_table"] is None

    main.run([
        *required,
        '--incremental',
        '--in_segments_table', dummy_table,
        '--in_fragments_table', dummy_table,
    ])
    assert options["incremental"] is True
    assert options["in_segments_table"] == dummy_table
    assert options["in_fragments_table"] == dummy_table


def test_cli(monkeypatch, tmp_path):

    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(pipeline, "run", lambda *x, **y: 0)

    log_file = os.path.join(tmp_path, 'segment.log')
    dummy_table = 'dummy_proj.dummy_dataset.dummy_table'

    main.run([
        '-v',
        '--log_file', log_file,
        'segment',
        '--in_normalized_messages_table', dummy_table,
        '--out_segmented_messages_table', dummy_table,
        '--fragments_table', dummy_table,
        '--out_segments_table', dummy_table
    ])

    assert os.path.exists(log_file)

    with pytest.raises(SystemExit):
        main.run([])
