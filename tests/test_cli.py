import os
import pytest

from pipe_segment.cli import cli
from pipe_segment import pipeline
from pipe_segment.segment_identity import pipeline as identity_pipeline


def test_cli(monkeypatch, tmp_path):

    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(pipeline, "run", lambda *x, **y: 0)

    log_file = os.path.join(tmp_path, 'segment.log')
    dummy_table = 'dummy_proj:dummy_dataset.dummy_table'
    dummy_table_short = 'dummy_dataset.dummy_table'

    cli.run([
        '-v',
        '--log_file', log_file,
        'segment',
        '--in_normalized_messages_table', dummy_table,
        '--out_segmented_messages_table', dummy_table_short,
        '--fragments_table', dummy_table_short,
        '--out_segments_table', dummy_table_short
    ])

    assert os.path.exists(log_file)

    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(identity_pipeline, "run", lambda *x, **y: 0)
    cli.run([
        '-v',
        '--log_file', log_file,
        'segment_identity',
        '--source_segments', dummy_table,
        '--source_fragments', dummy_table,
        '--dest_segment_identity', dummy_table,
    ])

    assert os.path.exists(log_file)

    with pytest.raises(SystemExit):
        cli.run([])
