import os
import pytest

from pipe_segment.cli import cli
from pipe_segment import pipeline


def test_cli(monkeypatch, tmp_path):

    # TODO: replace this monkey patch when design allows for more easy testing.
    monkeypatch.setattr(pipeline, "run", lambda *x, **y: 0)

    log_file = os.path.join(tmp_path, 'segment.log')
    dummy_table = 'dummy_table'

    cli.run([
        '-v',
        '--log_file', log_file,
        'segment',
        '--source', dummy_table,
        '--msg_dest', dummy_table,
        '--fragment_tbl', dummy_table,
        '--segment_dest', dummy_table
    ])

    assert os.path.exists(log_file)

    with pytest.raises(SystemExit):
        cli.run([])
