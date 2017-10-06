from pipeline.options.actions import ReadFileAction

def setup(parser):
    """
    Setup global pipeline options available both on local and remote runs.

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    parser.add_argument(
        '--window_size',
        help='Window size in seconds. Message will be divided in to windows of this duration by '
             'message timestamp and each window is precessed independently.  Set to 0 for no '
             'windowing.',
        type=int,
        default=0,
    )

    parser.add_argument(
        '--segmenter_params',
        help='Pass a json object with parameters to pass to the segmenter, or supply a file name to read with '
             "@path/to/file.json.   For Example:"
             ''
             '{'
             '  "max_hours": 24,'
             '  "max_speed": 30,'
             '  "noise_dist": 0,'
             '  "reported_speed_multiplier": 1.1,'
             '  "max_speed_multiplier": 15,'
             '  "max_speed_exponent": 1.3,'
             '}',
        action=ReadFileAction,
    )

    required = parser.add_argument_group('global required arguments')
    required.add_argument(
        '--messages_source',
        help='source of message records to process.  This can be a file pattern matching one or more files,'
             'a bigquery table or a bigquery sql query.  '
             ''
             'Local file references should start with ".", "/" or "file://".  '
             ''
             'Bigquery tables should be specified as "bq://PROJECT:DATASET.TABLE".  '
             ''
             'Anything else will be treated as a sql statement. '
             ''
             'You can also prepend the option with "@" to load the content of this option from a file'
             'as in "@path/to/file.sql"',
        required=True,
        action=ReadFileAction,
    )

    required.add_argument(
        '--messages_sink',
        help='destination where the pipeline will write message records.  This can be a file pattern matching one or more files,'
             'or a bigquery table.  '
             ''
             'Local file references should start with ".", "/" or "file://".  '
             ''
             'Bigquery tables should be specified as "bq://PROJECT:DATASET.TABLE".  ',
        required=True
    )

    required.add_argument(
        '--segments_sink',
        help='destination where the pipeline will write segment records.  This can be a file pattern matching one or more files,'
             'or a bigquery table.  '
             ''
             'Local file references should start with ".", "/" or "file://".  '
             ''
             'Bigquery tables should be specified as "bq://PROJECT:DATASET.TABLE".  ',
        required=True
    )

    parser.add_argument(
        '--sink_write_disposition',
        help='How to merge the output of this process with whatever records are already there in the sink tables. '
             'Might be WRITE_TRUNCATE to remove all existing data and write the new data, or WRITE_APPEND to add '
             'the new date without. Defaults to WRITE_APPEND.',
        default='WRITE_APPEND',
    )
