from pipeline.options.actions import ReadFileAction


def setup(parser):
    """
    Setup arguments parsed only on local test runs

    Arguments:
        parser -- argparse.ArgumentParser instance to setup
    """

    parser.add_argument(
        '--project',
        help='Project on which the source bigquey queries are run.',
    )

    # source = parser.add_mutually_exclusive_group()
    # source.add_argument(
    #     '--sourcequery',
    #     help="This is a query to runa against BigQuery that provides the source of records to process. "
    #          "or a file containing a query if using the `@path/to/file.sql syntax`. "
    #          "See examples/local.sql.",
    #     action=ReadFileAction,
    # )
    # source.add_argument(
    #     '--sourcefile',
    #     help="source of records to process.  This can be a file pattern matching one or more files "
    #          "in the local file system or in gcs",
    # )

    # required = parser.add_argument_group('local required arguments')
    # required.add_argument(
    #     '--sink',
    #     help='output file path prefix: The file path to write to. The files written will begin'
    #          ' with this prefix, followed by a shard identifier',
    #     required=True,
    # )
    # required.add_argument(
    #     '--segments_sink',
    #     help='output file path prefix: The file path to write segments. The files written will begin'
    #          ' with this prefix, followed by a shard identifier',
    #     required=True,
    # )
    #

