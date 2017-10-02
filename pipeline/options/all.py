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

