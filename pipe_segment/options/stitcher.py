import ujson as json

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import ReadFileAction

class StitcherOptions(PipelineOptions):
    DEFAULT_TEMP_SHARDS_PER_DAY=16

    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument(
            '--seg_source',
            required=True,
            action=ReadFileAction,
            help='Bigquery table, query or file to read segments')
        required.add_argument(
            '--track_dest',
            required=True,
            help='Bigquery table or file (prefix) to read and write tracks')
        optional.add_argument(
            '--temp_shards_per_day',
            type=int,
            help='Number of shards to write per day in messages output temporary storage. '
                 'A good value for this is the max number of workers.  Default %s'
                 % cls.DEFAULT_TEMP_SHARDS_PER_DAY)
        optional.add_argument(
            '--wait_for_job',
            default=False,
            action='store_true',
            help='Wait until the job finishes before returning.')
        optional.add_argument(
            '--date_range',
            help='Range of dates to read from source. format YYYY-MM-DD,YYYY-MM-DD')
        optional.add_argument(
            '--look_ahead', default=7, type=int,
            help="Maximum number of days to look_ahead when stitching")
        optional.add_argument(
            '--look_back', default=7, type=int,
            help="Maximum number of days to look_back when stitching. This is needed because"
                 "not all daily info is currently available in segments so we have to compute"
                 "some of it by differencing")
        optional.add_argument(
            '--min_secondary_track_count', default=1000, type=int,
            help='Minimum number of points for a secondary track to be considered not-noise')
        optional.add_argument(
            '--ssvid_to_skip', default='', 
            help='comma-separated list of ssvid to skip'
            )
        optional.add_argument(
            '--stitcher_params',
            help='Pass a json object with parameters to pass to the stitcher, or supply a file name to read from '
                 "@path/to/file.json.   For Example:"
                 ''
                 '{'
                 '  "buffer_hours": 1.0,'
                 '  "max_overlap_factor": 0.8,'
                 '}',
            default="{}",
            action=ReadFileAction,
        )
