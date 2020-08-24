import ujson as json

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import ReadFileAction

class ThinnerOptions(PipelineOptions):
    DEFAULT_TEMP_SHARDS_PER_DAY=16

    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument(
            '--msg_source',
            required=True,
            action=ReadFileAction,
            help='Bigquery table to read message from')
        required.add_argument(
            '--track_source',
            required=True,
            action=ReadFileAction,
            help='Bigquery table to read tracks from')
        required.add_argument(
            '--msg_sink',
            required=True,
            help='Bigquery table to write tracks to')
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

