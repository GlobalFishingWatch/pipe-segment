import ujson as json

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import ReadFileAction

class SegmentOptions(PipelineOptions):
    DEFAULT_TEMP_SHARDS_PER_DAY=16

    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        required.add_argument(
            '--source',
            required=True,
            action=ReadFileAction,
            help='Bigquery table, query or file to read normalized messages')
        optional.add_argument(
            '--source_schema',
            help='JSON schema for the source messages (bigquery).  This is ignored for tables or file sources. '
                 'See examples/message-schema.json for an example.  This must match the fields included in the '
                 'query or bq table.   You can use "@path/to/file.json" to load this from a file.',
            action=ReadFileAction,
        )
        optional.add_argument(
            '--date_range',
            help='Range of dates to read from source. format YYYY-MM-DD,YYYY-MM-DD')
        required.add_argument(
            '--dest',
            required=True,
            help='Bigquery table or file (prefix) to write segmented messages')
        required.add_argument(
            '--segments',
            required=True,
            help='Bigquery table or file (prefix) to read and write segments')
        optional.add_argument(
            '--temp_shards_per_day',
            type=int,
            help='Number of shards to write per day in messages output temporary storage. '
                 'A good value for this is the max number of workers.  Default %s'
                 % cls.DEFAULT_TEMP_SHARDS_PER_DAY)
        optional.add_argument(
            '--wait',
            default=False,
            action='store_true',
            help='Wait until the job finishes before returning.')
        optional.add_argument(
            '--lookahead',
            type=int,
            default=0,
            help='How many days to look ahead when performing segmenting 1 or 2 are good choices.'
            )
        optional.add_argument(
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
            default="{}",
            action=ReadFileAction,
        )
