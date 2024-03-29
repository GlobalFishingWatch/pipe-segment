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
            help='Bigquery table to read normalized messages')
        required.add_argument(
            '--msg_dest',
            required=True,
            help='Bigquery table to write segmented messages')
        required.add_argument(
            '--seg_dest',
            required=True,
            help='Bigquery table to read and write new (v2) segments')
        required.add_argument(
            '--legacy_seg_v1_dest',
            required=False,
            help='Bigquery table to read and write old (v1) segments')

        optional.add_argument(
            '--source_schema',
            help='JSON schema for the source messages (bigquery).  This is ignored for tables or file sources. '
                 'See examples/message-schema.json for an example.  This must match the fields included in the '
                 'query or bq table.   You can use "@path/to/file.json" to load this from a file.',
            action=ReadFileAction)
        optional.add_argument(
            '--date_range',
            help='Range of dates to read from source. format YYYY-MM-DD,YYYY-MM-DD')
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
            '--look_ahead',
            type=int,
            default=0,
            help='How many days to look ahead when performing segmenting 1 or 2 are good choices.')
        optional.add_argument(
            '--pipeline_start_date',
            help='Firs day of the pipeline data, used to know if we want to exclude the check of pading one day before YYYY-MM-DD')
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
            action=ReadFileAction)
        optional.add_argument(
            '--ssvid_filter_query',
            help='query that returns a list of ssvid to trim the sourced data down to. Note that '
                 'the resturned list is used in memory so should not be too large. This meant for '
                 'testing purposes and if tempted to use for production, more work should be done '
                 'so that the data is pruned on the way in.'
            )

