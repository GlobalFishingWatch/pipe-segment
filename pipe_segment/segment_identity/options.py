from apache_beam.options.pipeline_options import PipelineOptions

# from pipe_tools.options import ReadFileAction


class SegmentIdentityOptions(PipelineOptions):
    DEFAULT_TEMP_SHARDS_PER_DAY = 16

    @classmethod
    def _add_argparse_args(cls, parser):
        required = parser.add_argument_group("Required")
        optional = parser.add_argument_group("Optional")

        required.add_argument(
            "--source_segments",
            required=True,
            # action=ReadFileAction,
            help="Bigquery table, query or file to read segments from",
        )

        required.add_argument(
            "--source_fragments",
            required=True,
            # action=ReadFileAction,
            help="Bigquery table, query or file to read fragments from",
        )

        required.add_argument(
            "--dest_segment_identity",
            required=True,
            help="Bigquery table or file (prefix) to write daily segment identity records",
        )

        optional.add_argument(
            "--date_range",
            help="Range of dates to read from source. format YYYY-MM-DD,YYYY-MM-DD",
        )

        optional.add_argument(
            "--wait_for_job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )

        optional.add_argument(
            "--temp_shards_per_day",
            type=int,
            help="Number of shards to write per day in output temporary storage. "
            "A good value for this is the max number of workers.  Default %s"
            % cls.DEFAULT_TEMP_SHARDS_PER_DAY,
        )
