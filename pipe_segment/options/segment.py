from apache_beam.options.pipeline_options import PipelineOptions

# from pipe_tools.options import ReadFileAction


class SegmentOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group("Required")
        optional = parser.add_argument_group("Optional")

        required.add_argument(
            "--source", required=True, help="Bigquery table to read normalized messages"
        )
        optional.add_argument(
            "--sat_source",
            required=False,
            help="Bigquery table, query or file to read normalized messages,"
            "must be a subset of `source`",
        )
        optional.add_argument(
            "--norad_to_receiver_tbl",
            required=False,
            help="Bigquery table that links NORAD IDs and receivers",
        )
        optional.add_argument(
            "--sat_positions_tbl",
            required=False,
            help="Bigquery table with the distance to satellite by receiver"
            "at a one second resolution",
        )
        optional.add_argument(
            "--sat_offset_dest",
            required=False,
            help="Bigquery table to write satellite offsets to.`",
        )
        required.add_argument(
            "--msg_dest",
            required=True,
            help="Bigquery table to write segmented messages",
        )
        required.add_argument(
            "--fragment_tbl",
            required=True,
            help="Bigquery table to read and write fragments",
        )
        required.add_argument(
            "--segment_dest",
            required=True,
            help="Bigquery table to write segments-days",
        )
        optional.add_argument(
            "--bad_hour_padding",
            default=1,
            help="hours on either side of an hour with bad satellite timing to suppress",
        )
        optional.add_argument(
            "--max_timing_offset_s",
            type=int,
            default=30,
            help="max. number of seconds a satellite clock can be off before we drop its messages",
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
            "--segmenter_params",
            help="JSON object with fragmenter parameters, or filepath "
            "@path/to/file.json.   For Example:"
            ""
            "{"
            '  "max_hours": 24,'
            '  "max_speed": 30,'
            '  "noise_dist": 0,'
            '  "reported_speed_multiplier": 1.1,'
            '  "max_speed_multiplier": 15,'
            '  "max_speed_exponent": 1.3,'
            "}", default="{}",)
        optional.add_argument(
            "--merge_params",
            help="JSON object with fragmenter parameters, or filepath "
            "@path/to/file.json.   For Example:"
            ""
            "{"
            '  "buffer_hours": 0.5,'
            "}",
            default="{}",
        )
        optional.add_argument(
            "--ssvid_filter_query",
            help="query that returns a list of ssvid to trim the sourced data down to. Note that "
            "the returned list is used in memory so should not be too large. This meant for "
            "testing purposes and if tempted to use for production, more work should be done "
            "so that the data is pruned on the way in.",
        )
        optional.add_argument(
            "--bins_per_day",
            default=4,
            help="Amount of containers per day to tag fragments and messages.",
        )
