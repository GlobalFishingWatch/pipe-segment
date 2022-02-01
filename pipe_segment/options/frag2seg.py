from apache_beam.options.pipeline_options import PipelineOptions


class Frag2SegOptions(PipelineOptions):
    DEFAULT_TEMP_SHARDS_PER_DAY = 16

    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group("Required")
        optional = parser.add_argument_group("Optional")

        optional.add_argument(
            "--date_range",
            help="Range of dates to read from source. format YYYY-MM-DD,YYYY-MM-DD",
        )
        required.add_argument(
            "--frag_source",
            required=True,
            help="Bigquery table (prefix) to read and write fragments",
        )
        required.add_argument(
            "--seg_map_dest",
            required=True,
            help="Bigquery table (prefix) to write the map from segments to fragments",
        )
        optional.add_argument(
            "--temp_shards_per_day",
            type=int,
            help="Number of shards to write per day in messages output temporary storage. "
            "A good value for this is the max number of workers.  Default %s"
            % cls.DEFAULT_TEMP_SHARDS_PER_DAY,
        )
        optional.add_argument(
            "--matcher_params",
            help="Pass a json object with parameters to pass to the fragmenter, or supply a file name to read with "
            "@path/to/file.json.   For Example:"
            ""
            "{"
            '  "buffer_hours": 0.5,'
            "}",
            default="{}",
        )
        optional.add_argument(
            "--wait_for_job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )
