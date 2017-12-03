from pipe_template.definition import PipelineDefinition
import apache_beam as beam
import logging
import pipe_template.options.parser as parser


def run(args=None, force_wait=False):
    (options, pipeline_options) = parser.parse(args=args)

    definition = PipelineDefinition(options)
    pipeline = definition.build(beam.Pipeline(options=pipeline_options))
    job = pipeline.run()

    if force_wait or (options.remote and options.wait):
        job.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
