import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

import config


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='data/input.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    parser.add_argument('--runner',
                        dest='runner',
                        required=True,
                        help='Choose Runner [DirectRunner or DataflowRunner].')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args).from_dictionary(
        {
            "job_name": config.JOB_NAME,
            "project": config.PROJECT_ID,
            "region": config.REGION,
            "staging_location": config.STAGING_LOCATION,
            "temp_location": config.TEMP_LOCATION,
            "enable_streaming_engine": False,
            "runner": known_args.runner
        }
    )
    print("data: ", pipeline_options.display_data())
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'ReadMyFile' >> beam.io.ReadFromText(known_args.input)
    lines | "Write File" >> beam.io.WriteToText(known_args.output)
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(config.LOG_LEVEL)
    run()
