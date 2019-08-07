import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

import config


class FilterCategory(beam.DoFn):
    def process(self, element, *args, **kwargs):
        if element["category"] == "news":
            yield element
        else:
            yield beam.pvalue.TaggedOutput("failed", element)


def run(argv=None):
    parser = argparse.ArgumentParser()
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
            "streaming": True,
            "runner": known_args.runner
        }
    )
    print("data: ", pipeline_options.display_data())
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    message = p | 'Pubsub' >> beam.io.ReadFromPubSub(subscription=config.PUBSUB_SUBSCRIPTIONS, with_attributes=False)
    parsed_msg = message | "Parsing" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
    # parsed_msg | "Print" >> beam.Map(lambda x: print("res: {}".format(x)))
    filtered_msg = parsed_msg | "Filter Category" >> beam.ParDo(FilterCategory()).with_outputs("failed", main="success")
    success_data = filtered_msg.success | "change" >> beam.Map(lambda x: json.dumps(x))
    success_data | "Write" >> beam.io.WriteToText(file_path_prefix="data/success",
                                                  file_name_suffix=".txt",
                                                  append_trailing_newlines=True)
    failed_data = filtered_msg.failed | "Change for failed" >> beam.Map(json.dumps)
    failed_data | "Write for failed" >> beam.io.WriteToText(file_path_prefix="data/failed",
                                                            file_name_suffix=".txt",
                                                            append_trailing_newlines=True)
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(config.LOG_LEVEL)
    run()
