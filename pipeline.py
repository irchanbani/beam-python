import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

import config


class PrintData(beam.DoFn):
    def process(self, element, *args, **kwargs):
        print("Data: {}".format(element))


class FilterData(beam.DoFn):
    def process(self, element, *args, **kwargs):
        if element == "goat":
            yield element
        elif element == "alien":
            yield beam.pvalue.TaggedOutput("alien", element)
        else:
            yield beam.pvalue.TaggedOutput("non animal", element)


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
            "enable_streaming_engine": False,
            "runner": known_args.runner
        }
    )
    print("data: ", pipeline_options.display_data())
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    lines = p | 'Create' >> beam.Create(['fork alien', 'glass cup', 'goat'])
    output = lines | 'Split' >> (beam.FlatMap(lambda x: x.split(' ')).with_output_types(str))
    # output | 'Print' >> beam.ParDo(PrintData())
    data = output | 'filter' >> beam.ParDo(FilterData()).with_outputs("alien", "non animal", main="animal")
    animal = data.animal
    alien = data.alien
    non_animal = data["non animal"]
    # animal | 'Print' >> beam.ParDo(PrintData())
    alien | 'Print' >> beam.ParDo(PrintData())
    # non_animal | 'Print' >> beam.ParDo(PrintData())
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(config.LOG_LEVEL)
    run()
