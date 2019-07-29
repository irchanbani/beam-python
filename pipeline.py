import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())

lines = p | 'ReadMyFile' >> beam.io.ReadFromText('data/input.csv')

lines | "Write File" >> beam.io.WriteToText('data/output.csv')

p.run().wait_until_finish()
