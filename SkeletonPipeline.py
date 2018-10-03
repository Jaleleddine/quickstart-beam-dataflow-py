# Skeleton Pipeline for Apache Beam (Python)
# github.com/heerman
import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def runMyPipeline(argv=None):
  """Main entry point"""

  # Parse arguments, prepare pipeline options
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # Define the dataflow pipeline
  p = beam.Pipeline(options=pipeline_options)

  # Import local or cloud data and write contents to local disk
  dataset1 = '/Users/jdoe/mydataset.txt'
  dataset2 = 'gs://apache-beam-samples/shakespeare/kinglear.txt'

  dataout = (p
    | 'dataset_import' >> ReadFromText(dataset1)
    | 'write_to_disk' >> WriteToText('outputfile'))

  # Execute the pipeline
  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  runMyPipeline()
