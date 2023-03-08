import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  #Using SQL read the data from BigQuery
  BQ_source = beam.io.BigQuerySource(query = 'SELECT start_station_id, end_station_id, count(*) as count \
      FROM [bigquery-public-data:london_bicycles.cycle_hire] group by start_station_id, end_station_id \
          having start_station_id is not null and end_station_id is not null')
  # The pipeline will be run on exiting the with block.
 
  with beam.Pipeline(options=pipeline_options) as p:
    
    # Read the text file[pattern] into a PCollection.
    BQ_data = p | beam.io.Read(BQ_source)
    #Map the dictionary type data to tuples
    BQ_data2 = BQ_data | 'Format' >> beam.Map(lambda elem: (elem['start_station_id'],elem['end_station_id'], elem['count']) )

    # Method to format the output form tuple to text with comma seperator
    def format_result(word, count1, count2):
        return '%s, %s, %s' % (word, count1, count2)

    BQ_data3 = BQ_data2 | 'Format2' >> beam.MapTuple(format_result)

    # Write the output to the location as per the output argument while running the query 
    BQ_data3 | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()