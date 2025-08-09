import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Snehal Raut\Documents\Raut Snehal\Google DataFlow and Apache Beam\dataflow-course-467812-ac1182a1cdc8.json"

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


pipeline_options_dict = {
    'project' : 'dataflow-course-467812',
    'runner' : 'DataflowRunner',
    'region' : 'southamerica-east1',
    'staging_location' : 'gs://dataflow-course-7/temp',
    'temp_location' : 'gs://dataflow-course-7/temp',
    'template_location' : 'gs://dataflow-course-7/template/batch_dataflow_gcs_to_bq'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)
p = beam.Pipeline(options = pipeline_options)

class filter(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]
        
class split_lines(beam.DoFn):
    def process(self, record):
        return [record.split(',')]
    
def flatten_for_bq(record):
    airport, delayed_info = record
    return {
        'airport_name' : airport,
        'delayed_flights_num' : delayed_info.get('Delayed_num', [0]) [0],
        'delayed_flights_time' : delayed_info.get('Delayed_time', [0]) [0]
    }

table_schema = 'airport_name:STRING, delayed_flights_num:INTEGER, delayed_flights_time:INTEGER'
table = 'dataflow-course-467812:flights_dataflow.flights_delay_info'

delayed_time = (
    p
    | "Read Data for Time" >> beam.io.ReadFromText('gs://dataflow-course-7/input/flights_sample.csv', skip_header_lines=1)
    | "Split Time Records" >> beam.ParDo(split_lines())
    | "Filter Delays Time" >> beam.ParDo(filter())
    | "Key-Value Time" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum Delays by Key" >> beam.CombinePerKey(sum)
)

# Count number of delays per airport
delayed_num = (
    p
    | "Read Data for Count" >> beam.io.ReadFromText('gs://dataflow-course-7/input/flights_sample.csv', skip_header_lines=1)
    | "Split Count Records" >> beam.ParDo(split_lines())
    | "Filter Delays Count" >> beam.ParDo(filter())
    | "Key-Value Count" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Count Delays by Key" >> beam.combiners.Count.PerKey()
)

# Group results and write to output
final_output = (
    {"Delayed_time": delayed_time, "Delayed_num": delayed_num}
    | "Group by Airport" >> beam.CoGroupByKey()
    | "Unnest for bq" >> beam.Map(lambda record : flatten_for_bq(record))
    | "Write to BQ" >> beam.io.WriteToBigQuery(table,
                                                schema=table_schema,
                                                write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
                                                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                custom_gcs_temp_location = 'gs://dataflow-course-7/temp')
)

p.run()
