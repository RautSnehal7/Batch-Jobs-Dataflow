import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Set service account path (locally for template creation)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Snehal Raut\Documents\Raut Snehal\Google DataFlow and Apache Beam\dataflow-course-467812-ac1182a1cdc8.json"

# Define pipeline options
pipeline_options_dict = {
    'project': 'dataflow-course-467812',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://dataflow-course-7/temp',
    'temp_location': 'gs://dataflow-course-7/temp',
    'template_location': 'gs://dataflow-course-7/template/batch_job_dataflow_gcs_to_gcs_8'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options_dict)

# Define Beam pipeline
p = beam.Pipeline(options=pipeline_options)

# Custom filter DoFn class
class FilterDelays(beam.DoFn):
    def process(self, record):
        if int(record[8]) > 0:
            return [record]

# Compute total delay time per airport
delayed_time = (
    p
    | "Read Data for Time" >> beam.io.ReadFromText('gs://dataflow-course-7/input/flights_sample.csv', skip_header_lines=1)
    | "Split Time Records" >> beam.Map(lambda record: record.split(','))
    | "Filter Delays Time" >> beam.ParDo(FilterDelays())
    | "Key-Value Time" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Sum Delays by Key" >> beam.CombinePerKey(sum)
)

# Count number of delays per airport
delayed_num = (
    p
    | "Read Data for Count" >> beam.io.ReadFromText('gs://dataflow-course-7/input/flights_sample.csv', skip_header_lines=1)
    | "Split Count Records" >> beam.Map(lambda record: record.split(','))
    | "Filter Delays Count" >> beam.ParDo(FilterDelays())
    | "Key-Value Count" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Count Delays by Key" >> beam.combiners.Count.PerKey()
)

# Group results and write to output
final_output = (
    {"Delayed_time": delayed_time, "Delayed_num": delayed_num}
    | "Group by Airport" >> beam.CoGroupByKey()
    | "Write to GCS" >> beam.io.WriteToText('gs://dataflow-course-7/output/flights_output')
)

p.run()
