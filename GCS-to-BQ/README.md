# GCS to BigQuery Batch Pipeline

This pipeline reads data files from a Google Cloud Storage (GCS) bucket, processes them using Apache Beam on Google Cloud Dataflow, and writes the results into a BigQuery table.

## Steps Performed
1. Reads CSV/JSON data from GCS.
2. Parses and transforms the data.
3. Loads the processed data into a specified BigQuery table.

## Technologies Used
- Python
- Apache Beam (Batch mode)
- Google Cloud Dataflow
- Google Cloud Storage
- BigQuery

## How to Run
```bash
python pipeline.py \
    --runner DataflowRunner \
    --project <YOUR_PROJECT_ID> \
    --region <YOUR_REGION> \
    --staging_location gs://<YOUR_BUCKET>/staging \
    --temp_location gs://<YOUR_BUCKET>/temp \
    --output_table <YOUR_PROJECT_ID>:<DATASET>.<TABLE_NAME>
