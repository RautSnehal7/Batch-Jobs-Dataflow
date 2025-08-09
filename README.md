**Batch Job Dataflow Pipelines**
This repository contains two Google Cloud Dataflow batch pipelines built using Apache Beam in Python:

**GCS to GCS Pipeline** – Reads data from a source bucket, processes it, and writes the transformed data to a target bucket.

**GCS to BigQuery Pipeline** – Reads JSON data from GCS, transforms it (including flattening nested fields), and loads it into a BigQuery table.

**These projects demonstrate**:

Apache Beam batch processing
Integration with Google Cloud Storage and BigQuery
Data transformations including unnesting and formatting for BigQuery

**Tech Stack**
Python 3
Apache Beam
Google Cloud Dataflow
GCS
BigQuery

**How to Run**
Set up a virtual environment.
Install dependencies from requirements.txt.
Configure your GCP project, bucket names, and BigQuery dataset/table.
Run the pipeline using the DirectRunner (local) or DataflowRunner (cloud).
