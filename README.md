# datapipeline_project

The objective of this project is to build an end-to-end data pipeline that can handle event-based batch data and build a streaming pipeline.
1. Build an end-to-end data pipeline that can handle event-based batch data in cloud storage. 
2. Build a streaming data pipeline by leveraging Python to simulate from a dataset and publish them to Pub/Sub. Dataflow will process the streaming messages, do some transformations, and write the processed data to BigQuery.

#Solution Architecture

1. The user will upload a CSV file to the cloud storage bucket using gsutil command.

2. Cloud function - 1 will trigger a dataflow job (Job name: batch-pipeline-a) that reads from the CSV file, does some transformations, and writes the transformed data to another GCS bucket as a curated file. 

4. Cloud Function - 2 will trigger another dataflow job(Job name: batch-pipeline-b) that reads from the curated CSV file, does light transformations, and writes the results to BigQuery for storage and analysis.

5. Streaming Pipeline by Leveraging python to simulate messages from a dataset and publish them to cloud Pub/Sub.
Data Flow will process the streaming messages from pub/sub, applying transformations and writing the processed data to BigQuery.



