import os
from googleapiclient.discovery import build

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH TO KEY FILE'


project = 'PROJECT_ID'
job = 'batch-pipeline-b'
template='gs://TEMPLATE_BUCKET/templates/gcs_to_bigquery'

parameters = {
    'input': 'gs://INPUT_BUCKET/curated-00000-of-00001.csv',
    'output': 'DATASET_ID',
    'temproary_location': 'gs://external-temp-390005/temp/'
}


# def trigger(event, context):
dataflow = build("dataflow", "v1b3")
request = (
    dataflow.projects().templates().launch(
        projectId=project,
        gcsPath=template,
        body={
            "jobName": job,
            "parameters": parameters
        },
    )
)
response = request.execute()
print(response)
# return response
