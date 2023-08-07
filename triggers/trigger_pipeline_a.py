import os
from googleapiclient.discovery import build

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH to KEY file'

project = 'PROJECT_ID'
job = 'Batcb-pipeline-a'
template='gs://TEMPLATE_BUCKET/templates/gcs_to_curated'

parameters = {
    'input': 'gs://INPUT_BUCKET/app_data.csv',
    'output': 'gs://OUTPUT_BUCKET/staging/curated'
}

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