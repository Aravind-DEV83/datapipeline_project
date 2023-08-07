import os
from googleapiclient.discovery import build

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH TO KEY FILE'

project='PORJECT_ID'
job='batch-prev-pipeline'
template = 'gs://beam-templates-390005/templates/prev_gcs_to_bq'

parameters = {
    'input': 'gs://previous-application-390005/previous_app.csv',
    'output': 'TABLE_ID',
    'temproary_location': 'gs://external-temp-390005/temp/'
}

dataflow = build("Dataflow", "v1b3")

request = (
    dataflow.projects().templates().launch(
        projectId=project,
        gcsPath=template,
        body = {
            "jobName": job,
            "parameters": parameters
        }
    )
)

response = request.execute()