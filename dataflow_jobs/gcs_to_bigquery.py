import os
import time
import pytz
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import _BeamArgumentParser, PipelineOptions, GoogleCloudOptions
from apache_beam.utils.timestamp import Timestamp
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH to key file'

# gcs objects
BUCKET_APP = 'final-curated-390005'
BLOB_APP = 'curated-00000-of-00001.csv'


class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='project id'
        )
        parser.add_value_provider_argument(
            '--input',
            help='Path of the file to read from'
        )
        parser.add_argument(
            '--output',
            required=True,
            help='Output schema to write the file to'
        )
        parser.add_argument(
            '--temproary_location',
            help='Dataflow write the files to be loaded to BigQuery'
        )
     
################################ Application Transformations ######################################
def get_columns():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_APP)
    app = bucket.blob(BLOB_APP)

    with app.open('r') as file:
        header = file.readline()
        column_names = [i.strip() for i in header.split(',')]
    return column_names

class to_json_app(beam.DoFn):
    def __init__(self, column_names):
        self.column_names = column_names
    def process(self, element):
        values = element.split(',')
        row = dict(
            zip(self.column_names,values))
        return [row]

def RemoveColumnsForApp(element):
    del element['FLAG_PHONE']
    del element['WALLSMATERIAL_MODE']
    del element['ELEVATORS_MODE']
    del element['ELEVATORS_AVG']
    del element['ELEVATORS_MEDI']
    return element

################################### Start of Beam Pipeline ########################################
def run_pipeline():

    column_names = get_columns()
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        my_options = pipeline_options.view_as(DataflowOptions)
        # Read the Initial CSV File from GCS bucket
        read_application = (
            p
            | 'Read Application file from GCS' >> beam.io.ReadFromText(my_options.input, skip_header_lines=1)
        )
        # Apply Transformations to Application Pcollection
        application_transformations = (
            read_application
            | 'Applications strings to Json for Bigquery Insert' >> beam.ParDo(to_json_app(column_names))
            | 'Remove unwanted columns in Application file' >> beam.Map(RemoveColumnsForApp)
        )
        # Write Application Transformation Results to BQ
        write_application_to_BQ = (
            application_transformations
            | 'Write application result to Bigquery' >> beam.io.WriteToBigQuery(
                table=my_options.output,
                custom_gcs_temp_location=my_options.temproary_location,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method='FILE_LOADS'
            )
        )

run_pipeline()
