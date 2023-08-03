import os
import apache_beam as beam
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/aravind_jarpala/Downloads/Projects/secret/key.json'

BUCKET_PREV = 'previous-application-390005'
BLOB_PREV = 'previous_app.csv'

class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='Project ID'
        )
        parser.add_value_provider_argument(
            '--input',
            help='Input GCS path'
        )
        parser.add_argument(
            '--output',
            help='output table id'
        )
        parser.add_argument(
            '--temproary_location'
        )

def to_upper(element):
    element[2] = element[2].upper()
    return element
def del_column(element):
    del element['NAME_CLIENT_TYPE']
    return element

def get_columns():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_PREV)
    app = bucket.blob(BLOB_PREV)

    with app.open('r') as file:
        header = file.readline()
        column_names = [i.strip() for i in header.split(',')]
    return column_names

class to_json_app(beam.DoFn):
    def __init__(self, column_names):
        self.column_names = column_names
    def process(self, element):
        # values = element.split(',')
        row = dict(
            zip(self.column_names,element))
        return [row]



def run_pipeline():
    column_names = get_columns()
    pipeline_options = PipelineOptions()


    with beam.Pipeline(options=pipeline_options) as p:
        my_options = pipeline_options.view_as(DataflowOptions)

        read_prev_application = (
            p
            | "Read Previous Application file" >> beam.io.ReadFromText(my_options.input, skip_header_lines=1)
        )
        transformations = (
            read_prev_application
            | 'Split String' >> beam.Map(lambda element: element.split(','))
            | "Convert values to Upper case" >> beam.Map(to_upper)
            | 'Applications strings to Json for Bigquery Insert' >> beam.ParDo(to_json_app(column_names))
            | 'Removed columns' >> beam.Map(del_column)
            # | beam.Map(print)
        )
        write_to_bq = (
            transformations
            | 'Write transformed result to Bigquery' >> beam.io.WriteToBigQuery(
                table=my_options.output,
                custom_gcs_temp_location=my_options.temproary_location,
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method='FILE_LOADS'
            )
        )

run_pipeline()