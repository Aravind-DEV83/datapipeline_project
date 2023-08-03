import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH to key file'

class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--project_id',
            help='project id'
        )
        parser.add_argument(
            '--input',
            # default='gs://',
            help='Path of the file to read from')
        parser.add_argument(
          '--output',
          required=True,
          help='Output file to write results to')


################################ Light Transformations on Applications data ######################################
def map_transform(element):
    mapping = {
        "M": "Male",
        "F": "Female",
        "N": "Not specified"
        }
    if element[3] in mapping:
        element[3] = mapping[element[3]]
    return element

def to_upper(element):
    element[2] = element[2].upper()
    return element
    
def filter_null(element):
    columns_to_replace = [45,46,73]
    for index in columns_to_replace:
        if element[index] == '':
            element[index] = "0"
    return element

def to_csv(element):
    return ','.join([str(i) for i in element])


def run_pipeline():

    # pipeline_options = PipelineOptions.from_dictionary(beam_options)
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:

        my_options = pipeline_options.view_as(DataflowOptions)

        # Read the Intial Application file from GCS bucket
        read_application = (
            p 
            | "Read from input file" >> beam.io.ReadFromText(my_options.input, skip_header_lines=1)
        )
        # Apply Transformations to each pcollection element
        transformations = (
            read_application
            | "Split string" >> beam.Map(lambda element: element.split(','))
            | "Mapping Transform based on column values" >> beam.Map(map_transform)
            | "string to uppercase" >> beam.Map(to_upper)
            | "Replace Null with Zero" >> beam.Map(filter_null)
            | "To csv " >> beam.Map(to_csv)
        )
        # Write the transformations to curated gcs bucket
        write_to_gcs = (
            transformations
            | "write to output file" >> beam.io.WriteToText(my_options.output, file_name_suffix='.csv', num_shards=1)
        )
run_pipeline()