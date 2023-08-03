import os
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions,SetupOptions
from google.cloud import bigquery
from apache_beam.io.gcp.internal.clients import bigquery as bq

# Schema for BigQuery Table 

SCHEMA = 'SK_ID_CURR:INTEGER,TARGET:INTEGER,NAME_CONTRACT_TYPE:STRING,CODE_GENDER:STRING,FLAG_OWN_CAR:STRING,FLAG_OWN_REALTY:STRING,CNT_CHILDREN:INTEGER,AMT_INCOME_TOTAL:FLOAT,AMT_CREDIT:FLOAT,AMT_ANNUITY:FLOAT,AMT_GOODS_PRICE:FLOAT,NAME_TYPE_SUITE:STRING,NAME_INCOME_TYPE:STRING,NAME_EDUCATION_TYPE:STRING,NAME_FAMILY_STATUS:STRING,NAME_HOUSING_TYPE:STRING,REGION_POPULATION_RELATIVE:FLOAT,DAYS_BIRTH:INTEGER,DAYS_EMPLOYED:INTEGER,DAYS_REGISTRATION:INTEGER,DAYS_ID_PUBLISH:INTEGER,OWN_CAR_AGE:STRING,FLAG_MOBIL:INTEGER,FLAG_EMP_PHONE:INTEGER,FLAG_WORK_PHONE:INTEGER,FLAG_CONT_MOBILE:INTEGER,FLAG_PHONE:INTEGER,FLAG_EMAIL:INTEGER,OCCUPATION_TYPE:STRING,CNT_FAM_MEMBERS:INTEGER,REGION_RATING_CLIENT:INTEGER,REGION_RATING_CLIENT_W_CITY:INTEGER,WEEKDAY_APPR_PROCESS_START:STRING,HOUR_APPR_PROCESS_START:INTEGER,REG_REGION_NOT_LIVE_REGION:INTEGER,REG_REGION_NOT_WORK_REGION:INTEGER,LIVE_REGION_NOT_WORK_REGION:INTEGER,REG_CITY_NOT_LIVE_CITY:INTEGER,REG_CITY_NOT_WORK_CITY:INTEGER,LIVE_CITY_NOT_WORK_CITY:INTEGER,ORGANIZATION_TYPE:STRING,EXT_SOURCE_1:FLOAT,EXT_SOURCE_2:FLOAT,EXT_SOURCE_3:FLOAT,APARTMENTS_AVG:FLOAT,BASEMENTAREA_AVG:FLOAT,YEARS_BEGINEXPLUATATION_AVG:FLOAT,YEARS_BUILD_AVG:FLOAT,COMMONAREA_AVG:FLOAT,ELEVATORS_AVG:FLOAT,ENTRANCES_AVG:FLOAT,FLOORSMAX_AVG:FLOAT,FLOORSMIN_AVG:FLOAT,LANDAREA_AVG:FLOAT,LIVINGAPARTMENTS_AVG:FLOAT,LIVINGAREA_AVG:FLOAT,NONLIVINGAPARTMENTS_AVG:FLOAT,NONLIVINGAREA_AVG:FLOAT,APARTMENTS_MODE:FLOAT,BASEMENTAREA_MODE:FLOAT,YEARS_BEGINEXPLUATATION_MODE:FLOAT,YEARS_BUILD_MODE:FLOAT,COMMONAREA_MODE:FLOAT,ELEVATORS_MODE:FLOAT,ENTRANCES_MODE:FLOAT,FLOORSMAX_MODE:FLOAT,FLOORSMIN_MODE:FLOAT,LANDAREA_MODE:FLOAT,LIVINGAPARTMENTS_MODE:FLOAT,LIVINGAREA_MODE:FLOAT,NONLIVINGAPARTMENTS_MODE:FLOAT,NONLIVINGAREA_MODE:FLOAT,APARTMENTS_MEDI:FLOAT,BASEMENTAREA_MEDI:FLOAT,YEARS_BEGINEXPLUATATION_MEDI:FLOAT,YEARS_BUILD_MEDI:FLOAT,COMMONAREA_MEDI:FLOAT,ELEVATORS_MEDI:FLOAT,ENTRANCES_MEDI:FLOAT,FLOORSMAX_MEDI:FLOAT,FLOORSMIN_MEDI:FLOAT,LANDAREA_MEDI:FLOAT,LIVINGAPARTMENTS_MEDI:FLOAT,LIVINGAREA_MEDI:FLOAT,NONLIVINGAPARTMENTS_MEDI:FLOAT,NONLIVINGAREA_MEDI:FLOAT,FONDKAPREMONT_MODE:STRING,HOUSETYPE_MODE:STRING,TOTALAREA_MODE:FLOAT,WALLSMATERIAL_MODE:STRING,EMERGENCYSTATE_MODE:STRING,OBS_30_CNT_SOCIAL_CIRCLE:INTEGER,DEF_30_CNT_SOCIAL_CIRCLE:INTEGER,OBS_60_CNT_SOCIAL_CIRCLE:INTEGER,DEF_60_CNT_SOCIAL_CIRCLE:INTEGER,DAYS_LAST_PHONE_CHANGE:FLOAT,FLAG_DOCUMENT_2:INTEGER,FLAG_DOCUMENT_3:INTEGER,FLAG_DOCUMENT_4:INTEGER,FLAG_DOCUMENT_5:INTEGER,FLAG_DOCUMENT_6:INTEGER,FLAG_DOCUMENT_7:INTEGER,FLAG_DOCUMENT_8:INTEGER,FLAG_DOCUMENT_9:INTEGER,FLAG_DOCUMENT_10:INTEGER,FLAG_DOCUMENT_11:INTEGER,FLAG_DOCUMENT_12:INTEGER,FLAG_DOCUMENT_13:INTEGER,FLAG_DOCUMENT_14:INTEGER,FLAG_DOCUMENT_15:INTEGER,FLAG_DOCUMENT_16:INTEGER,FLAG_DOCUMENT_17:INTEGER,FLAG_DOCUMENT_18:INTEGER,FLAG_DOCUMENT_19:INTEGER,FLAG_DOCUMENT_20:INTEGER,FLAG_DOCUMENT_21:INTEGER,AMT_REQ_CREDIT_BUREAU_HOUR:FLOAT,AMT_REQ_CREDIT_BUREAU_DAY:FLOAT,AMT_REQ_CREDIT_BUREAU_WEEK:FLOAT,AMT_REQ_CREDIT_BUREAU_MON:FLOAT,AMT_REQ_CREDIT_BUREAU_QRT:FLOAT,AMT_REQ_CREDIT_BUREAU_YEAR:FLOAT'

# To set GOOGLE_APPLICATION_CREDENTIALS environment variable, which is often used by Google Cloud SDK and related libraries.

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'PATH to KEY file'

# Bigquery objects

dataset='Bank_Loan'
table='bank_loan_stream_data'

# Passing command line arguments

class DataflowOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--subscription', help='provide the subscription id', default='projects/bi-team-390005/subscriptions/stream-bank-loan-sub')

################################ Transformations on data before sending data to bigquery ################################

def regular_exp(element):
  pattern = r'"([^"]*)"'
  matches = re.findall(pattern, element)
  for match in matches:
    replacement = match.replace(',', '')
    element = element.replace(match, replacement)
  result = re.sub(pattern, r'\1', element)
  return result

def create_record(element):
   global SCHEMA 
   dict={}
   value = SCHEMA.split(',')
   for i in range(len(value)):
      col = value[i].split(':')
      dict[col[0]] = element[i]
   return dict

def type_data(element):
   dict2=element
   col = SCHEMA.split(',')
   for i in range(len(col)):
      col1 = col[i].split(':')
      key=col1[0]
      value1=col1[1]
      value2 = dict2[key]

      if value1 == "INTEGER":
        try:
            dict2[key] = int(value2)
        except:
           if value2 == "":
              dict2[key]=0
           else:
              dict2[key] = int(float(value2))

      elif value1 == "FLOAT":
        try:
           dict2[key] = float(value2)
        except:
           dict2[key] = 0.0

      elif value1 == "BOOLEAN":
        if value2 == '0':
           dict2[key]='true'
        else:
           dict2[key]='false'
           
   return dict2

################################ Start of Beam Pipeline ################################################################

def run_pipeline():

  pipeline_options = PipelineOptions()
  
  pipeline_options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=pipeline_options) as p:

    my_options = pipeline_options.view_as(DataflowOptions)

    # Read the data from PubSub

    read_application = (
            p 
            | 'Read Data' >> beam.io.ReadFromPubSub(subscription=my_options.subscription)
        )
    
    # Apply Transformations to each pcollection element

    transformations = (
            read_application
        | 'Decode Data' >> beam.Map(lambda x: x.decode('UTF-8'))
        | 'Transformation1' >> beam.Map(regular_exp)
        | 'Splitting data' >> beam.Map(lambda x:x.split(','))
        | 'Formatting to Dictionary' >> beam.Map(create_record)
        | 'Transformation2' >> beam.Map(type_data)
        )

    # Write the transformations to BigQuery

    write_to_bq = transformations  | 'writeto bq' >> beam.io.WriteToBigQuery(
        table='bi-team-390005:{}.{}'.format(dataset,table),
        schema=SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method='STREAMING_INSERTS' 
    )

run_pipeline()
    

