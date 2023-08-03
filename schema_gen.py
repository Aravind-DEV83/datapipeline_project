import json
import pandas as pd
from google.cloud import storage


# def get_schema():   
# # Load the CSV file into a pandas DataFrame
#     df = pd.read_csv('sample.csv')
#     print(df.dtypes.head(60))
# Generate the schema
    # schema = []
    # for column in df.columns:
    #     if df[column].dtype == 'object':
    #         schema.append({'name': column, 'type': 'STRING', 'mode': 'NULLABLE'})
    #     elif df[column].dtype == 'int64':
    #         schema.append({'name': column, 'type': 'INTEGER', 'mode': 'NULLABLE'})
    #     elif df[column].dtype == 'float64':
    #         schema.append({'name': column, 'type': 'FLOAT', 'mode': 'NULLABLE'})
    #     elif df[column].dtype == 'bool':
    #         schema.append({'name': column, 'type': 'BOOLEAN', 'mode': 'NULLABLE'})
    #     # schema.append({'name': 'Last_Updated_Time', 'type': 'TIMESTAMP', 'mode':'NULLABLE'})

    # return schema
    
def schema():
    df = pd.read_csv('app_data.csv')
    cols = df.columns
    types = ["int64","float64","object"]

    cols_types=[]
    for i in cols:
        if df[i].dtype=='int64':
            ans = i+':INTEGER'
            cols_types.append(ans)
        if df[i].dtype=='float64':
            ans=i+':FLOAT'
            cols_types.append(ans)
        if df[i].dtype=='object':
            ans=i+':STRING'
            cols_types.append(ans)
        
    return ",".join(cols_types)

with open('schema.txt','w') as file:
    file.write(schema())

