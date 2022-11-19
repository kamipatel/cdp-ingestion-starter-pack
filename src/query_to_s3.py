import os
import boto3
import pandas as pd
import json
from io import StringIO
from datetime import datetime
import logging
from timeit import default_timer as timer
from datetime import timedelta
from query import query_cdp_data as query

session = boto3.Session()
s3 = session.resource('s3')

logger = logging.getLogger()

def write_csv_to_s3(df, bucket, k):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Bucket(bucket).put_object(Key= k, Body=csv_buffer.getvalue())     

def lambda_handler(event, context):
    print('*** Query start')
    try:

        ingest_stage_bucket_name = os.environ['STAGE_BUCKET'] 

        ########## Step 1: Get the data ##########
        df = query(event)

        ### Store in S3 bucket so data enrichment function can pick it up
        today = datetime.today().strftime('%Y-%m-%d,%H:%M:%S')
        year= datetime.today().strftime('%Y')
        month= datetime.today().strftime('%m')
        day= datetime.today().strftime('%d')
        
        k = "stage/" + event['tenant'] + "/Salesforce-c360-Data/" + year + "/" + month + "/" + day + "/dlo/" + dlo_object + "/" + today  + ".csv"
        print(k)
        write_csv_to_s3(df, ingest_stage_bucket_name, k)        

        print('*** Query end')

    except Exception as e: 
        print('Query exception: '+ str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Query complete!')
    }

