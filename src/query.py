import os
import boto3
import pandas as pd
from cdpcon.connection import SalesforceCDPConnection
from cdpcon.authentication_helper import AuthenticationHelper
from cdpcon.query_submitter import QuerySubmitter
import json
from io import StringIO
from datetime import datetime
import logging
from timeit import default_timer as timer
from datetime import timedelta

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
        conn = SalesforceCDPConnection(
                event['login_url'],
                event['user_name'],
                event['password'],
                event['client_id'],
                event['client_secret']
            )
        
        print('got connection')
        
        ### Query CDP data ###        
        dlo_object = event['dlo_object']
        dlo_filter= event['dlo_filter']
        ingest_stage_bucket_name = event['STAGE_BUCKET']

        start_time = timer()
        df = conn.get_pandas_dataframe(f'select * from {dlo_object} {dlo_filter}')
        print("Query results in %s for total records %s", str(timedelta(seconds=timer() - start_time)), len(df))

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

