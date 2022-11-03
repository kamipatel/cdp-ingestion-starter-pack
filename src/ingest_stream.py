import os
import random
import boto3
import pandas as pd
from cdpcon.connection import SalesforceCDPConnection
from cdpcon.authentication_helper import AuthenticationHelper
from cdpcon.query_submitter import QuerySubmitter
import json
from io import StringIO
from datetime import datetime
import logging
import io

session = boto3.Session()
s3 = session.resource('s3')

logger = logging.getLogger()

def write_csv_to_s3(df, bucket, k):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Bucket(bucket).put_object(Key= k, Body=csv_buffer.getvalue())     

def lambda_handler(event, context):
    print('Ingest called')
    try:

        ingest_bucket_name = os.environ['LOAD_DATA_BUCKET'] 

        ########## Step 1: Get the data ##########
        conn = SalesforceCDPConnection(
                event['login_url'],
                event['user_name'],
                event['password'],
                event['client_id'],
                event['client_secret']
            )
        
        print('got connection')        

        ## Ingest the data ##
        authenticationHelper = AuthenticationHelper(conn)
        token, instance_url = authenticationHelper.get_token()


        # Streaming sample
        athletes = []
        athletes_insert_count = 5
        for c in range(1, athletes_insert_count):
            i = random.randrange(100, 10000, 3)
            fname = f"fname-{i}@kam.cdp"
            lname = f"lname-{i}@kam.cdp"
            email = f"test-{i}@kam.cdp"
            a = {
            "maid":1010,
            "first_name":fname,
            "last_name":lname,
            "email":email,
            "gender":"Male",
            "city":"austin",
            "state":"TX",
            "created":"2021-10-07T09:11:11.816319Z"
            }
            athletes.append(a)

        dlo = ''
        data = {"data": athletes}

        #streaming api
        sources = "athlete_api"
        object = ""
        json_payload = QuerySubmitter._post_ingest_stream(dlo, instance_url, token, data, sources, object)        
        print(json_payload)
                    
        print('Ingest Stream complete')

    except Exception as e: 
        print('Ingest Stream exception: '+ str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Ingest Stream complete!')
    }


#lambda_handler({}, {})