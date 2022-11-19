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

session = boto3.Session()
s3 = session.resource('s3')

logger = logging.getLogger()

def read_s3_contents_with_download(bucket, k):
    response = s3.Object(bucket, k).get()
    return response['Body']

def write_csv_to_s3(df, bucket, k):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Bucket(bucket).put_object(Key= k, Body=csv_buffer.getvalue())     

def lambda_handler(event, context):
    print('***Enrich start')
    try:

        ingest_stage_bucket_name = event['STAGE_BUCKET']
        #ingest_stage_bucket_name = os.environ['STAGE_BUCKET'] 
        ingest_stage_bucket = s3.Bucket(ingest_stage_bucket_name)
        stage_path = 'stage/'

        print("Enrich Process staged data, path->" + ingest_stage_bucket_name)
        for bin_object in ingest_stage_bucket.objects.filter(Prefix= stage_path):                      
            print(bin_object.key)
            k = bin_object.key.split('/')            
            if(".csv" not in bin_object.key):                
                continue
            else:
                ########### Step 1: Read the file and take a backup by putting under 'processed' folder ##########                
                data_csv = read_s3_contents_with_download(ingest_stage_bucket_name, bin_object.key)
                df = pd.read_csv(data_csv)                  
                print("Enrich file")
                print(len(df))
                print(k)
                processed_key=bin_object.key.replace('stage', 'enrich-processed')
                write_csv_to_s3(df, ingest_stage_bucket_name, processed_key)        

                ########### Step 2: Process the incoming data ##########
                '''
                df["score"] = 0
                df.loc[df.state__c == "austin", "score"] = 8
                df.loc[df.state__c == "hanoi", "score"] = 7
                df.drop(['datasourceobject__c', 'datasource__c'], axis=1, inplace=True)
                '''
                df["ssot__Description__c"] = "enriched"
                

                ### Step 3: Store the enriched file in the folder of the S3 bucket so either CDP can pick it up or bulk API can load the data in CDP. Delete the source file so it does not needs to be processed again               
                today = datetime.today().strftime('%Y-%m-%d,%H:%M:%S')
                tenant = k[1]                
                dlopath = ''
                if( len(k) > 6 and "dlo" in k[6]):
                    dlo=k[7]
                    dlopath= '/' + k[7]

                k = "out/" + tenant + dlopath + "/" + str(today) + ".csv"
                print(k)
                write_csv_to_s3(df, ingest_stage_bucket_name, k)        

                s3.Object(ingest_stage_bucket_name,bin_object.key).delete()
                                
        print('***Enrich end')

    except Exception as e: 
        print('Enrich exception: '+ str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Enrich complete!')
    }
