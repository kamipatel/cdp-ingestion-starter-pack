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

def read_s3_contents_with_download(bucket, k):
    response = s3.Object(bucket, k).get()
    return response['Body']

def write_csv_to_s3(df, bucket, k):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.Bucket(bucket).put_object(Key= k, Body=csv_buffer.getvalue())     

def lambda_handler(event, context):
    print('***Ingest start')
    try:

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

        print(instance_url)

        #csv = "maid,first_name,last_name,email,gender,city,state,created\n1010,kamfle,patfl,kssam@coms.cooo,male,sgn,CA,2021-10-07T09:11:11.816319Z\n10120,sss,sssssdf,kvsssam@cocms.cooo,male,hanoi,CA,2021-10-07T09:11:11.816319Z\n"
        objinfo = {
            "object":event['object'],
            "sourceName":event['sourceName'],
            "operation":"upsert"            
        }

        #ingest_stage_bucket_name = os.environ['STAGE_BUCKET'] 
        ingest_stage_bucket_name = event['STAGE_BUCKET']
        ingest_stage_bucket = s3.Bucket(ingest_stage_bucket_name)
        stage_path = 'out/'

        print("Bulk Process staged data, path->" + ingest_stage_bucket_name)
        for bin_object in ingest_stage_bucket.objects.filter(Prefix= stage_path):                      
            print(bin_object.key)
            k = bin_object.key.split('/')            
            if(".csv" not in bin_object.key):                
                continue
            else:
                ########### Step 1: Read the filecall bulk API. You can modify this to read all file and put in single dataframe and then call 1 bulk API ##########                
                data_csv = read_s3_contents_with_download(ingest_stage_bucket_name, bin_object.key)                

                df = pd.read_csv(data_csv)                  
                s_buf = io.StringIO()
                df.to_csv(s_buf, index=False)
                csv = s_buf.getvalue()       

                # Do not start if any open jobs
                inflight_job = False
                
                instance_url = "https://" + instance_url
                json_payload = QuerySubmitter._get_ingest_bulk_jobs(instance_url, token)    
                #print(json_payload)   
                
                for j in json_payload["data"]:
                    if(j['state'] != 'JobComplete' or j['state'] == 'Open' or  j['state'] == 'UploadComplete'):
                        inflight_job = True
                        print(f"Open job id {j['id']}")
                        break        
                
                if(inflight_job):
                    print("Found an open job so can not start another bulk job right now!")
                    raise Exception("Found an open job so can not start another bulk job right now!")
                else:                                        
                    json_payload = QuerySubmitter._post_ingest_bulk_job_start(instance_url, token, objinfo)        
                    print("initiate job")
                    #print(json_payload)            
                    jobid = json_payload["id"]
                    print(jobid)                    
                    
                    #jobid = "e3dbde53-1f4a-4ac4-b0c5-25659d9eb621"            
                    json_payload = QuerySubmitter._post_ingest_bulk_job_upload_data(instance_url, token, jobid, csv)        
                    print("added data")
                    print(json_payload)                    

                    json_payload = QuerySubmitter._patch_ingest_bulk_job_update_status(instance_url, token, jobid, "", api_version='V2', enable_arrow_stream=False, state='UploadComplete')
                    print("started data job")
                    print(json_payload)               

                    processed_key=bin_object.key.replace('out', 'bulk-processed')
                    write_csv_to_s3(df, ingest_stage_bucket_name, processed_key)        
                    s3.Object(ingest_stage_bucket_name,bin_object.key).delete()

                    break # process only one file. You can modify this to load all all files in one Pandas dataframe and then call bulk api
            
                    
        print('***Ingest end')

    except Exception as e: 
        print('Ingest exception: '+ str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Ingest complete!')
    }


