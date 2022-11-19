import os
import random
import pandas as pd
from cdpcon.connection import SalesforceCDPConnection
from cdpcon.authentication_helper import AuthenticationHelper
from cdpcon.query_submitter import QuerySubmitter
import json
from io import StringIO
from datetime import datetime
import logging
import io

def ingest_cdp_data_bulk(event, df):
    print('***ingest_cdp_data_bulk start')
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
            "object":event['dlo_name'],
            "sourceName":event['dlo_source_name'],
            "operation":event['bulk_operation_type']        
        }

        ########### Step 1: Read the filecall bulk API. You can modify this to read all file and put in single dataframe and then call 1 bulk API ##########                
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
                    
            json_payload = QuerySubmitter._post_ingest_bulk_job_upload_data(instance_url, token, jobid, csv)        
            print("added data")
            print(json_payload)                    

            json_payload = QuerySubmitter._patch_ingest_bulk_job_update_status(instance_url, token, jobid, "", api_version='V2', enable_arrow_stream=False, state='UploadComplete')
            print("started data job")
            print(json_payload)               

            print('***ingest_cdp_data_bulk end')

            return json_payload
            
    except Exception as e: 
        print('ingest_cdp_data_bulk exception: '+ str(e))
        raise e
    


