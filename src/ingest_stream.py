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

def ingest_cdp_data_stream(event, json_data):
    print('***ingest_cdp_data_stream start')

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

        data = {"data": json_data}

        #streaming api
        sources =  event['dlo_source_name']
        object = event['dlo_name']
        json_payload = QuerySubmitter._post_ingest_stream(instance_url, token, data, sources, object)        
        print(json_payload)
                    
        print('ingest_cdp_data_stream complete')

    except Exception as e: 
        print('ingest_cdp_data_stream exception: '+ str(e))
    
