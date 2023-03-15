import os
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

def query_cdp_data(event):

    print('*** query_cdp_data start')

    try:

        ########## Step 1: Get the data ##########
        conn = SalesforceCDPConnection(
                event['login_url'],
                event['client_id'],
                event['client_secret']
            )
        
        print('got CDP connection')
        
        ### Query CDP data ###        
        dlo_object = event['dlo_object']
        dlo_filter= event['dlo_filter']

        start_time = timer()
        df = conn.get_pandas_dataframe(f'select * from {dlo_object} {dlo_filter}')
        print(f"Query results took { str(timedelta(seconds=timer() - start_time))} time, for total records {len(df)}")

        print('*** query_cdp_data end')
        return df

    except Exception as e: 
        print('Query exception: '+ str(e))
        raise e

    
