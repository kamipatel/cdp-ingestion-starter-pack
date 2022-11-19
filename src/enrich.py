import os
import pandas as pd
from cdpcon.connection import SalesforceCDPConnection
from cdpcon.authentication_helper import AuthenticationHelper
from cdpcon.query_submitter import QuerySubmitter
import json
from io import StringIO
from datetime import datetime
import logging

def enrich_cdp_data(df):
    print('*** enrich_cdp_data start')

    try:

        df["score"] = 10
        df.loc[df.state__c == "TX", "score"] = 8
        df.loc[df.state__c == "hanoi", "score"] = 7
        df.drop(['datasourceobject__c', 'datasource__c'], axis=1, inplace=True)
                
        print('***Enrich end')
        return df

    except Exception as e: 
        print('Query exception: '+ str(e))
        raise e
