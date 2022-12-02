import sys
import boto3
import pandas as pd
import json
import io

# modify this to your local path
sys.path.insert(0, '/Users/kamlesh.patel/kam/cdp/cdpisv/cdp_ingestion_starter_pack/src')

from query import query_cdp_data 
from enrich import enrich_cdp_data 
from ingest_bulk import ingest_cdp_data_bulk 

# Update this before running the test
event = {}
event['login_url'] = 'https://login.salesforce.com' 
event['user_name'] = '' #cdp org's username
event['password'] = '' #cdp org's password
event['client_id'] = '' #cdp connected app clientid
event['client_secret'] = '' #cdp connected app client secret
event['dlo_source_name'] = 'External_Lead_API' #CDP org's Ingestion API source name 
event['dlo_name'] = 'External_Lead_Object' #CDP org's DLO name
event['dlo_object'] = '' #DLO object name to be queried. Copy from Data stream's "Object API Name" field
event['dlo_filter'] = '' #Where clause for the query. 
event['bulk_operation_type'] = 'upsert' #Where clause for the query  

df = query_cdp_data(event)
df = enrich_cdp_data(df)
ingest_cdp_data_bulk(event,df)
