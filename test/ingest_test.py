import sys
import boto3
import pandas as pd
import json
import io

# modify this to your local path
sys.path.insert(0, '/Users/kamlesh.patel/kam/cdp/cdpisv/cdp_ingestion_starter_pack/src')

from query import query_cdp_data as query
from enrich import enrich_cdp_data as enrich
from ingest_bulk import ingest_cdp_data_bulk as ingest_bulk

# Update this before running the test
event = {}
event['login_url'] = 'https://login.salesforce.com' 
event['user_name'] = '' #CDP org's username
event['password'] = '' #CDP org's password
event['client_id'] = '' #CDP org's connected app client id
event['client_secret'] = '' #CDP org's connected app client secret
event['dlo_name'] = 'athlete_profiles' #CDP org's DLO name
event['dlo_source_name'] = 'athlete_api' #CDP org's Ingestion API source name 
event['STAGE_BUCKET'] = '' #Bucket name (excluding S3://)
event['tenant'] = 'northwind' #Customer tanent name (can be anything for testing) 
event['dlo_object'] = 'athlete_api_athlete_profiles_E6B17A31__dll' #DLO object name to be queried
event['dlo_filter'] = 'limit 100' #Where clause for the query
event['bulk_operation_type'] = 'upsert' #Where clause for the query

df = query(event)
df = enrich(df)
ingest_bulk(event,df)
