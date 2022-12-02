## Disclaimer
This is not an official repo. It is just for the general guidance. AWS stack in this blog post is provided as-is, use it as a general guidance. For AWS you are responsible for everything including all the expenses incurred and security aspects. 

## About
> CDP Ingestion starter pack solution for ISVs is designed to run in your AWS environment. It is completely optional to use this for your implementation, you can take this code, repurpose and run it anywhere to suit your needs.

## Summary
- Provides python based wrapper functions for calling CDP ingestion APIs. It leverages code from existing open source <a href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_python_connector.htm"> python connector </a> as a base.
- Provides python based Notebook as a playground for easier testing of CDP
- Provides AWS stack i.e. Lambda functions which you can orchestrate using airflow or step functions

## Related developer docs
> <a href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_salesforce_cdp_ingestion.htm">Developer docs</a>

## Technical Implementation
> 4 python wrapper functions  
- "query.py": Query the data from customer's DLO (Data Lake object) and return pandas dataframe
- "enrich.py": Take pandas dataframe, perform enrichment and return pandas frame
- "ingest_bulk.py": Push the pandas dataframe to the Customer's CDP DLO using bulk API
- "ingest_stream.py": Push the pandas dataframe to the Customer's CDP DLO using streaming API

## Pre-req
> Create a new connected app (<a href="https://help.salesforce.com/s/articleView?id=sf.c360_a_create_ingestion_api_connected_app.htm&type=5"> developer doc</a>)     
- From Setup -> Apps->Apps Manager->New Connected app  
- Note down client id and secret values
> Create a new Ingestion API connector (<a href="https://help.salesforce.com/s/articleView?id=sf.c360_a_connect_an_ingestion_source.htm&type=5"> developer doc</a>)   
- From CDP setup -> Configuration->Ingestion API->Click New  
- Give it a name "External Lead API". From the git repo's Config directory, upload external_lead.yaml as a schema  
> From "Customer Data Platform" app-> "Data Streams" tab 
- Create a new data stream using the above Ingestion API. 
- Note down the value of the field "Object API Name" as dlo_object  

## Test using a python based notebook
> Demo use-case
In the External_Lead DLO, create Leads using streaming API => fetch the data using query API -> 
enrich the data using hard-coded logic and push the enriched data back to the DLO using bulk API

> Option 1: Test the wrapper functions by duplicating the deepnote notebook  
- Update the event values in the notebook
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

- Run through each cell (it is self explanatory)

> Option 2: Run the python function unser test/ingest_test.py after updating the event values

