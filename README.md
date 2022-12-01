## Disclaimer
This is not an official repo. It is just for the general guidance. AWS stack in this blog post is provided as-is, use it as a general guidance. For AWS you are responsible for everything including all the expenses incurred and security aspects. 

## About
> CDP Ingestion starter pack solution for ISVs is designed to run in your AWS environment. It is completely optional to use this for your implementation, you can take this code, repurpose and run it anywhere to suit your needs.
> Demo use-case: Partner retrieves the athlete data from customer CDP's DLO (Data Lake Object), enriches that with a score and pushes the enriched data back to the customer CDP's DLO   

## Summary
- This AWS stack provides a boiler plate code for calling CDP ingestion APIs. It leverages code from existing open source <a href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_python_connector.htm"> python connector </a>

## Design consideration  
> Supports csv format only  

## CDP API 
> <a href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_salesforce_cdp_ingestion.htm">Developer docs</a>

## Install the AWS stack (optional)
On your local machine, if you do not have npm or AWS CDK…  
Install npm by running the command “npm install -g npm”  
Install AWS CLI by running command “npm install -g aws-cdk”  

From command line:  
> Run “AWS configure” (<a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html"> use your IAM credentials for this. Check the section Managing access keys (console))</a>  
> Run the command “cdk bootstrap aws://YOUR-AWS-ACCOUNT-NUMBER/us-east-1” (replacing YOUR-AWS-ACCOUNT-NUMBER with your AWS account number)
> Run the command "cdk deploy" which will create the stack in your AWS

## Key components
> 1 S3 bucket  
- cdp-ingest-data-bucket-*** Stores ingestion data for a specific customer (sent by customer's salesforce CDP). You will need a seperate bucket for each customer data. 

> 4 lambda functions  
- "query.py": Query the data from customer's DLO (Data Lake object) and return pandas dataframe
- "enrich.py": Take pandas dataframe, perform enrichment and return pandas frame
- "ingest_bulk.py": Push the pandas dataframe to the Customer's CDP DLO using bulk API
- "ingest_stream.py": Push the pandas dataframe to the Customer's CDP DLO using streaming API

## Testing Pre-req
- Create a new Ingestion API connector (<a href="https://help.salesforce.com/s/articleView?id=sf.c360_a_connect_an_ingestion_source.htm&type=5"> developer doc</a>)   
> From CDP setup -> Configuration->Ingestion API->Click New  
Give it a name "External Lead". From the git repo's Config directory, upload lead.yaml as a schema 
- Create a new connected app (<a href="https://help.salesforce.com/s/articleView?id=sf.c360_a_create_ingestion_api_connected_app.htm&type=5"> developer doc</a>)  
> From Setup -> Apps->Apps Manager
Create a new data stream (<a href="https://help.salesforce.com/s/articleView?id=sf.c360_a_create_ingestion_data_stream.htm&type=5"> developer doc</a>)   
> From "Customer Data Platform" app, "Data Streams" tab, create a new data stream using the Ingestion API 



