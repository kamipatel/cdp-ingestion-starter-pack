## Disclaimer
This is not an official repo. It is just for the general guidance. AWS stack in this blog post is provided as-is, use it as a general guidance. For AWS you are responsible for everything including all the expenses incurred and security aspects. 

## About
> CDP Ingestion starter pack solution for ISVs is designed to run in your AWS environment. It is completely optional to use this for your implementation, you can take this code, repurpose and run it anywhere to suit your needs.
> Demo use-case: Partner retrieves the athlete data from customer CDP's DLO (Data Lake Object), enriches that with a score and pushes the enriched data back to the customer CDP's DLO   

## Summary
- This AWS stack provides a boiler plate code for calling CDP ingestion APIs. It leverages code from existing open source <a herf="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_python_connector.htm"> python connector </a>

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
- "query.py": Query the data from customer's DLO (Data Lake object) and store in the S3 bucket 
- "enrich.py": Enrich the S3 bucket data (coming from customer's CDP org) and store it in a seperate folder so either customer's CDP S3 connector can pull it or use bulk API to push the data to CDP
- "ingest_bulk.py": Push the data from the S3 bucket to the Customer's CDP DLO using bulk API
- "ingest_stream.py": Push the data from the S3 bucket to the Customer's CDP DLO using streaming API

## Testing 
> Follow this <a href="https://developer.salesforce.com/docs/atlas.en-us.c360a_api.meta/c360a_api/c360a_api_get_started.htm">doc</a> and setup 
- Setup Ingestion API connector: defines the endpoint and payload to consume the data.
- Create and Deploy Data Stream: configures ingestion jobs and exposes the API for external consumption.
For the sample data stream, leverage the yaml athlete.file from data
- Configure Connected App: enables external applications to integrate with Salesforce using OAuth.
- Request Customer Data Platform access token from Salesforce OAuth Authorization Flows: implements OAuth dance to obtain and refresh access token required for making API requests.
- Update test/ingest_test.py event parameter with the information from above step
- For running test/ingest_test.py on local machine, install python3 and pandas libraries

> Run test/ingest_test.py from local machine which will call query, enrich, ingest_bulk functions. 
> Run Lambda functions from AWS Lambda console 

