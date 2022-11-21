import os
import random
import boto3
import pandas as pd
import json
from io import StringIO
from datetime import datetime
import logging

session = boto3.Session()
s3 = session.resource('s3')

# Run python3 test/ingest_fake_data.py

def lambda_handler(event, context):
    print('Ingest fake data called')
    try:
        
        total_records = 1000
        state = ["NY", "TX", "CT", "CA"]
        gender = ["Male", "Female"]
        source = ["Website", "InPerson", "Phone"]

        leads = []
        for i in range(total_records):             
            a = {
                "first_name":'fname-' + str(i+1),
                "last_name": 'lname-' + str(i+1),
                "email": 'customer-' + str(i+1) + '@test.cdpdata',
                "state":random.choice(state),
                "source":random.choice(source),
            }
            leads.append(a)

        df = pd.DataFrame(leads)

        df.to_csv("./test/data/fake.csv", index=False)
                    
        print('Ingest fake data complete')

    except Exception as e: 
        print('Ingest exception: '+ str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Ingest fake data complete!')
    }


lambda_handler({}, {})