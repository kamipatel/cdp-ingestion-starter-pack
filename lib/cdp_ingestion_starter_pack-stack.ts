import * as cdk from 'aws-cdk-lib';
import { Stack, StackProps, Duration, aws_events as events, aws_events_targets as eventTargets, aws_stepfunctions as sfn, aws_stepfunctions_tasks as tasks, aws_events_targets as targets, aws_iam as iam, CfnParameter, aws_s3 as s3, aws_logs as logs} from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as eventsTarget from 'aws-cdk-lib/aws-events-targets';
import console = require('console');
import * as path from 'path';
import { Construct } from 'constructs';

export class CdpIngestionStarterPackStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    /*********** S3 buckets  ***********/  
    const cdpIngestDataBucket = new s3.Bucket(this, 'cdp-ingest-data-bucket', {
      versioned: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL
    });
       
    /*********** where is functions buckets  ***********/  
    var tmplBucketName = 'cdp-pub-' + this.region;
    //tmplBucketName = "pi-pub-ap-northeast-1";
    console.log("tmplBucketName  is=" + tmplBucketName);
    const bucket = s3.Bucket.fromBucketName(this,tmplBucketName, tmplBucketName);
    console.log("template bucket is=" + bucket);
    const functionszip = 'ingestion-functions.zip';

    /*********** lambda layers  ***********/  
    const piAwsWranglerLayer = new lambda.LayerVersion(this, 'pi-aws-wrangler-layer', {
      compatibleRuntimes: [
        lambda.Runtime.PYTHON_3_8,
      ],      
      code: lambda.Code.fromBucket(bucket, "awswrangler-layer-2.10.0-py3.8.zip"),
      description: 'aws datawrangler library',
    });

    /*********** Lambbda functions  ***********/  
    const cdpQueryDataFunctionMemorySize = 2048;    
    const cdpQueryDataHandler = new lambda.Function(this, "cdp-query-data", {
      runtime: lambda.Runtime.PYTHON_3_8, 
      code: lambda.Code.fromAsset(path.join(__dirname, './../src')),      
      handler: "query.lambda_handler",
      environment: {
        STAGE_BUCKET: cdpIngestDataBucket.bucketName,
        REGION: this.region
      },
      functionName: 'cdp-query-data',
      layers: [piAwsWranglerLayer],
      timeout: Duration.minutes(14),      
      memorySize: cdpQueryDataFunctionMemorySize
    });      
    cdpIngestDataBucket.grantRead(cdpQueryDataHandler); 
    
    const cdpEnrichDataFunctionMemorySize = 2048;    
    const cdpEnrichDataHandler = new lambda.Function(this, "cdp-enrich-data", {
      runtime: lambda.Runtime.PYTHON_3_8, 
      code: lambda.Code.fromAsset(path.join(__dirname, './../src')),      
      handler: "enrich.lambda_handler",
      environment: {
        STAGE_BUCKET: cdpIngestDataBucket.bucketName,
        OUT_BUCKET: cdpIngestDataBucket.bucketName,
        REGION: this.region
      },
      functionName: 'cdp-enrich-data',
      layers: [piAwsWranglerLayer],
      timeout: Duration.minutes(14),      
      memorySize: cdpQueryDataFunctionMemorySize
    });      
    cdpIngestDataBucket.grantRead(cdpEnrichDataHandler); 
    cdpIngestDataBucket.grantRead(cdpEnrichDataHandler); 

    const cdpIngestBulkDataFunctionMemorySize = 2048;    
    const cdpIngestBulkDataHandler = new lambda.Function(this, "cdp-ingest-bulk-data", {
      runtime: lambda.Runtime.PYTHON_3_8, 
      code: lambda.Code.fromAsset(path.join(__dirname, './../src')),      
      handler: "ingest-bulk.lambda_handler",
      environment: {
        OUT_BUCKET: cdpIngestDataBucket.bucketName,
        REGION: this.region
      },
      functionName: 'cdp-ingest-bulk-data',
      layers: [piAwsWranglerLayer],
      timeout: Duration.minutes(14),      
      memorySize: cdpIngestBulkDataFunctionMemorySize
    });      
    cdpIngestDataBucket.grantRead(cdpIngestBulkDataHandler); 

    const cdpIngestStreamDataFunctionMemorySize = 2048;    
    const cdpIngestStreamDataHandler = new lambda.Function(this, "cdp-ingest-stream-data", {
      runtime: lambda.Runtime.PYTHON_3_8, 
      code: lambda.Code.fromAsset(path.join(__dirname, './../src')),      
      handler: "ingest-stream.lambda_handler",
      environment: {
        OUT_BUCKET: cdpIngestDataBucket.bucketName,
        REGION: this.region
      },
      functionName: 'cdp-ingest-stream-data',
      layers: [piAwsWranglerLayer],
      timeout: Duration.minutes(14),      
      memorySize: cdpIngestStreamDataFunctionMemorySize
    });      
    cdpIngestDataBucket.grantRead(cdpIngestStreamDataHandler); 

  }
}
