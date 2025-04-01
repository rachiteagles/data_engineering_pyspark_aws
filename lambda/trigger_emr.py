import boto3
import json
import os

s3_client = boto3.client("s3")
emr_client = boto3.client("emr")
events_client = boto3.client("events")

S3_BUCKET = os.getenv("S3_BUCKET", "your-bucket-name")
S3_SUCCESS_PREFIX = "processed/success/"
S3_FAILED_PREFIX = "processed/failed/"
S3_SCRIPT_PATH = "s3://your-bucket-name/scripts/stock_etl.py"

def lambda_handler(event, context):
    """Triggered when a file is uploaded to S3."""
    try:
        # Extract S3 file info
        record = event["Records"][0]
        file_name = record["s3"]["object"]["key"]
        print(f"New file detected: {file_name}")

        # Create EMR Cluster
        response = emr_client.run_job_flow(
            Name="StockETLCluster",
            ReleaseLabel="emr-6.9.0",
            Applications=[{"Name": "Spark"}],
            Instances={
                "InstanceGroups": [
                    {"Name": "Master", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
                    {"Name": "Core", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2}
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False
            },
            LogUri=f"s3://{S3_BUCKET}/logs/",
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
        )
        cluster_id = response["JobFlowId"]
        print(f"Created EMR Cluster: {cluster_id}")

        # Submit Spark Job
        step_response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Stock Data ETL",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["spark-submit", S3_SCRIPT_PATH, f"s3://{S3_BUCKET}/{file_name}"]
                    },
                }
            ]
        )
        step_id = step_response["StepIds"][0]

        # Create CloudWatch Event Rule to monitor EMR job
        rule_name = f"MonitorEMR_{cluster_id}"
        events_client.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({
                "source": ["aws.emr"],
                "detail-type": ["EMR Step Status Change"],
                "detail": {"stepId": [step_id]}
            }),
            State="ENABLED"
        )
        
        # Add target Lambda function to the rule
        events_client.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1", "Arn": os.getenv("MONITOR_LAMBDA_ARN")}]
        )

        return {"statusCode": 200, "body": f"EMR Job started: {cluster_id}, Step: {step_id}"}

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
