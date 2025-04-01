import boto3
import json
import os

s3_client = boto3.client("s3")
emr_client = boto3.client("emr")

S3_BUCKET = os.getenv("S3_BUCKET", "your-bucket-name")
S3_SUCCESS_PREFIX = "processed/success/"
S3_FAILED_PREFIX = "processed/failed/"

def lambda_handler(event, context):
    """Triggered by CloudWatch Event to monitor EMR job status."""
    try:
        detail = event["detail"]
        cluster_id = detail["clusterId"]
        step_id = detail["stepId"]
        state = detail["state"]
        
        print(f"Monitoring EMR Step: {step_id} on Cluster: {cluster_id}, Status: {state}")

        if state in ["COMPLETED"]:
            move_file("success")
            terminate_cluster(cluster_id)
        elif state in ["FAILED", "CANCELLED"]:
            move_file("failed")
            terminate_cluster(cluster_id)

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def move_file(status):
    """Move file to success or failed folder."""
    src_key = os.getenv("S3_FILE_PATH")
    dest_key = f"{S3_SUCCESS_PREFIX if status == 'success' else S3_FAILED_PREFIX}{src_key.split('/')[-1]}"
    
    s3_client.copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": src_key}, Key=dest_key)
    s3_client.delete_object(Bucket=S3_BUCKET, Key=src_key)
    print(f"File moved to: {dest_key}")

def terminate_cluster(cluster_id):
    """Terminate EMR cluster."""
    emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f"Terminated EMR Cluster: {cluster_id}")
