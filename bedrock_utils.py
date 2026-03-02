import os
import boto3
import time


# REGION = os.environ.get("AWS_REGION", "us-east-1")
# # REGION = "us-east-1"
# KNOWLEDGE_BASE_ID = "XXXXXXXXXX"
# DATA_SOURCE_ID = "YYYYYYYYYY"
#
# S3_BUCKET = "rag-class-docs-sassons"
# S3_PREFIX = "kb-data/"  # must match your data source prefix (or be under it)


# def create_s3_client(region_name = REGION):
#     return boto3.client("s3", region_name=region_name)
#
#
# def upload_file(s3, local_path: str, key_name: str | None = None) -> str:
#     filename = os.path.basename(local_path)
#     key = key_name or f"{S3_PREFIX}{filename}"
#     s3.upload_file(local_path, S3_BUCKET, key)
#     return key


# Example
# key = upload_file("./docs/handbook.pdf")
# print("Uploaded to s3://%s/%s" % (S3_BUCKET, key))

def create_bedrock_client(region_name):
    return boto3.client("bedrock-agent", region_name=region_name)


def start_ingestion(bedrock_agent, knowledge_base_id, data_source_id, description: str = "sync new docs") -> str:
    resp = bedrock_agent.start_ingestion_job(
        knowledgeBaseId=knowledge_base_id,
        dataSourceId=data_source_id,
        description=description,
    )
    return resp["ingestionJob"]["ingestionJobId"]


def wait_for_ingestion(bedrock_agent, knowledge_base_id: str, data_source_id: str, ingestion_job_id: str, poll_seconds: int = 10) -> None:
    while True:
        resp = bedrock_agent.get_ingestion_job(
            knowledgeBaseId=knowledge_base_id,
            dataSourceId=data_source_id,
            ingestionJobId=ingestion_job_id,
        )
        job = resp["ingestionJob"]
        status = job["status"]  # e.g. STARTING/RUNNING/COMPLETE/FAILED
        print("Ingestion status:", status)
        if status in ("COMPLETE", "FAILED"):
            if status == "FAILED":
                # job often includes failure reasons in fields like 'failureReasons' depending on API version
                raise RuntimeError(f"Ingestion FAILED: {job}")
            return
        time.sleep(poll_seconds)

# example:
# job_id = start_ingestion("ingest after upload")
# wait_for_ingestion(job_id)
# print("Ingestion complete.")
