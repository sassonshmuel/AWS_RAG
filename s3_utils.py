import os
from typing import List, Dict, Any

import boto3

class S3Client:
    def __init__(self, boto_config):
        self.client = boto3.client("s3", config=boto_config)

    def upload_file(self, bucket, prefix, local_path: str, key_name: str | None = None) -> str:
        filename = os.path.basename(local_path)
        key = key_name or f"{prefix}{filename}"
        self.client.upload_file(local_path, bucket, key)
        return key

    def upload_file_object(self, bucket, key, file_object):
        self.client.upload_fileobj(
            Fileobj=file_object,
            Bucket=bucket,
            Key=key,
        )

    def list_contents(self, bucket, prefix):
        resp = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for item in resp.get("Contents", []):
            print(item["Key"], item["Size"])
        return resp.get("Contents", [])

    def list_s3_files(self, bucket, prefix) -> List[Dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        items: List[Dict[str, Any]] = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                items.append(
                    {
                        "key": obj["Key"],
                        "size": int(obj.get("Size", 0)),
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )

        items.sort(key=lambda x: x["last_modified"], reverse=True)
        return items

    def delete_object(self, bucket, key):
        print(f"Deleting object {key} from bucket {bucket}")
        self.client.delete_object(Bucket=bucket, Key=key)