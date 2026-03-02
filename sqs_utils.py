import boto3


class SQSClient:
    def __init__(self, boto_config, queue_url, long_poll_seconds):
        self.queue_url = queue_url
        self.long_poll_seconds = long_poll_seconds
        self.client = boto3.client("sqs", config=boto_config)

    def receive_message(self):
        return self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=min(max(self.long_poll_seconds, 0), 20),
            VisibilityTimeout=60,
        )

    def delete_message(self, receipt_handle):
        self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
        )
