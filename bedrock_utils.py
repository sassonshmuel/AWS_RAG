import boto3
import time

class BedrockClient:
    def __init__(self, boto_cfg, kb_id, kb_data_source_id, model_arn):
        self.kb_id = kb_id
        self.kb_data_source_id = kb_data_source_id
        self.model_arn = model_arn
        self.bedrock_agent = boto3.client("bedrock-agent", config=boto_cfg)
        self.bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", config=boto_cfg)


    # def start_ingestion(self, knowledge_base_id, data_source_id, description: str = "sync new docs") -> str:
    #     resp = self.bedrock_agent.start_ingestion_job(
    #         knowledgeBaseId=knowledge_base_id,
    #         dataSourceId=data_source_id,
    #         description=description,
    #     )
    #     return resp["ingestionJob"]["ingestionJobId"]

    def start_ingestion_job(self):
        return self.bedrock_agent.start_ingestion_job(
            knowledgeBaseId=self.kb_id,
            dataSourceId=self.kb_data_source_id,
        )

    def get_ingestion_job(self, job_id):
        return self.bedrock_agent.get_ingestion_job(
            knowledgeBaseId=self.kb_id,
            dataSourceId=self.kb_data_source_id,
            ingestionJobId=job_id,
        )

    def wait_for_ingestion(self, ingestion_job_id: str, poll_seconds: int = 10) -> None:
        while True:
            resp = self.bedrock_agent.get_ingestion_job(
                knowledgeBaseId=self.kb_id,
                dataSourceId=self.kb_data_source_id,
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

    def retrieve_and_generate(self, prompt):
        return self.bedrock_agent_runtime.retrieve_and_generate(
            input={"text": prompt},
            retrieveAndGenerateConfiguration={
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {
                    "knowledgeBaseId": self.kb_id,
                    "modelArn": self.model_arn,
                },
            },
        )

    def retrieve(self, query):
        return self.bedrock_agent_runtime.retrieve(
            knowledgeBaseId=self.kb_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": 5}
            },
        )
# example:
# job_id = start_ingestion("ingest after upload")
# wait_for_ingestion(job_id)
# print("Ingestion complete.")
