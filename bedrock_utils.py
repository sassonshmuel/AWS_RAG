from typing import List, Dict, Any, Optional

import boto3
import time

class BedrockClient:
    def __init__(self, boto_cfg, kb_id, kb_data_source_id, model_arn):
        self.kb_id = kb_id
        self.kb_data_source_id = kb_data_source_id
        self.model_arn = model_arn
        self._bedrock_agent = boto3.client("bedrock-agent", config=boto_cfg)
        self._bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", config=boto_cfg)


    def start_ingestion_job(self):
        resp = self._bedrock_agent.start_ingestion_job(
            knowledgeBaseId=self.kb_id,
            dataSourceId=self.kb_data_source_id,
        )
        return resp["ingestionJob"]["ingestionJobId"]

    def get_ingestion_job(self, job_id):
        return self._bedrock_agent.get_ingestion_job(
            knowledgeBaseId=self.kb_id,
            dataSourceId=self.kb_data_source_id,
            ingestionJobId=job_id,
        )

    def wait_for_ingestion(self, ingestion_job_id: str, poll_seconds: int = 10) -> None:
        while True:
            resp = self._bedrock_agent.get_ingestion_job(
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
        return self._bedrock_agent_runtime.retrieve_and_generate(
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
        return self._bedrock_agent_runtime.retrieve(
            knowledgeBaseId=self.kb_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": 5}
            },
        )

    def list_kb_documents(self, max_results: int = 200) -> List[Dict[str, Any]]:
        """
        Lists documents in the KB data source (document-level view).
        Uses ListKnowledgeBaseDocuments. :contentReference[oaicite:1]{index=1}
        """
        docs: List[Dict[str, Any]] = []
        token: Optional[str] = None

        while True:
            kwargs = {
                "knowledgeBaseId": self.kb_id,
                "dataSourceId": self.kb_data_source_id,
                "maxResults": min(max_results, 1000),
            }
            if token:
                kwargs["nextToken"] = token

            resp = self._bedrock_agent.list_knowledge_base_documents(**kwargs)
            docs.extend(resp.get("documentDetails", []) or [])

            token = resp.get("nextToken")
            if not token:
                break

            # safety guard to avoid huge loops if someone sets max_results tiny
            if len(docs) >= max_results:
                docs = docs[:max_results]
                break

        return docs