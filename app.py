import os
import json
import time
import uuid
import threading
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from flask import (
    Flask,
    jsonify,
    render_template,
    request,
    session,
)

from bedrock_utils import BedrockClient
from s3_utils import S3Client
from sqs_utils import SQSClient

load_dotenv()

# ---------------------------
# Config (ENV)
# ---------------------------
S3_BUCKET = os.getenv("S3_BUCKET", "rag-class-docs-sassons")
S3_PREFIX = os.getenv("S3_PREFIX", "kb-data/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/418052138013/rag-class-docs-queue-sassons")

# Bedrock Knowledge Base ingestion
KB_ID = os.getenv("BEDROCK_KB_ID", "1Q1XOOHCIT")
KB_DATA_SOURCE_ID = os.getenv("BEDROCK_KB_DATA_SOURCE_ID", "JRIKD3UQR7")

# Bedrock runtime retrieve&generate (Knowledge Base)
BEDROCK_MODEL_ARN = os.getenv("BEDROCK_MODEL_ARN", "arn:aws:bedrock:us-east-1:418052138013:inference-profile/us.anthropic.claude-3-5-haiku-20241022-v1:0")

# Poll cadence requirement: every 1 minute
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# SQS long polling max is 20 seconds
SQS_LONG_POLL_SECONDS = int(os.getenv("SQS_LONG_POLL_SECONDS", "20"))

MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "25"))
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")

# Optional: if you want an extra throttle so you never start ingestions too frequently
MIN_SECONDS_BETWEEN_INGESTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_INGESTIONS", "60"))


# ---------------------------
# AWS clients
# ---------------------------
boto_cfg = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 10, "mode": "standard"},
)

s3 = S3Client(boto_cfg)
sqs = SQSClient(boto_cfg, SQS_QUEUE_URL, SQS_LONG_POLL_SECONDS)

# Bedrock clients
bedrock = BedrockClient(boto_cfg, KB_ID, KB_DATA_SOURCE_ID, BEDROCK_MODEL_ARN)
sts = boto3.client("sts", config=boto_cfg)

# ---------------------------
# App
# ---------------------------
app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024


# ---------------------------
# In-memory state (single-process)
# NOTE: For production/multi-instance, persist in Redis/DynamoDB.
# ---------------------------
@dataclass
class IngestionState:
    status: str  # "IDLE" | "IN_PROGRESS" | "FAILED"
    pending_change: bool = False  # <-- guard flag
    last_job_id: Optional[str] = None
    last_job_status: Optional[str] = None
    last_event_time: Optional[float] = None
    last_error: Optional[str] = None
    last_ingestion_start_time: Optional[float] = None


_state_lock = threading.Lock()
_ingestion_state = IngestionState(status="IDLE")

_files_cache_lock = threading.Lock()
_files_cache: List[Dict[str, Any]] = []
_files_cache_last_refresh: Optional[float] = None

_chat_lock = threading.Lock()
_chat_store: Dict[str, List[Dict[str, str]]] = {}  # cid -> [{role, text}, ...]


def _require_env() -> Tuple[bool, List[str]]:
    missing = []
    for key, val in [
        ("S3_BUCKET", S3_BUCKET),
        ("SQS_QUEUE_URL", SQS_QUEUE_URL),
        ("BEDROCK_KB_ID", KB_ID),
        ("BEDROCK_KB_DATA_SOURCE_ID", KB_DATA_SOURCE_ID),
        ("BEDROCK_MODEL_ARN", BEDROCK_MODEL_ARN),
    ]:
        if not val:
            missing.append(key)
    return len(missing) == 0, missing


def refresh_files_cache() -> None:
    global _files_cache, _files_cache_last_refresh
    files = s3.list_s3_files(S3_BUCKET, S3_PREFIX)
    with _files_cache_lock:
        _files_cache = files
        _files_cache_last_refresh = time.time()


def _can_start_new_ingestion() -> bool:
    if MIN_SECONDS_BETWEEN_INGESTIONS <= 0:
        return True
    with _state_lock:
        t = _ingestion_state.last_ingestion_start_time
    if not t:
        return True
    return (time.time() - t) >= MIN_SECONDS_BETWEEN_INGESTIONS


def start_kb_ingestion() -> Optional[str]:
    try:
        return bedrock.start_ingestion_job()
    except ClientError as e:
        set_ingestion_failed(str(e), job_status="START_FAILED")
        return None


def get_ingestion_job_status(job_id: str) -> Optional[str]:
    try:
        resp = bedrock.get_ingestion_job(job_id)
        job = resp.get("ingestionJob") or resp.get("job") or resp
        return job.get("status")
    except Exception:
        return None


def set_ingestion_in_progress(job_id: Optional[str]) -> None:
    with _state_lock:
        _ingestion_state.status = "IN_PROGRESS"
        _ingestion_state.last_job_id = job_id
        _ingestion_state.last_job_status = "STARTED"
        _ingestion_state.last_event_time = time.time()
        _ingestion_state.last_error = None
        _ingestion_state.last_ingestion_start_time = time.time()
        # when we start a job we are "consuming" the pending change
        _ingestion_state.pending_change = False


def set_ingestion_idle(job_status: Optional[str] = None) -> None:
    with _state_lock:
        _ingestion_state.status = "IDLE"
        if job_status:
            _ingestion_state.last_job_status = job_status
        _ingestion_state.last_event_time = time.time()
        _ingestion_state.last_error = None
        _ingestion_state.last_job_id = None


def set_ingestion_failed(err: str, job_status: Optional[str] = None) -> None:
    with _state_lock:
        _ingestion_state.status = "FAILED"
        _ingestion_state.last_error = err
        _ingestion_state.last_event_time = time.time()
        if job_status:
            _ingestion_state.last_job_status = job_status


def mark_pending_change() -> None:
    with _state_lock:
        # If ingestion is currently running, remember to run one more after it finishes
        _ingestion_state.pending_change = True
        _ingestion_state.last_event_time = time.time()


# ---------------------------
# SQS message parsing (best-effort)
# ---------------------------
def _extract_s3_event_records(body: str) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(body)
    except Exception:
        return []

    # SNS wrapper
    if isinstance(payload, dict) and "Message" in payload and isinstance(payload["Message"], str):
        try:
            payload = json.loads(payload["Message"])
        except Exception:
            return []

    records = payload.get("Records")
    if isinstance(records, list):
        return records
    return []


def _has_relevant_s3_change(records: List[Dict[str, Any]]) -> bool:
    for r in records:
        event_name = (r.get("eventName") or "").lower()
        if "objectcreated" in event_name or "objectremoved" in event_name:
            return True
    return False


# ---------------------------
# Single-ingestion guard core
# ---------------------------
def maybe_start_ingestion_or_mark_pending() -> None:
    """
    Called when we observed relevant S3 changes (via SQS).
    Implements:
      - If IDLE -> start ingestion (one job)
      - If IN_PROGRESS -> pending_change=True
      - If FAILED -> keep FAILED (optional: you can choose to mark pending too)
    """
    with _state_lock:
        status = _ingestion_state.status

    if status == "IN_PROGRESS":
        mark_pending_change()
        return

    if status == "FAILED":
        # Conservative: don't auto-start when failed; keep error visible.
        # If you prefer auto-recovery, treat FAILED like IDLE here.
        mark_pending_change()
        return

    # status is IDLE
    if not _can_start_new_ingestion():
        # If throttled, just mark pending so the next tick can start
        mark_pending_change()
        return

    job_id = start_kb_ingestion()
    if job_id:
        set_ingestion_in_progress(job_id)
    else:
        # start failed already set FAILED
        pass


def update_ingestion_progress() -> None:
    """
    Periodic status updater:
      - If IN_PROGRESS: check job status
         - If complete and pending_change: start one more ingestion
         - Else: set IDLE
    """
    with _state_lock:
        if _ingestion_state.status != "IN_PROGRESS":
            return
        job_id = _ingestion_state.last_job_id
        pending = _ingestion_state.pending_change

    if not job_id:
        # We don't have a job id; treat as failed.
        set_ingestion_failed("Missing ingestion job id.", job_status="UNKNOWN")
        return

    st = get_ingestion_job_status(job_id)
    if not st:
        # Can't read status this tick; keep IN_PROGRESS
        return

    # Normalize possible status values
    st_up = str(st).upper()

    if st_up in ("COMPLETE", "SUCCEEDED", "SUCCESS"):
        if pending:
            # Start exactly one extra ingestion to catch up
            if _can_start_new_ingestion():
                new_job = start_kb_ingestion()
                if new_job:
                    set_ingestion_in_progress(new_job)
                else:
                    # start failed -> FAILED
                    pass
            else:
                # throttled; keep pending so next tick can start
                mark_pending_change()
        else:
            set_ingestion_idle(job_status=st_up)
        return

    if st_up in ("FAILED", "ERROR", "STOPPED", "CANCELLED", "CANCELED"):
        set_ingestion_failed("Ingestion job failed.", job_status=st_up)
        return

    # Otherwise still running (e.g., IN_PROGRESS)
    with _state_lock:
        _ingestion_state.last_job_status = st_up
        _ingestion_state.last_event_time = time.time()


# ---------------------------
# Background worker: poll SQS every minute (with long polling)
# ---------------------------
_worker_started = False


def sqs_poller_loop() -> None:
    # Initial refresh
    try:
        refresh_files_cache()
    except Exception:
        pass

    while True:
        tick_start = time.time()

        # 1) always update ingestion progress each tick
        try:
            update_ingestion_progress()
        except Exception:
            # don't crash worker
            pass

        # 2) pull messages from SQS (long poll)
        saw_relevant_change = False
        received_msgs: List[Dict[str, Any]] = []

        try:
            resp = sqs.receive_message()
            received_msgs = resp.get("Messages", []) or []

            if received_msgs:
                # determine relevance
                for m in received_msgs:
                    recs = _extract_s3_event_records(m.get("Body", ""))
                    if _has_relevant_s3_change(recs):
                        saw_relevant_change = True
                        break

                # 3) delete (ack) messages so they won't re-trigger
                for m in received_msgs:
                    try:
                        sqs.delete_message(m["ReceiptHandle"])
                    except Exception:
                        # If delete fails, message may reappear later (at-least-once delivery).
                        # Guard + optional dedupe still keeps ingestion safe.
                        pass

                # 4) refresh file list after seeing events
                try:
                    refresh_files_cache()
                except Exception:
                    pass

                # 5) apply single-ingestion guard
                if saw_relevant_change:
                    maybe_start_ingestion_or_mark_pending()

        except ClientError as e:
            set_ingestion_failed(str(e), job_status="SQS_ERROR")
        except Exception as e:
            set_ingestion_failed(str(e), job_status="WORKER_ERROR")

        # Enforce 1-minute cadence
        elapsed = time.time() - tick_start
        time.sleep(max(0.0, POLL_INTERVAL_SECONDS - elapsed))


def start_worker_once() -> None:
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
    t = threading.Thread(target=sqs_poller_loop, daemon=True)
    t.start()


@app.before_request
def _ensure_worker() -> None:
    start_worker_once()
    if "cid" not in session:
        session["cid"] = str(uuid.uuid4())
        with _chat_lock:
            _chat_store[session["cid"]] = []


# ---------------------------
# UI route
# ---------------------------
@app.get("/")
def index():
    ok, missing = _require_env()
    return render_template("index.html", env_ok=ok, missing=missing, bucket=S3_BUCKET)


# ---------------------------
# API: status + files
# ---------------------------
@app.get("/health")
def api_status():
    with _state_lock:
        st = asdict(_ingestion_state)
    with _files_cache_lock:
        last_refresh = _files_cache_last_refresh
        file_count = len(_files_cache)
    st["files_cached"] = file_count
    st["files_last_refresh"] = last_refresh
    return jsonify(st)


@app.get("/files")
def api_files():
    force = request.args.get("force") == "1"
    if force:
        refresh_files_cache()
    with _files_cache_lock:
        return jsonify(
            {
                "bucket": S3_BUCKET,
                "files": _files_cache,
                "last_refresh": _files_cache_last_refresh,
            }
        )


# ---------------------------
# API: upload + delete
# ---------------------------
@app.post("/upload")
def api_upload():
    if "file" not in request.files:
        return jsonify({"error": "Missing file field"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400

    key = S3_PREFIX + (request.form.get("key") or f.filename)

    try:
        s3.upload_file_object(S3_BUCKET, key, f)
    except ClientError as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"ok": True, "key": key})


@app.post("/delete")
def api_delete():
    data = request.get_json(silent=True) or {}
    key = data.get("key")
    if not key:
        return jsonify({"error": "Missing key"}), 400
    try:
        s3.delete_object(S3_BUCKET, key)
    except ClientError as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


# ---------------------------
# API: chat (RetrieveAndGenerate) + history
# ---------------------------
def _get_chat_history() -> List[Dict[str, str]]:
    cid = session.get("cid")
    if not cid:
        return []
    with _chat_lock:
        return list(_chat_store.get(cid, []))


def _append_chat(role: str, text: str) -> None:
    cid = session.get("cid")
    if not cid:
        return
    with _chat_lock:
        _chat_store.setdefault(cid, []).append({"role": role, "text": text})


@app.get("/history")
def api_history():
    return jsonify({"history": _get_chat_history()})


@app.post("/ask")
def api_chat():
    data = request.get_json(silent=True) or {}
    prompt = (data.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "Empty prompt"}), 400

    _append_chat("user", prompt)

    try:
        resp = bedrock.retrieve_and_generate(prompt)

        output = resp.get("output") or {}
        answer = output.get("text") or ""

        if not answer:
            answer = json.dumps(resp, indent=2)[:2000]

    except ClientError as e:
        answer = f"Bedrock error: {e}"
    except Exception as e:
        answer = f"Unexpected error: {e}"

    _append_chat("assistant", answer)
    return jsonify({"answer": answer, "history": _get_chat_history()})


@app.get("/debug")
def api_debug():
    ident = sts.get_caller_identity()
    return jsonify({
        "aws_region": AWS_REGION,
        "caller_account": ident.get("Account"),
        "caller_arn": ident.get("Arn"),
        "kb_id": KB_ID,
        "data_source_id": KB_DATA_SOURCE_ID,
        "model_arn": BEDROCK_MODEL_ARN,
        "s3_bucket": S3_BUCKET,
        "sqs_queue_url": SQS_QUEUE_URL,
    })


@app.get("/whoami")
def api_whoami():
    ident = sts.get_caller_identity()
    return jsonify({
        "account": ident.get("Account"),
        "arn": ident.get("Arn"),
        "userId": ident.get("UserId"),
        "region": AWS_REGION,
        "kb_id": KB_ID,
        "data_source_id": KB_DATA_SOURCE_ID,
        "model_arn": BEDROCK_MODEL_ARN,
    })

@app.post("/retrieve")
def api_retrieve():
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"error": "Empty query"}), 400

    resp = bedrock.retrieve(query)

    results = resp.get("retrievalResults") or []
    out = []
    for r in results:
        content = (r.get("content") or {}).get("text") or ""
        location = r.get("location") or {}
        score = r.get("score")
        out.append({
            "score": score,
            "location": location,
            "snippet": content[:300],
        })

    return jsonify({"count": len(out), "results": out})

@app.get("/kb/documents")
def api_kb_documents():
    # optional: ?limit=200
    try:
        limit = int(request.args.get("limit", "200"))
    except ValueError:
        limit = 200
    limit = max(1, min(limit, 1000))

    try:
        docs = bedrock.list_kb_documents(max_results=limit)
    except ClientError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {e}"}), 500

    # normalize for UI
    out = []
    for d in docs:
        ident = d.get("identifier") or {}
        s3 = (ident.get("s3") or {})
        out.append({
            "status": d.get("status"),
            "statusReason": d.get("statusReason"),
            "updatedAt": (d.get("updatedAt").isoformat() if hasattr(d.get("updatedAt"), "isoformat") else d.get("updatedAt")),
            "dataSourceType": ident.get("dataSourceType"),
            "uri": s3.get("uri") or None,
            "customId": ((ident.get("custom") or {}).get("id") if isinstance(ident.get("custom"), dict) else None),
        })

    return jsonify({"count": len(out), "documents": out})

# ---------------------------
# Run
# ---------------------------
if __name__ == "__main__":
    ok, missing = _require_env()
    if not ok:
        print("Missing required env vars:", ", ".join(missing))
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)