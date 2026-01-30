import json
import boto3
import os
import uuid
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RAW_BUCKET = os.environ["RAW_BUCKET"]

def handler(event, context):
    logger.info("Received API event: %s", json.dumps(event))

    try:
        body = event.get("body")

        if not body:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Request body is required"})
            }

        data = json.loads(body)

        # Basic validation (API-level, not business validation)
        if "id" not in data or "name" not in data:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required fields: id, name"})
            }

        # Add ingestion metadata
        data["ingested_at"] = datetime.now(timezone.utc).isoformat()

        object_key = f"{uuid.uuid4()}.json"

        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=object_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "File ingested successfully",
                "object_key": object_key
            })
        }

    except Exception as e:
        logger.exception("Ingestion failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }
