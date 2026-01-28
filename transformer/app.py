import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("ProcessedDataTable")


PROCESSED_BUCKET = "gurdeep-processed-data"
ARCHIVE_BUCKET = "gurdeep-archive-data"


def handler(event, context):
    """
    Triggered by EventBridge after validation.
    Transforms data and moves files to processed & archive buckets.
    """

    logger.info("Received EventBridge event: %s", json.dumps(event))

    try:
        # 1. Extract details from EventBridge event
        detail = event.get("detail", {})
        source_bucket = detail.get("bucket")
        object_key = detail.get("key")

        if not source_bucket or not object_key:
            raise ValueError("Missing bucket or key in event detail")

        logger.info("Processing file: %s/%s", source_bucket, object_key)

        # 2. Read file from raw bucket
        response = s3.get_object(
            Bucket=source_bucket,
            Key=object_key
        )

        file_content = response["Body"].read().decode("utf-8")
        data = json.loads(file_content)

        # 3. Simple transformation (example)
        data["processed"] = True

        transformed_data = json.dumps(data)

        # 4. Write transformed file to processed bucket
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=object_key,
            Body=transformed_data
        )

        table.put_item(
            Item={
                "id": data["id"],
                "name": data["name"],
                "timestamp": data["timestamp"],
                "processed_at": context.aws_request_id,
                "source_bucket": object_key,
                }
        )



        logger.info("Written transformed file to %s", PROCESSED_BUCKET)

        # 5. Copy original file to archive bucket
        s3.copy_object(
            Bucket=ARCHIVE_BUCKET,
            CopySource={
                "Bucket": source_bucket,
                "Key": object_key
            },
            Key=object_key
        )

        # 6. Delete original file from raw bucket
        s3.delete_object(
            Bucket=source_bucket,
            Key=object_key
        )

        logger.info("Archived original file to %s", ARCHIVE_BUCKET)

        return {
            "status": "success",
            "file": object_key
        }

    except Exception as e:
        logger.exception("Transformer failed")
        raise e
