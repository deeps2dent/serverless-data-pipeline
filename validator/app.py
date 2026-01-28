import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
events = boto3.client("events")

REQUIRED_FIELDS = {"id", "timestamp", "name"}

def handler(event, context):
    logger.info(f"Received S3 event: {event}")

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # Only allow JSON files
    if not key.endswith(".json"):
        logger.warning(f"Rejected non-JSON file: {key}")
        return

    try:
        # Read file
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        data = json.loads(body)

        # Schema validation
        missing_fields = REQUIRED_FIELDS - data.keys()
        if missing_fields:
            logger.error(
                f"Validation failed for {key}. Missing fields: {missing_fields}"
            )
            return  # STOP pipeline here

        # Emit EventBridge event ONLY if valid
        events.put_events(
            Entries=[
                {
                    "Source": "custom.validation",
                    "DetailType": "ValidationCompleted",
                    "Detail": json.dumps({
                        "bucket": bucket,
                        "key": key
                    })
                }
            ]
        )

        logger.info(f"Validation successful for {key}. Event sent.")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON format: {key}")
    except Exception as e:
        logger.exception(f"Unexpected error processing {key}: {e}")
