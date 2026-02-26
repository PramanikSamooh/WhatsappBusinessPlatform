"""MinIO/S3 Media Storage Abstraction

Handles upload, download, presigned URL generation, and lifecycle policy
for WhatsApp media files and call recordings stored in MinIO.

If MINIO_ENDPOINT is not set, all operations gracefully return None/False,
allowing the application to fall back to its previous behavior.
"""

import os
from datetime import date
from io import BytesIO

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger

# Configuration from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "whatsapp-media")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

PRESIGNED_URL_EXPIRY = 3600  # 1 hour
LIFECYCLE_EXPIRY_DAYS = int(os.getenv("MEDIA_EXPIRY_DAYS", "7"))

_s3_client = None


def is_configured() -> bool:
    """Check if MinIO is configured."""
    return bool(MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY)


def get_client():
    """Get or create the boto3 S3 client (lazy singleton)."""
    global _s3_client
    if _s3_client is None:
        if not is_configured():
            return None
        _s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
            config=Config(signature_version="s3v4"),
        )
    return _s3_client


def init_bucket():
    """Create the bucket if it does not exist, and apply lifecycle expiry rule.

    Called once at server startup from the lifespan handler.
    """
    client = get_client()
    if not client:
        logger.info("MinIO not configured, skipping bucket initialization")
        return

    # Create bucket if missing
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
        logger.info(f"MinIO bucket '{MINIO_BUCKET}' exists")
    except ClientError:
        try:
            client.create_bucket(Bucket=MINIO_BUCKET)
            logger.info(f"MinIO bucket '{MINIO_BUCKET}' created")
        except ClientError as e:
            logger.error(f"Failed to create MinIO bucket: {e}")
            return

    # Apply lifecycle expiration rule
    try:
        client.put_bucket_lifecycle_configuration(
            Bucket=MINIO_BUCKET,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": f"auto-expire-{LIFECYCLE_EXPIRY_DAYS}d",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Expiration": {"Days": LIFECYCLE_EXPIRY_DAYS},
                    }
                ]
            },
        )
        logger.info(
            f"MinIO lifecycle rule set: expire after {LIFECYCLE_EXPIRY_DAYS} days"
        )
    except ClientError as e:
        logger.warning(f"Failed to set lifecycle rule: {e}")


def upload_bytes(
    key: str, data: bytes, content_type: str = "application/octet-stream",
) -> bool:
    """Upload binary data to MinIO.

    Args:
        key: S3 object key (e.g. 'media/2026-02-26/conv123/image.jpg')
        data: Raw bytes to upload.
        content_type: MIME type.

    Returns:
        True on success, False on failure.
    """
    client = get_client()
    if not client:
        return False
    try:
        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=BytesIO(data),
            ContentLength=len(data),
            ContentType=content_type,
        )
        logger.info(f"Uploaded to MinIO: {key} ({len(data)} bytes)")
        return True
    except ClientError as e:
        logger.error(f"MinIO upload failed for {key}: {e}")
        return False


def generate_presigned_url(key: str, expiry: int = PRESIGNED_URL_EXPIRY) -> str | None:
    """Generate a presigned GET URL for an object.

    Args:
        key: S3 object key.
        expiry: URL validity in seconds (default 1 hour).

    Returns:
        Presigned URL string, or None on failure.
    """
    if not key:
        return None
    client = get_client()
    if not client:
        return None
    try:
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": MINIO_BUCKET, "Key": key},
            ExpiresIn=expiry,
        )
        return url
    except ClientError as e:
        logger.error(f"Presigned URL generation failed for {key}: {e}")
        return None


def delete_object(key: str) -> bool:
    """Delete an object from MinIO."""
    if not key:
        return False
    client = get_client()
    if not client:
        return False
    try:
        client.delete_object(Bucket=MINIO_BUCKET, Key=key)
        logger.info(f"Deleted from MinIO: {key}")
        return True
    except ClientError as e:
        logger.error(f"MinIO delete failed for {key}: {e}")
        return False


def build_media_key(conv_id: str, filename: str) -> str:
    """Build a storage key for WhatsApp media.

    Pattern: media/{date}/{conv_id}/{filename}
    """
    today = date.today().isoformat()
    return f"media/{today}/{conv_id}/{filename}"


def build_recording_key(call_id: str) -> str:
    """Build a storage key for a call recording.

    Pattern: recordings/{call_id}.wav
    """
    return f"recordings/{call_id}.wav"
