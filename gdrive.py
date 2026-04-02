"""Google Drive Folder Image Listing

Lists image files from a Google Drive folder and generates
public viewable links for use as WhatsApp template header images.

Uses the same service account credentials as sheets_lookup.py
(GOOGLE_SHEETS_CREDENTIALS_JSON env var).
"""

import asyncio
import json
import os

from loguru import logger

CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")

DRIVE_CONFIGURED = bool(CREDENTIALS_PATH)

# Supported image MIME types
_IMAGE_MIMES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "image/bmp", "image/tiff",
}


def _get_drive_service():
    """Create Google Drive API service using service account credentials."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        logger.error("google-auth or google-api-python-client not installed")
        return None

    try:
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        if os.path.isfile(CREDENTIALS_PATH):
            creds = service_account.Credentials.from_service_account_file(
                CREDENTIALS_PATH, scopes=scopes,
            )
        else:
            info = json.loads(CREDENTIALS_PATH)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=scopes,
            )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to create Drive service: {e}")
        return None


def _list_folder_images(folder_id: str) -> list[dict]:
    """List all image files from a Google Drive folder (synchronous).

    The folder must be shared with the service account email.

    Returns list of dicts: {name, image_url, thumbnail, file_id, sort_key}
    sorted by filename (numeric sort if filenames are numbers).
    """
    service = _get_drive_service()
    if not service:
        raise RuntimeError("Google Drive API not configured or credentials invalid")

    try:
        results = []
        page_token = None
        query = f"'{folder_id}' in parents and trashed = false"

        while True:
            resp = service.files().list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, thumbnailLink)",
                pageSize=500,
                pageToken=page_token,
                orderBy="name",
            ).execute()

            for f in resp.get("files", []):
                mime = f.get("mimeType", "")
                if mime not in _IMAGE_MIMES:
                    continue

                file_id = f["id"]
                name = f.get("name", "")
                image_url = f"https://drive.google.com/uc?export=view&id={file_id}"
                thumbnail = f.get("thumbnailLink", "")

                # Extract sort key from filename (numeric if possible)
                name_without_ext = name.rsplit(".", 1)[0] if "." in name else name
                try:
                    sort_key = int(name_without_ext)
                except ValueError:
                    sort_key = 0

                results.append({
                    "file_id": file_id,
                    "name": name,
                    "sort_key": sort_key,
                    "image_url": image_url,
                    "thumbnail": thumbnail,
                })

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        # Sort by sort_key (numeric filename), then by name
        results.sort(key=lambda x: (x["sort_key"], x["name"]))

        logger.info(f"Listed {len(results)} images from Drive folder {folder_id}")
        return results

    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg or "notFound" in error_msg:
            raise RuntimeError(f"Folder not found. Make sure the folder is shared with the service account.")
        if "403" in error_msg or "forbidden" in error_msg:
            raise RuntimeError(f"Access denied. Share the folder with the service account email.")
        raise RuntimeError(f"Drive API error: {error_msg}")


async def list_folder_images(folder_id: str) -> list[dict]:
    """Async wrapper for listing folder images."""
    return await asyncio.to_thread(_list_folder_images, folder_id)
