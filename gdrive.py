"""Google Drive Folder Image Listing

Lists image files from a public Google Drive folder and generates
viewable links for use as WhatsApp template header images.

Uses GOOGLE_API_KEY for public folders (no service account needed).
Falls back to service account credentials if API key is not set.
"""

import asyncio
import json
import os

import aiohttp
from loguru import logger

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")

DRIVE_CONFIGURED = bool(GOOGLE_API_KEY) or bool(CREDENTIALS_PATH)

# Supported image MIME types
_IMAGE_MIMES = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "image/bmp", "image/tiff",
}

_DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"


async def list_folder_images(folder_id: str) -> list[dict]:
    """List all image files from a public Google Drive folder.

    Uses simple API key auth — no service account needed for public folders.

    Returns list of dicts: {name, image_url, thumbnail, file_id, sort_key}
    sorted by filename (numeric sort if filenames are numbers).
    """
    if GOOGLE_API_KEY:
        return await _list_with_api_key(folder_id)
    elif CREDENTIALS_PATH:
        return await asyncio.to_thread(_list_with_service_account, folder_id)
    else:
        raise RuntimeError("Google API not configured. Set GOOGLE_API_KEY env var.")


async def _list_with_api_key(folder_id: str) -> list[dict]:
    """List folder images using simple API key (for public folders)."""
    from whatsapp_messaging import _get_session

    results = []
    page_token = ""
    query = f"'{folder_id}' in parents and trashed = false"

    try:
        session = await _get_session()

        while True:
            params = {
                "q": query,
                "fields": "nextPageToken,files(id,name,mimeType,thumbnailLink)",
                "pageSize": "500",
                "orderBy": "name",
                "key": GOOGLE_API_KEY,
            }
            if page_token:
                params["pageToken"] = page_token

            async with session.get(_DRIVE_FILES_URL, params=params) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    if "404" in body or "notFound" in body:
                        raise RuntimeError("Folder not found. Make sure the folder is public (Anyone with the link).")
                    if "403" in body or "forbidden" in body.lower():
                        raise RuntimeError("Access denied. Make sure the folder is public (Anyone with the link).")
                    raise RuntimeError(f"Drive API error ({resp.status}): {body[:200]}")

                data = await resp.json()

            for f in data.get("files", []):
                mime = f.get("mimeType", "")
                if mime not in _IMAGE_MIMES:
                    continue

                file_id = f["id"]
                name = f.get("name", "")
                image_url = f"https://lh3.googleusercontent.com/d/{file_id}"
                thumbnail = f"https://lh3.googleusercontent.com/d/{file_id}=s200"

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

            page_token = data.get("nextPageToken", "")
            if not page_token:
                break

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Drive API error: {e}")

    results.sort(key=lambda x: (x["sort_key"], x["name"]))
    logger.info(f"Listed {len(results)} images from Drive folder {folder_id}")
    return results


def _list_with_service_account(folder_id: str) -> list[dict]:
    """Fallback: list folder images using service account (for private folders)."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise RuntimeError("google-auth or google-api-python-client not installed")

    try:
        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        if os.path.isfile(CREDENTIALS_PATH):
            creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scopes)
        else:
            info = json.loads(CREDENTIALS_PATH)
            creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        service = build("drive", "v3", credentials=creds)
    except Exception as e:
        raise RuntimeError(f"Failed to create Drive service: {e}")

    results = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"

    try:
        while True:
            resp = service.files().list(
                q=query,
                fields="nextPageToken,files(id,name,mimeType,thumbnailLink)",
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
                image_url = f"https://lh3.googleusercontent.com/d/{file_id}"
                thumbnail = f"https://lh3.googleusercontent.com/d/{file_id}=s200"

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
    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg or "notFound" in error_msg:
            raise RuntimeError("Folder not found. Share the folder with the service account.")
        if "403" in error_msg or "forbidden" in error_msg:
            raise RuntimeError("Access denied. Share the folder with the service account email.")
        raise RuntimeError(f"Drive API error: {error_msg}")

    results.sort(key=lambda x: (x["sort_key"], x["name"]))
    logger.info(f"Listed {len(results)} images from Drive folder {folder_id}")
    return results
