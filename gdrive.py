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

_PDF_MIME = "application/pdf"


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


async def list_folder_pdfs(folder_id: str) -> list[dict]:
    """List all PDF files in a public Google Drive folder.

    Returns a list of dicts: {file_id, name, size}.
    """
    if not GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not configured; cannot list PDFs from Drive.")

    from whatsapp_messaging import _get_session

    results = []
    page_token = ""
    query = f"'{folder_id}' in parents and mimeType='{_PDF_MIME}' and trashed=false"

    try:
        session = await _get_session()
        while True:
            params = {
                "q": query,
                "fields": "nextPageToken,files(id,name,size)",
                "pageSize": "1000",
                "orderBy": "name",
                "key": GOOGLE_API_KEY,
            }
            if page_token:
                params["pageToken"] = page_token

            async with session.get(_DRIVE_FILES_URL, params=params) as resp:
                body = await resp.text()
                if resp.status != 200:
                    if "notFound" in body or resp.status == 404:
                        raise RuntimeError("Drive folder not found. Make it 'Anyone with the link'.")
                    if "forbidden" in body.lower() or resp.status == 403:
                        raise RuntimeError("Drive folder access denied. Make it 'Anyone with the link'.")
                    raise RuntimeError(f"Drive API error ({resp.status}): {body[:200]}")
                try:
                    data = json.loads(body)
                except (ValueError, TypeError):
                    raise RuntimeError(f"Drive API returned non-JSON: {body[:200]}")

            for f in data.get("files", []):
                results.append({
                    "file_id": f["id"],
                    "name": f.get("name", ""),
                    "size": int(f["size"]) if f.get("size") and str(f["size"]).isdigit() else 0,
                })

            page_token = data.get("nextPageToken", "")
            if not page_token:
                break
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Drive API error: {e}")

    logger.info(f"Listed {len(results)} PDFs from Drive folder {folder_id}")
    return results


async def download_pdf(file_id: str, max_bytes: int = 100 * 1024 * 1024) -> bytes:
    """Download a PDF from Google Drive by file_id.

    Verifies the response Content-Type is application/pdf (refuses the
    HTML virus-scan interstitial that Drive serves for very large files).
    """
    if not GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not configured")

    from whatsapp_messaging import _get_session

    url = f"{_DRIVE_FILES_URL}/{file_id}"
    params = {"alt": "media", "key": GOOGLE_API_KEY}
    headers = {"Accept": "application/pdf"}

    last_err = ""
    for attempt in range(5):
        try:
            session = await _get_session()
            async with session.get(url, params=params, headers=headers) as resp:
                ct = (resp.headers.get("Content-Type") or "").lower()
                if resp.status == 200 and "application/pdf" in ct:
                    data = await resp.read()
                    if len(data) > max_bytes:
                        raise RuntimeError(f"PDF too large ({len(data)} bytes > {max_bytes})")
                    if not data.startswith(b"%PDF-"):
                        raise RuntimeError("Downloaded bytes are not a valid PDF (missing %PDF- header)")
                    return data
                body = await resp.text()
                last_err = f"{resp.status} {ct or 'no-ct'}: {body[:200]}"
                # Drive returns 403 with reason "userRateLimitExceeded" /
                # "rateLimitExceeded" / "quotaExceeded" — all transient.
                lowered = body.lower()
                soft_403 = resp.status == 403 and (
                    "ratelimitexceeded" in lowered or "quotaexceeded" in lowered
                )
                if resp.status in (429, 500, 502, 503, 504) or soft_403:
                    wait = (2 ** attempt) + 0.5 * attempt
                    logger.info(f"Drive download {file_id} got {resp.status}; retrying in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                # Not retryable
                break
        except asyncio.TimeoutError:
            last_err = "timeout"
            await asyncio.sleep(2 ** attempt)
        except RuntimeError:
            raise
        except Exception as e:
            last_err = str(e)
            await asyncio.sleep(2 ** attempt)

    raise RuntimeError(f"Drive download failed for {file_id}: {last_err}")


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
