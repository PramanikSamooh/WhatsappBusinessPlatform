"""Media staging for the Dues campaign.

For each recipient with a serial number, looks up the matching PDF in a
Google Drive folder, downloads it via Drive API, uploads it to WhatsApp
/media endpoint, and stores the returned media_id in the recipient's
extra_data. Media IDs are reusable for ~30 days.

Throttled to stay well under Drive's per-user quota.
"""

import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import aiosqlite
from loguru import logger

from campaign_db import (
    get_recipients_for_staging,
    get_staging_progress,
    update_recipient_extra,
)
from db import DB_PATH
from gdrive import download_pdf, list_folder_pdfs
from whatsapp_messaging import upload_media_pdf

DUES_STAGING_RPS = float(os.getenv("DUES_STAGING_RPS", "5"))
DUES_MEDIA_TTL_DAYS = int(os.getenv("DUES_MEDIA_TTL_DAYS", "29"))

# Tracking dicts (campaign_id keyed)
_running_stagers: dict[str, asyncio.Task] = {}
_progress: dict[str, dict] = {}


def _norm_name(name: str) -> str:
    """Normalize a filename for matching: NFKC, lowercase, collapse whitespace."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", str(name)).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def is_staging(campaign_id: str) -> bool:
    task = _running_stagers.get(campaign_id)
    return task is not None and not task.done()


def get_progress(campaign_id: str) -> dict:
    return _progress.get(campaign_id, {})


def cancel_staging(campaign_id: str) -> bool:
    task = _running_stagers.get(campaign_id)
    if task and not task.done():
        task.cancel()
        return True
    return False


def start_staging(campaign_id: str, drive_folder_id: str, filename_template: str | None = None) -> bool:
    """Start a background staging task for a campaign. Returns False if one is already running."""
    if is_staging(campaign_id):
        return False
    template = filename_template or os.getenv("DUES_PDF_FILENAME_TEMPLATE", "All Letters_{serial}.pdf")
    task = asyncio.create_task(
        _stage_campaign_media(campaign_id, drive_folder_id, template, DUES_STAGING_RPS)
    )
    _running_stagers[campaign_id] = task
    return True


async def _stage_campaign_media(
    campaign_id: str,
    drive_folder_id: str,
    filename_template: str,
    rps: float,
) -> None:
    """Stage all unstaged/expired recipients for a campaign."""
    _progress[campaign_id] = {"phase": "listing_drive", "total": 0, "staged": 0,
                              "missing": 0, "error": 0, "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        files = await list_folder_pdfs(drive_folder_id)
        name_to_file: dict[str, str] = {}
        for f in files:
            key = _norm_name(f["name"])
            if key and key not in name_to_file:
                name_to_file[key] = f["file_id"]

        recipients = await get_recipients_for_staging(campaign_id)
        _progress[campaign_id].update({"phase": "staging", "total": len(recipients)})

        interval = 1.0 / max(rps, 0.1)
        for recipient in recipients:
            try:
                await _stage_one(recipient, filename_template, name_to_file)
                # Refresh progress from DB after each row so the UI reflects state
                snapshot = await get_staging_progress(campaign_id)
                _progress[campaign_id].update({
                    "phase": "staging",
                    "total": snapshot["total"],
                    "staged": snapshot["staged"],
                    "missing": snapshot["missing"],
                    "error": snapshot["error"],
                })
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Stage row error rid={recipient.get('id')}: {e}")
                try:
                    await update_recipient_extra(
                        recipient["id"],
                        staging_status="error",
                        staging_error=str(e)[:300],
                    )
                except Exception:
                    pass
            await asyncio.sleep(interval)

        snapshot = await get_staging_progress(campaign_id)
        _progress[campaign_id].update({
            "phase": "done",
            "total": snapshot["total"],
            "staged": snapshot["staged"],
            "missing": snapshot["missing"],
            "error": snapshot["error"],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(f"Staging complete for campaign {campaign_id}: {_progress[campaign_id]}")
    except asyncio.CancelledError:
        _progress[campaign_id]["phase"] = "cancelled"
        logger.info(f"Staging cancelled for campaign {campaign_id}")
        raise
    except Exception as e:
        logger.error(f"Staging failed for campaign {campaign_id}: {e}")
        _progress[campaign_id]["phase"] = "failed"
        _progress[campaign_id]["error_message"] = str(e)[:300]
    finally:
        _running_stagers.pop(campaign_id, None)


async def _stage_one(
    recipient: dict,
    filename_template: str,
    name_to_file: dict[str, str],
) -> None:
    """Stage one recipient: find PDF, download, upload, persist media_id."""
    rid = recipient["id"]
    try:
        extra = json.loads(recipient.get("extra_data") or "{}")
    except (ValueError, TypeError):
        extra = {}
    serial = str(extra.get("serial") or "").strip()
    if not serial:
        await update_recipient_extra(rid, staging_status="error", staging_error="missing serial in extra_data")
        return

    expected = filename_template.format(serial=serial).strip()
    file_id = name_to_file.get(_norm_name(expected))
    if not file_id:
        # Try with .pdf appended if template didn't include it
        if "." not in expected:
            file_id = name_to_file.get(_norm_name(expected + ".pdf"))
    if not file_id:
        await update_recipient_extra(
            rid,
            staging_status="missing",
            staging_error=f"PDF '{expected}' not found in Drive folder",
            pdf_filename=expected,
        )
        return

    pdf_bytes = await download_pdf(file_id)
    upload_result = await upload_media_pdf(pdf_bytes, expected)
    if not upload_result.get("success"):
        await update_recipient_extra(
            rid,
            staging_status="error",
            staging_error=f"WhatsApp upload failed: {upload_result.get('error', '')[:200]}",
            pdf_filename=expected,
        )
        return

    expires_at = (datetime.now(timezone.utc) + timedelta(days=DUES_MEDIA_TTL_DAYS)).isoformat()
    await update_recipient_extra(
        rid,
        staging_status="staged",
        staging_error="",
        media_id=upload_result["media_id"],
        media_expires_at=expires_at,
        pdf_filename=expected,
    )


async def restage_one(recipient_id: str, drive_folder_id: str, filename_template: str | None = None) -> dict:
    """Re-stage a single recipient on demand (e.g., media_id expired).

    Returns the upload result dict {success, media_id|error}. Persists the new
    media_id to extra_data on success.
    """
    template = filename_template or os.getenv("DUES_PDF_FILENAME_TEMPLATE", "All Letters_{serial}.pdf")

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM campaign_recipients WHERE id = ?", (recipient_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return {"success": False, "error": "recipient not found"}
            recipient = dict(row)

    files = await list_folder_pdfs(drive_folder_id)
    name_to_file = {_norm_name(f["name"]): f["file_id"] for f in files}
    await _stage_one(recipient, template, name_to_file)
    # Re-read to confirm
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT extra_data FROM campaign_recipients WHERE id = ?", (recipient_id,)
        ) as cursor:
            row = await cursor.fetchone()
    try:
        extra = json.loads(row[0]) if row and row[0] else {}
    except (ValueError, TypeError):
        extra = {}
    if extra.get("staging_status") == "staged" and extra.get("media_id"):
        return {"success": True, "media_id": extra["media_id"]}
    return {"success": False, "error": extra.get("staging_error", "restage failed")}
