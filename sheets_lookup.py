"""Google Sheets Room/Dharamshala Lookup

Looks up visitor room and dharamshala details from a Google Sheet
when visitors ask on WhatsApp. Uses a Google Service Account for
API access.

Environment variables:
    GOOGLE_SHEETS_CREDENTIALS_JSON: Path to service account JSON file
                                    or inline JSON string
    DHARAMSHALA_SHEET_ID: Google Sheet ID containing room data

Expected sheet columns (flexible naming):
    Name, Phone/Mobile, Room Number, Dharamshala, Check-in, Check-out
"""

import asyncio
import json
import os
import re
import time

from loguru import logger

CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON", "")
SHEET_ID = os.getenv("DHARAMSHALA_SHEET_ID", "")

SHEETS_CONFIGURED = bool(SHEET_ID) and bool(CREDENTIALS_PATH)

# Cache sheet data in memory with TTL
_cache: dict = {"data": [], "fetched_at": 0}
_CACHE_TTL = 300  # 5 minutes


def _get_sheets_service():
    """Create Google Sheets API service using service account credentials."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        logger.error("google-auth or google-api-python-client not installed")
        return None

    try:
        # Try as file path first, then as inline JSON
        if os.path.isfile(CREDENTIALS_PATH):
            creds = service_account.Credentials.from_service_account_file(
                CREDENTIALS_PATH,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
        else:
            info = json.loads(CREDENTIALS_PATH)
            creds = service_account.Credentials.from_service_account_info(
                info,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )
        return build("sheets", "v4", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to create Sheets service: {e}")
        return None


def _fetch_sheet_data() -> list[dict]:
    """Fetch all rows from the dharamshala sheet (synchronous)."""
    service = _get_sheets_service()
    if not service:
        return []

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range="A:Z")
            .execute()
        )
        rows = result.get("values", [])
        if len(rows) < 2:
            return []

        # Parse headers (first row)
        raw_headers = [str(h).strip().lower() for h in rows[0]]

        # Map to standard field names
        col_map = {}
        for i, h in enumerate(raw_headers):
            if h in ("name", "naam"):
                col_map["name"] = i
            elif h in ("phone", "mobile", "phone number", "mobile number", "mob"):
                col_map["phone"] = i
            elif h in ("room", "room number", "room no", "room_number"):
                col_map["room_number"] = i
            elif h in ("dharamshala", "dharamshala name", "dharmshala", "building"):
                col_map["dharamshala"] = i
            elif h in ("check-in", "checkin", "check in", "arrival"):
                col_map["checkin"] = i
            elif h in ("check-out", "checkout", "check out", "departure"):
                col_map["checkout"] = i

        records = []
        for row in rows[1:]:
            record = {}
            for field, idx in col_map.items():
                record[field] = str(row[idx]).strip() if idx < len(row) and row[idx] else ""
            # Normalize phone
            if record.get("phone"):
                record["phone"] = re.sub(r"[+\s\-]", "", record["phone"])
            if record.get("name") or record.get("phone"):
                records.append(record)

        logger.info(f"Fetched {len(records)} rows from dharamshala sheet")
        return records

    except Exception as e:
        logger.error(f"Failed to fetch sheet data: {e}")
        return []


def _get_cached_data() -> list[dict]:
    """Get sheet data from cache or fetch fresh."""
    now = time.time()
    if _cache["data"] and (now - _cache["fetched_at"]) < _CACHE_TTL:
        return _cache["data"]

    data = _fetch_sheet_data()
    _cache["data"] = data
    _cache["fetched_at"] = now
    return data


async def lookup_room(phone: str = "", name: str = "") -> dict | None:
    """Look up dharamshala room details by phone or name.

    Args:
        phone: Visitor's phone number (will be normalized)
        name: Visitor's name (case-insensitive partial match)

    Returns:
        Dict with room details, or None if not found.
    """
    if not SHEETS_CONFIGURED:
        return None

    data = await asyncio.to_thread(_get_cached_data)
    if not data:
        return None

    # Search by phone first (exact match after normalization)
    if phone:
        normalized = re.sub(r"[+\s\-]", "", phone)
        for record in data:
            if record.get("phone") and record["phone"] == normalized:
                return record
        # Try matching last 10 digits
        if len(normalized) > 10:
            last10 = normalized[-10:]
            for record in data:
                if record.get("phone") and record["phone"][-10:] == last10:
                    return record

    # Search by name (case-insensitive partial match)
    if name:
        name_lower = name.lower()
        for record in data:
            if record.get("name") and name_lower in record["name"].lower():
                return record

    return None


def format_room_info(info: dict) -> str:
    """Format room details into a readable string for the AI context."""
    parts = []
    if info.get("name"):
        parts.append(f"Name: {info['name']}")
    if info.get("room_number"):
        parts.append(f"Room Number: {info['room_number']}")
    if info.get("dharamshala"):
        parts.append(f"Dharamshala: {info['dharamshala']}")
    if info.get("checkin"):
        parts.append(f"Check-in: {info['checkin']}")
    if info.get("checkout"):
        parts.append(f"Check-out: {info['checkout']}")
    return "\n".join(parts) if parts else ""
