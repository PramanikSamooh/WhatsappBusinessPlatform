r"""Call Campaign Database Layer

AI outbound phone-call campaigns over VoiceLink: a list of people the AI
calls one by one about a purpose (an event, an admissions follow-up, ...).

Recipient lifecycle:
    pending -> dialing -> in_call -> completed
                       \-> no_answer / busy / failed   (retried until max_attempts)
"""

import json
from datetime import datetime, timedelta, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH, _enable_foreign_keys, _validate_columns
from utils import generate_id

VALID_STATUSES = ("draft", "running", "paused", "completed", "failed")

# Recipient states that mean "a call attempt is in progress".
ACTIVE_RECIPIENT_STATUSES = ("dialing", "in_call")
# Recipient states that may be retried if attempts remain.
RETRYABLE_RECIPIENT_STATUSES = ("no_answer", "busy", "failed")


async def init_call_campaign_tables():
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS call_campaigns (
                id                  TEXT PRIMARY KEY,
                name                TEXT NOT NULL,
                purpose             TEXT NOT NULL,
                brief               TEXT DEFAULT '',
                followup_template   TEXT DEFAULT '',
                status              TEXT DEFAULT 'draft',
                max_attempts        INTEGER DEFAULT 2,
                retry_gap_minutes   INTEGER DEFAULT 60,
                call_window_start   TEXT DEFAULT '10:00',
                call_window_end     TEXT DEFAULT '19:00',
                last_error          TEXT DEFAULT '',
                started_at          TEXT,
                completed_at        TEXT,
                created_at          TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS call_campaign_recipients (
                id                TEXT PRIMARY KEY,
                campaign_id       TEXT NOT NULL,
                phone             TEXT NOT NULL,
                name              TEXT DEFAULT '',
                status            TEXT DEFAULT 'pending',
                attempts          INTEGER DEFAULT 0,
                last_attempt_at   TEXT,
                next_attempt_at   TEXT,
                outbound_queue_id INTEGER,
                call_id           TEXT DEFAULT '',
                duration_seconds  REAL DEFAULT 0,
                handoff_requested INTEGER DEFAULT 0,
                hangup_reason     TEXT DEFAULT '',
                error_message     TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (campaign_id) REFERENCES call_campaigns(id),
                UNIQUE(campaign_id, phone)
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_call_recipients_campaign "
            "ON call_campaign_recipients(campaign_id, status)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_call_recipients_queue "
            "ON call_campaign_recipients(outbound_queue_id)"
        )
        await db.commit()
    logger.info("Call campaign tables initialized")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Campaign CRUD ---


async def create_call_campaign(
    name: str,
    purpose: str,
    brief: str = "",
    followup_template: str = "",
    max_attempts: int = 2,
    retry_gap_minutes: int = 60,
    call_window_start: str = "10:00",
    call_window_end: str = "19:00",
) -> dict:
    campaign_id = generate_id()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO call_campaigns (id, name, purpose, brief, followup_template,
               max_attempts, retry_gap_minutes, call_window_start, call_window_end, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, name, purpose, brief, followup_template, max_attempts,
             retry_gap_minutes, call_window_start, call_window_end, _now()),
        )
        await db.commit()
    logger.info(f"Call campaign created: {campaign_id} ({name})")
    return await get_call_campaign(campaign_id)


async def get_call_campaign(campaign_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM call_campaigns WHERE id = ?", (campaign_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def list_call_campaigns(limit: int = 100, status: str = "") -> list[dict]:
    query = "SELECT * FROM call_campaigns"
    params: list = []
    if status in VALID_STATUSES:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            campaigns = [dict(r) for r in await cursor.fetchall()]
    for c in campaigns:
        c["recipient_stats"] = await get_call_recipient_stats(c["id"])
    return campaigns


_CAMPAIGN_ALLOWED_COLUMNS = {
    "name", "purpose", "brief", "followup_template", "status", "max_attempts",
    "retry_gap_minutes", "call_window_start", "call_window_end", "last_error",
    "started_at", "completed_at",
}


async def update_call_campaign(campaign_id: str, **kwargs) -> dict | None:
    if kwargs:
        _validate_columns(kwargs, _CAMPAIGN_ALLOWED_COLUMNS)
        set_clause = ", ".join(f"{k} = ?" for k in kwargs)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                f"UPDATE call_campaigns SET {set_clause} WHERE id = ?",
                [*kwargs.values(), campaign_id],
            )
            await db.commit()
    return await get_call_campaign(campaign_id)


async def delete_call_campaign(campaign_id: str) -> bool:
    campaign = await get_call_campaign(campaign_id)
    if not campaign:
        return False
    if campaign["status"] == "running":
        raise ValueError("Cannot delete a running campaign. Pause it first.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM call_campaign_recipients WHERE campaign_id = ?", (campaign_id,))
        await db.execute("DELETE FROM call_campaigns WHERE id = ?", (campaign_id,))
        await db.commit()
    logger.info(f"Call campaign deleted: {campaign_id}")
    return True


# --- Recipients ---


def normalize_indian_phone(raw: str) -> str:
    """Return 91XXXXXXXXXX, or '' if it isn't a valid Indian mobile/landline number."""
    digits = "".join(c for c in str(raw or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10:
        digits = "91" + digits
    return digits if len(digits) == 12 and digits.startswith("91") else ""


async def add_call_recipients(campaign_id: str, records: list[dict]) -> dict:
    """Add recipients ({phone, name}); dedupes by phone within the campaign."""
    added = duplicate = invalid = 0
    async with aiosqlite.connect(DB_PATH) as db:
        existing = set()
        async with db.execute(
            "SELECT phone FROM call_campaign_recipients WHERE campaign_id = ?", (campaign_id,)
        ) as cursor:
            async for row in cursor:
                existing.add(row[0])

        for record in records:
            phone = normalize_indian_phone(record.get("phone", ""))
            if not phone:
                invalid += 1
                continue
            if phone in existing:
                duplicate += 1
                continue
            await db.execute(
                "INSERT INTO call_campaign_recipients (id, campaign_id, phone, name) VALUES (?, ?, ?, ?)",
                (generate_id(), campaign_id, phone, str(record.get("name", "")).strip()),
            )
            existing.add(phone)
            added += 1
        await db.commit()
    logger.info(f"Call campaign {campaign_id}: added {added} recipients ({duplicate} dup, {invalid} invalid)")
    return {"added": added, "duplicate": duplicate, "invalid": invalid}


async def get_call_recipient(recipient_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM call_campaign_recipients WHERE id = ?", (recipient_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_call_recipient_by_queue_id(outbound_queue_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM call_campaign_recipients WHERE outbound_queue_id = ?", (outbound_queue_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def next_call_recipient(campaign_id: str, max_attempts: int) -> tuple[dict | None, str | None]:
    """Pick who to call next.

    Returns (recipient, None) if someone is due now, or (None, earliest_retry_iso)
    if only future retries remain, or (None, None) if the campaign is finished.
    """
    now = _now()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM call_campaign_recipients WHERE campaign_id = ? AND status = 'pending' "
            "ORDER BY created_at ASC LIMIT 1",
            (campaign_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row), None

        placeholders = ",".join("?" for _ in RETRYABLE_RECIPIENT_STATUSES)
        async with db.execute(
            f"SELECT * FROM call_campaign_recipients WHERE campaign_id = ? "
            f"AND status IN ({placeholders}) AND attempts < ? "
            f"ORDER BY next_attempt_at ASC LIMIT 1",
            (campaign_id, *RETRYABLE_RECIPIENT_STATUSES, max_attempts),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None, None
            row = dict(row)
            if not row.get("next_attempt_at") or row["next_attempt_at"] <= now:
                return row, None
            return None, row["next_attempt_at"]


_RECIPIENT_ALLOWED_COLUMNS = {
    "status", "attempts", "last_attempt_at", "next_attempt_at", "outbound_queue_id",
    "call_id", "duration_seconds", "handoff_requested", "hangup_reason", "error_message",
}


async def update_call_recipient(recipient_id: str, **kwargs):
    if not kwargs:
        return
    _validate_columns(kwargs, _RECIPIENT_ALLOWED_COLUMNS)
    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE call_campaign_recipients SET {set_clause} WHERE id = ?",
            [*kwargs.values(), recipient_id],
        )
        await db.commit()


async def mark_attempt_unanswered(recipient_id: str, status: str, retry_gap_minutes: int, reason: str = ""):
    """Record a no_answer/busy/failed attempt and schedule the retry time."""
    next_at = (datetime.now(timezone.utc) + timedelta(minutes=retry_gap_minutes)).isoformat()
    await update_call_recipient(
        recipient_id, status=status, next_attempt_at=next_at,
        **({"hangup_reason": reason} if reason else {}),
    )


async def reset_interrupted_recipients(campaign_id: str | None = None):
    """After a restart, attempts that were mid-call can't be resumed — count them as unanswered.

    With no campaign_id, applies to every campaign (used at startup).
    """
    placeholders = ",".join("?" for _ in ACTIVE_RECIPIENT_STATUSES)
    query = (
        f"UPDATE call_campaign_recipients SET status = 'no_answer', "
        f"hangup_reason = 'interrupted by server restart' "
        f"WHERE status IN ({placeholders})"
    )
    params: list = list(ACTIVE_RECIPIENT_STATUSES)
    if campaign_id:
        query += " AND campaign_id = ?"
        params.append(campaign_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def get_call_recipient_stats(campaign_id: str) -> dict:
    stats = {
        "total": 0, "pending": 0, "dialing": 0, "in_call": 0, "completed": 0,
        "no_answer": 0, "busy": 0, "failed": 0, "handoff": 0,
    }
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status, COUNT(*), SUM(handoff_requested) FROM call_campaign_recipients "
            "WHERE campaign_id = ? GROUP BY status",
            (campaign_id,),
        ) as cursor:
            async for status, count, handoffs in cursor:
                if status in stats:
                    stats[status] = count
                stats["total"] += count
                stats["handoff"] += handoffs or 0
    return stats


async def list_call_recipients(campaign_id: str, limit: int = 200, offset: int = 0) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM call_campaign_recipients WHERE campaign_id = ? "
            "ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (campaign_id, limit, offset),
        ) as cursor:
            return [dict(r) for r in await cursor.fetchall()]


async def export_call_campaign_results(campaign_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT phone, name, status, attempts, last_attempt_at, duration_seconds, "
            "handoff_requested, hangup_reason, error_message, call_id "
            "FROM call_campaign_recipients WHERE campaign_id = ? ORDER BY created_at ASC",
            (campaign_id,),
        ) as cursor:
            return [dict(r) for r in await cursor.fetchall()]


def parse_custom_parameters(value) -> dict:
    """VoiceLink may hand custom parameters back as a dict or a JSON string."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}
