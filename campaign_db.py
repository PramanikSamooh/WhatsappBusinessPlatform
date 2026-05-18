"""Campaign Database Layer

Manages bulk template message campaigns with recipient tracking.
Each campaign targets a list of recipients and sends a WhatsApp
template message with rate-limited delivery.
"""

import json
from datetime import datetime, timezone

import aiosqlite
from loguru import logger

from db import DB_PATH, _enable_foreign_keys, _validate_columns
from utils import generate_id

VALID_CAMPAIGN_STATUSES = ("draft", "running", "paused", "completed", "failed")


async def init_campaign_tables():
    """Create campaigns and campaign_recipients tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id                TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                template_name     TEXT NOT NULL,
                template_category TEXT DEFAULT '',
                language          TEXT DEFAULT 'en',
                template_params   TEXT DEFAULT '[]',
                status            TEXT DEFAULT 'draft',
                recipient_count   INTEGER DEFAULT 0,
                sent_count        INTEGER DEFAULT 0,
                delivered_count   INTEGER DEFAULT 0,
                read_count        INTEGER DEFAULT 0,
                failed_count      INTEGER DEFAULT 0,
                rate_limit_per_min INTEGER DEFAULT 60,
                started_at        TEXT,
                completed_at      TEXT,
                created_at        TEXT DEFAULT (datetime('now'))
            )
        """)
        # Migration: add template_category column if missing
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN template_category TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migration: add source column for filtering (e.g., 'greetings')
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN source TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migration: add header_image_url for campaigns with image header templates
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN header_image_url TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migration: add pdf_filename_template for dues campaigns (per-campaign so
        # different campaigns can match files with different naming conventions).
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN pdf_filename_template TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migration: add pdf_display_filename_template — the filename WhatsApp
        # shows to the recipient. Decoupled from the Drive-lookup filename so
        # donor-facing names can be friendly (e.g., Hindi) without renaming
        # the files in Drive.
        try:
            await db.execute("ALTER TABLE campaigns ADD COLUMN pdf_display_filename_template TEXT DEFAULT ''")
            await db.commit()
        except Exception:
            pass  # Column already exists
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaign_recipients (
                id              TEXT PRIMARY KEY,
                campaign_id     TEXT NOT NULL,
                phone           TEXT NOT NULL,
                name            TEXT DEFAULT '',
                status          TEXT DEFAULT 'pending',
                wa_message_id   TEXT DEFAULT '',
                sent_at         TEXT,
                delivered_at    TEXT,
                read_at         TEXT,
                error_message   TEXT DEFAULT '',
                created_at      TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
                UNIQUE(campaign_id, phone)
            )
        """)
        # Migration: add extra_data column for per-recipient data (e.g., image_url for greetings)
        try:
            await db.execute("ALTER TABLE campaign_recipients ADD COLUMN extra_data TEXT DEFAULT '{}'")
            await db.commit()
        except Exception:
            pass  # Column already exists
        # Migration: add wa_message_ids history (JSON array) so fallback retries
        # preserve earlier attempts. The single wa_message_id column always holds
        # the latest wamid for webhook lookups.
        try:
            await db.execute("ALTER TABLE campaign_recipients ADD COLUMN wa_message_ids TEXT DEFAULT '[]'")
            await db.commit()
        except Exception:
            pass  # Column already exists
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipients_campaign
            ON campaign_recipients(campaign_id, status)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipients_wamid
            ON campaign_recipients(wa_message_id)
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_campaigns_status
            ON campaigns(status)
        """)
        await db.commit()
    logger.info("Campaign tables initialized")


# --- Campaign CRUD ---


async def create_campaign(
    name: str,
    template_name: str,
    language: str = "en",
    template_category: str = "",
    template_params: list | None = None,
    rate_limit_per_min: int = 60,
    source: str = "",
    header_image_url: str = "",
) -> dict:
    """Create a new campaign in draft status."""
    campaign_id = generate_id()
    now = datetime.now(timezone.utc).isoformat()
    params_json = json.dumps(template_params or [], ensure_ascii=False)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO campaigns (id, name, template_name, template_category, language, template_params,
               rate_limit_per_min, source, header_image_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, name, template_name, template_category, language, params_json, rate_limit_per_min, source, header_image_url, now),
        )
        await db.commit()

    logger.info(f"Campaign created: {campaign_id} ({name})")
    return await get_campaign(campaign_id)


async def get_campaign(campaign_id: str) -> dict | None:
    """Get a single campaign by ID with recipient stats."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return _parse_campaign(row)


async def list_campaigns(limit: int = 100, status: str = "") -> list[dict]:
    """List campaigns, optionally filtered by status."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status and status in VALID_CAMPAIGN_STATUSES:
            async with db.execute(
                "SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ) as cursor:
                rows = await cursor.fetchall()
        return [_parse_campaign(row) for row in rows]


_CAMPAIGN_ALLOWED_COLUMNS = {
    "name", "template_name", "template_category", "language", "template_params", "status",
    "recipient_count", "sent_count", "delivered_count", "read_count",
    "failed_count", "rate_limit_per_min", "started_at", "completed_at", "source",
    "header_image_url", "pdf_filename_template", "pdf_display_filename_template",
}


async def update_campaign(campaign_id: str, **kwargs) -> dict | None:
    """Update campaign fields. Only whitelisted columns are accepted."""
    if not kwargs:
        return await get_campaign(campaign_id)

    _validate_columns(kwargs, _CAMPAIGN_ALLOWED_COLUMNS)

    if "template_params" in kwargs and isinstance(kwargs["template_params"], list):
        kwargs["template_params"] = json.dumps(kwargs["template_params"], ensure_ascii=False)

    set_clause = ", ".join(f"{k} = ?" for k in kwargs)
    values = list(kwargs.values()) + [campaign_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(f"UPDATE campaigns SET {set_clause} WHERE id = ?", values)
        await db.commit()
    return await get_campaign(campaign_id)


async def delete_campaign(campaign_id: str) -> bool:
    """Delete a campaign and its recipients. Only allowed for draft/completed/failed."""
    campaign = await get_campaign(campaign_id)
    if not campaign:
        return False
    if campaign["status"] == "running":
        raise ValueError("Cannot delete a running campaign. Pause it first.")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM campaign_recipients WHERE campaign_id = ?", (campaign_id,))
        await db.execute("DELETE FROM campaigns WHERE id = ?", (campaign_id,))
        await db.commit()
    logger.info(f"Campaign deleted: {campaign_id}")
    return True


# --- Recipients ---


async def add_recipients(campaign_id: str, records: list[dict]) -> dict:
    """Add recipients to a campaign. Deduplicates by phone within the campaign.

    Args:
        campaign_id: Campaign ID
        records: List of dicts with keys: phone (required), name (optional),
                 extra_data (optional dict, e.g. {"image_url": "..."})

    Returns:
        Dict with added, duplicate, invalid counts.
    """
    added = 0
    duplicate = 0
    invalid = 0

    async with aiosqlite.connect(DB_PATH) as db:
        # Get existing phones for this campaign
        existing_phones = set()
        async with db.execute(
            "SELECT phone FROM campaign_recipients WHERE campaign_id = ?",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                existing_phones.add(row[0])

        for record in records:
            phone = str(record.get("phone", "")).strip()
            if not phone:
                invalid += 1
                continue
            # Normalize: remove +, spaces, dashes
            phone = phone.replace("+", "").replace(" ", "").replace("-", "")
            if not phone.isdigit() or len(phone) < 10:
                invalid += 1
                continue

            if phone in existing_phones:
                duplicate += 1
                continue

            name = str(record.get("name", "")).strip()
            extra = record.get("extra_data")
            extra_json = json.dumps(extra, ensure_ascii=False) if isinstance(extra, dict) else "{}"
            recipient_id = generate_id()
            await db.execute(
                "INSERT INTO campaign_recipients (id, campaign_id, phone, name, extra_data) VALUES (?, ?, ?, ?, ?)",
                (recipient_id, campaign_id, phone, name, extra_json),
            )
            existing_phones.add(phone)
            added += 1

        # Update recipient_count
        await db.execute(
            "UPDATE campaigns SET recipient_count = (SELECT COUNT(*) FROM campaign_recipients WHERE campaign_id = ?) WHERE id = ?",
            (campaign_id, campaign_id),
        )
        await db.commit()

    logger.info(f"Campaign {campaign_id}: added {added} recipients ({duplicate} dup, {invalid} invalid)")
    return {"added": added, "duplicate": duplicate, "invalid": invalid}


async def get_pending_recipients(campaign_id: str, limit: int = 100) -> list[dict]:
    """Get pending recipients ready to send."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM campaign_recipients WHERE campaign_id = ? AND status = 'pending' LIMIT ?",
            (campaign_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


_RECIPIENT_ALLOWED_COLUMNS = {
    "status", "wa_message_id", "error_message", "sent_at", "delivered_at", "read_at",
}


async def update_recipient_status(
    recipient_id: str,
    status: str,
    wa_message_id: str = "",
    error_message: str = "",
) -> None:
    """Update a recipient's delivery status."""
    now = datetime.now(timezone.utc).isoformat()
    updates = {"status": status}
    if wa_message_id:
        updates["wa_message_id"] = wa_message_id
    if error_message:
        updates["error_message"] = error_message
    if status == "sent":
        updates["sent_at"] = now
    elif status == "delivered":
        updates["delivered_at"] = now
    elif status == "read":
        updates["read_at"] = now

    _validate_columns(updates, _RECIPIENT_ALLOWED_COLUMNS)
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [recipient_id]
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        await db.execute(
            f"UPDATE campaign_recipients SET {set_clause} WHERE id = ?", values
        )
        await db.commit()


async def update_recipient_by_wamid(wa_message_id: str, status: str, error_message: str = "") -> bool:
    """Update recipient status by WhatsApp message ID (for delivery webhooks)."""
    if not wa_message_id:
        return False
    now = datetime.now(timezone.utc).isoformat()
    field_map = {"delivered": "delivered_at", "read": "read_at", "sent": "sent_at"}
    time_field = field_map.get(status)

    async with aiosqlite.connect(DB_PATH) as db:
        if status == "failed" and error_message:
            if time_field:
                await db.execute(
                    f"UPDATE campaign_recipients SET status = ?, error_message = ?, {time_field} = ? WHERE wa_message_id = ?",
                    (status, error_message[:500], now, wa_message_id),
                )
            else:
                await db.execute(
                    "UPDATE campaign_recipients SET status = ?, error_message = ? WHERE wa_message_id = ?",
                    (status, error_message[:500], wa_message_id),
                )
        elif time_field:
            await db.execute(
                f"UPDATE campaign_recipients SET status = ?, {time_field} = ? WHERE wa_message_id = ?",
                (status, now, wa_message_id),
            )
        else:
            await db.execute(
                "UPDATE campaign_recipients SET status = ? WHERE wa_message_id = ?",
                (status, wa_message_id),
            )
        changes = db.total_changes
        await db.commit()
    return changes > 0


async def refresh_campaign_stats(campaign_id: str) -> None:
    """Recount campaign stats from recipient rows using single GROUP BY query."""
    async with aiosqlite.connect(DB_PATH) as db:
        await _enable_foreign_keys(db)
        counts = {"pending": 0, "sent": 0, "delivered": 0, "read": 0, "failed": 0}
        async with db.execute(
            "SELECT status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY status",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                if row[0] in counts:
                    counts[row[0]] = row[1]

        await db.execute(
            """UPDATE campaigns SET
               sent_count = ?, delivered_count = ?, read_count = ?, failed_count = ?
               WHERE id = ?""",
            (counts["sent"], counts["delivered"], counts["read"], counts["failed"], campaign_id),
        )
        await db.commit()


async def get_recipient_stats(campaign_id: str) -> dict:
    """Get count per status for a campaign using single GROUP BY query."""
    async with aiosqlite.connect(DB_PATH) as db:
        stats = {"pending": 0, "sent": 0, "delivered": 0, "read": 0, "failed": 0, "total": 0}
        async with db.execute(
            "SELECT status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY status",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                if row[0] in stats:
                    stats[row[0]] = row[1]
                stats["total"] += row[1]
        return stats


async def list_recipients(
    campaign_id: str,
    limit: int = 100,
    offset: int = 0,
    status: str = "",
) -> list[dict]:
    """List recipients for a campaign with optional status filter."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            async with db.execute(
                "SELECT * FROM campaign_recipients WHERE campaign_id = ? AND status = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
                (campaign_id, status, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM campaign_recipients WHERE campaign_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
                (campaign_id, limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def export_campaign_results(campaign_id: str) -> list[dict]:
    """Export all recipients with their statuses for CSV download.

    Includes per-recipient JSON fields (serial, alternate numbers, used number,
    PDF filename, staging status) when present in extra_data.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT phone, name, status, sent_at, delivered_at, read_at,
                      error_message, extra_data, wa_message_ids
               FROM campaign_recipients
               WHERE campaign_id = ?
               ORDER BY created_at ASC""",
            (campaign_id,),
        ) as cursor:
            rows = await cursor.fetchall()

    out = []
    for row in rows:
        rec = dict(row)
        extra = {}
        if rec.get("extra_data"):
            try:
                extra = json.loads(rec["extra_data"]) or {}
            except (ValueError, TypeError):
                extra = {}
        chain = extra.get("fallback_chain") or []
        idx = extra.get("fallback_index") if isinstance(extra.get("fallback_index"), int) else 0
        used = chain[idx] if 0 <= idx < len(chain) else rec.get("phone", "")
        attempts = extra.get("attempted_numbers") or []
        out.append({
            "serial": extra.get("serial", ""),
            "name": rec.get("name", ""),
            "wa_number": extra.get("wa_number", ""),
            "phone_number": extra.get("phone_number", ""),
            "alt_number": extra.get("alt_number", ""),
            "used_number": used,
            "current_phone": rec.get("phone", ""),
            "status": rec.get("status", ""),
            "attempts": len(attempts),
            "sent_at": rec.get("sent_at", "") or "",
            "delivered_at": rec.get("delivered_at", "") or "",
            "read_at": rec.get("read_at", "") or "",
            "error_message": rec.get("error_message", "") or "",
            "pdf_filename": extra.get("pdf_filename", ""),
            "staging_status": extra.get("staging_status", ""),
            "media_id": extra.get("media_id", ""),
        })
    return out


# --- JSON helpers for extra_data ---


async def update_recipient_extra(recipient_id: str, **patch) -> None:
    """Merge keys into a recipient's extra_data JSON column.

    Reads existing JSON, applies the patch in-process (so this isn't perfectly
    race-free), and writes back. For our staging/runner flows each recipient is
    written by at most one task at a time, so this is safe in practice.
    """
    if not patch:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT extra_data FROM campaign_recipients WHERE id = ?",
            (recipient_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return
        try:
            extra = json.loads(row[0]) if row[0] else {}
        except (ValueError, TypeError):
            extra = {}
        extra.update(patch)
        await db.execute(
            "UPDATE campaign_recipients SET extra_data = ? WHERE id = ?",
            (json.dumps(extra, ensure_ascii=False), recipient_id),
        )
        await db.commit()


async def append_wamid(recipient_id: str, wamid: str) -> None:
    """Append a wamid to the wa_message_ids history and set wa_message_id to it."""
    if not wamid:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT wa_message_ids FROM campaign_recipients WHERE id = ?",
            (recipient_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return
        try:
            history = json.loads(row[0]) if row[0] else []
            if not isinstance(history, list):
                history = []
        except (ValueError, TypeError):
            history = []
        if wamid not in history:
            history.append(wamid)
        await db.execute(
            "UPDATE campaign_recipients SET wa_message_ids = ?, wa_message_id = ? WHERE id = ?",
            (json.dumps(history, ensure_ascii=False), wamid, recipient_id),
        )
        await db.commit()


async def update_recipient_phone(recipient_id: str, phone: str) -> None:
    """Update the active phone column for a recipient (used when chain advances)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE campaign_recipients SET phone = ? WHERE id = ?",
            (phone, recipient_id),
        )
        await db.commit()


# --- Dues-specific queries (staging + deferred retry) ---


async def get_recipients_for_staging(campaign_id: str, limit: int = 5000) -> list[dict]:
    """Return recipients whose media is not yet staged (or has expired)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT * FROM campaign_recipients
               WHERE campaign_id = ?
                 AND (
                   json_extract(extra_data, '$.staging_status') IS NULL
                   OR json_extract(extra_data, '$.staging_status') NOT IN ('staged')
                   OR (
                     json_extract(extra_data, '$.media_expires_at') IS NOT NULL
                     AND datetime(json_extract(extra_data, '$.media_expires_at')) <= datetime('now')
                   )
                 )
               LIMIT ?""",
            (campaign_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_staging_progress(campaign_id: str) -> dict:
    """Return counts by staging_status for a campaign."""
    counts = {"total": 0, "staged": 0, "missing": 0, "pending": 0, "error": 0}
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT COALESCE(json_extract(extra_data, '$.staging_status'), 'pending') AS s,
                      COUNT(*) AS n
               FROM campaign_recipients
               WHERE campaign_id = ?
               GROUP BY s""",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                s = row[0] or "pending"
                n = row[1] or 0
                counts["total"] += n
                if s in counts:
                    counts[s] = n
                else:
                    counts["error"] += n
    return counts


async def get_recipients_needing_retry(
    campaign_id: str,
    failed_after_minutes: int,
    stuck_after_minutes: int,
    limit: int = 500,
) -> list[dict]:
    """Recipients that should advance their fallback chain.

    Returns rows where the chain has unused numbers AND either:
      - status = 'failed' AND sent_at older than failed_after_minutes, OR
      - status in ('sent') AND delivered_at IS NULL AND sent_at older than stuck_after_minutes
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT * FROM campaign_recipients
               WHERE campaign_id = ?
                 AND json_extract(extra_data, '$.fallback_index') IS NOT NULL
                 AND json_extract(extra_data, '$.fallback_chain') IS NOT NULL
                 AND (
                   CAST(json_extract(extra_data, '$.fallback_index') AS INTEGER)
                   < (json_array_length(json_extract(extra_data, '$.fallback_chain')) - 1)
                 )
                 AND (
                   (status = 'failed'
                     AND sent_at IS NOT NULL
                     AND datetime(sent_at) <= datetime('now', ?))
                   OR
                   (status = 'sent'
                     AND delivered_at IS NULL
                     AND sent_at IS NOT NULL
                     AND datetime(sent_at) <= datetime('now', ?))
                 )
               LIMIT ?""",
            (
                campaign_id,
                f"-{int(failed_after_minutes)} minutes",
                f"-{int(stuck_after_minutes)} minutes",
                limit,
            ),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def reset_recipient_for_retry(recipient_id: str) -> None:
    """Reset status/timestamps so the runner picks the recipient up again."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE campaign_recipients
               SET status = 'pending',
                   wa_message_id = '',
                   sent_at = NULL,
                   delivered_at = NULL,
                   read_at = NULL,
                   error_message = ''
               WHERE id = ?""",
            (recipient_id,),
        )
        await db.commit()


async def upsert_dues_recipients(campaign_id: str, records: list[dict]) -> dict:
    """Upsert recipients keyed by extra_data.serial.

    Each record dict must have:
      - phone (canonical current attempt; usually first valid number in chain)
      - name
      - extra_data (with at least: serial, fallback_chain, fallback_index=0,
        pdf_filename, staging_status='pending', wa_number/phone_number/alt_number)

    For existing serial: updates phone (if chain restart wanted), name, and merges
    extra_data — but PRESERVES status, wamids, and any media_id/expiry already set.
    For new serial: inserts a fresh row with status='pending'.
    """
    added = 0
    updated = 0
    invalid = 0

    async with aiosqlite.connect(DB_PATH) as db:
        existing_by_serial: dict[str, dict] = {}
        async with db.execute(
            """SELECT id, phone, name, status, extra_data
               FROM campaign_recipients
               WHERE campaign_id = ?""",
            (campaign_id,),
        ) as cursor:
            async for row in cursor:
                rid, phone, name, status, ed = row
                try:
                    ed_parsed = json.loads(ed) if ed else {}
                except (ValueError, TypeError):
                    ed_parsed = {}
                serial = str(ed_parsed.get("serial") or "")
                if serial:
                    existing_by_serial[serial] = {
                        "id": rid,
                        "phone": phone,
                        "name": name,
                        "status": status,
                        "extra_data": ed_parsed,
                    }

        for record in records:
            extra = record.get("extra_data") or {}
            serial = str(extra.get("serial") or "").strip()
            if not serial:
                invalid += 1
                continue
            chain = extra.get("fallback_chain") or []
            if not chain or not isinstance(chain, list):
                invalid += 1
                continue

            phone = str(record.get("phone") or chain[0] or "").strip()
            name = str(record.get("name") or "").strip()

            if serial in existing_by_serial:
                ex = existing_by_serial[serial]
                # Merge: preserve media_id/expires + staging if still valid + status
                merged = dict(ex["extra_data"])
                preserved_keys = {"media_id", "media_expires_at", "pdf_filename", "staging_status",
                                  "staging_error", "attempted_numbers"}
                for k, v in extra.items():
                    if k in preserved_keys and merged.get(k) is not None:
                        continue  # keep existing
                    merged[k] = v
                # Always keep canonical record fields fresh
                merged["serial"] = serial
                await db.execute(
                    "UPDATE campaign_recipients SET name = ?, extra_data = ? WHERE id = ?",
                    (name, json.dumps(merged, ensure_ascii=False), ex["id"]),
                )
                updated += 1
            else:
                from utils import generate_id as _gen
                rid = _gen()
                phone_for_db = phone
                # Work around UNIQUE(campaign_id, phone) when two donors share a number:
                # disambiguate by appending #serial. send_one() always sends to
                # chain[idx] (the clean number), so this only affects the column value.
                try:
                    await db.execute(
                        """INSERT INTO campaign_recipients
                           (id, campaign_id, phone, name, status, extra_data)
                           VALUES (?, ?, ?, ?, 'pending', ?)""",
                        (rid, campaign_id, phone_for_db, name, json.dumps(extra, ensure_ascii=False)),
                    )
                except aiosqlite.IntegrityError:
                    phone_for_db = f"{phone}#{serial}"
                    await db.execute(
                        """INSERT INTO campaign_recipients
                           (id, campaign_id, phone, name, status, extra_data)
                           VALUES (?, ?, ?, ?, 'pending', ?)""",
                        (rid, campaign_id, phone_for_db, name, json.dumps(extra, ensure_ascii=False)),
                    )
                added += 1

        await db.execute(
            "UPDATE campaigns SET recipient_count = (SELECT COUNT(*) FROM campaign_recipients WHERE campaign_id = ?) WHERE id = ?",
            (campaign_id, campaign_id),
        )
        await db.commit()

    logger.info(f"Dues campaign {campaign_id}: {added} added, {updated} updated, {invalid} invalid")
    return {"added": added, "updated": updated, "invalid": invalid}


def _parse_campaign(row) -> dict:
    """Parse JSON fields in a campaign row."""
    record = dict(row)
    if record.get("template_params"):
        try:
            record["template_params"] = json.loads(record["template_params"])
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"Campaign {record.get('id', '?')}: failed to parse template_params JSON")
    return record
