"""Web Push notifications (VAPID) for the dashboard PWA.

Lets the dashboard (installed as an Android PWA) receive a push when a new
inbound WhatsApp message arrives — even when the app is backgrounded.

Storage: a `push_subscriptions` table in the shared SQLite DB.
Sending: pywebpush (synchronous) run in a thread so it doesn't block the loop.

If VAPID keys aren't configured, every function degrades to a safe no-op so
the rest of the platform keeps working.

Generate a key pair once with:
    python -c "from py_vapid import Vapid01; v=Vapid01(); v.generate_keys(); \
        import base64; \
        print('PUBLIC', base64.urlsafe_b64encode(v.public_key.public_bytes(__import__('cryptography').hazmat.primitives.serialization.Encoding.X962, __import__('cryptography').hazmat.primitives.serialization.PublicFormat.UncompressedPoint)).rstrip(b'=').decode())"
(or use the helper endpoint / README instructions).
"""

import asyncio
import json
import os

import aiosqlite
from loguru import logger

from db import DB_PATH

VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "").strip()
# Web Push requires a "sub" claim — a mailto: or https: contact for your service.
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "").strip() or "mailto:admin@example.com"

_enabled = bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)
if not _enabled:
    logger.info("Push notifications disabled (VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY not set)")


def is_enabled() -> bool:
    return _enabled


def get_public_key() -> str:
    return VAPID_PUBLIC_KEY


async def init_push_table():
    """Create the push_subscriptions table. Called from init_db()."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                endpoint     TEXT PRIMARY KEY,
                subscription TEXT NOT NULL,
                created_at   TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()
    logger.info("Push subscriptions table initialized")


async def save_subscription(subscription: dict) -> bool:
    """Upsert a browser push subscription. Returns True on success."""
    endpoint = (subscription or {}).get("endpoint")
    if not endpoint:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO push_subscriptions (endpoint, subscription) VALUES (?, ?)",
            (endpoint, json.dumps(subscription)),
        )
        await db.commit()
    logger.info(f"Push subscription saved ({endpoint[:48]}...)")
    return True


async def delete_subscription(endpoint: str) -> None:
    if not endpoint:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,))
        await db.commit()


async def _list_subscriptions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT subscription FROM push_subscriptions") as cursor:
            rows = await cursor.fetchall()
    subs = []
    for (raw,) in rows:
        try:
            subs.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue
    return subs


def _send_one(subscription: dict, payload: str):
    """Blocking send of a single web-push message (runs in a worker thread)."""
    from pywebpush import webpush  # imported lazily so the dep is optional
    webpush(
        subscription_info=subscription,
        data=payload,
        vapid_private_key=VAPID_PRIVATE_KEY,
        vapid_claims={"sub": VAPID_SUBJECT},
        ttl=600,
    )


async def send_to_all(title: str, body: str, url: str = "/static/dashboard.html", tag: str = "") -> int:
    """Send a notification to every stored subscription.

    Dead subscriptions (404/410) are pruned. Returns the number delivered.
    """
    if not _enabled:
        return 0

    subs = await _list_subscriptions()
    if not subs:
        return 0

    payload = json.dumps({"title": title, "body": body, "url": url, "tag": tag or "wa-msg"})
    delivered = 0

    for sub in subs:
        endpoint = sub.get("endpoint", "")
        try:
            await asyncio.to_thread(_send_one, sub, payload)
            delivered += 1
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (404, 410):
                # Subscription expired/unsubscribed — remove it.
                await delete_subscription(endpoint)
                logger.info(f"Pruned dead push subscription ({endpoint[:48]}...)")
            else:
                logger.warning(f"Push send failed ({endpoint[:48]}...): {e}")

    logger.info(f"Push: delivered {delivered}/{len(subs)} — {title}: {body[:40]}")
    return delivered
