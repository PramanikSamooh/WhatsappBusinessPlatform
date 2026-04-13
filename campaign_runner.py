"""Campaign Runner — High-Throughput Rate-Limited Bulk Template Sender

Sends messages CONCURRENTLY with rate limiting using a token bucket.
Multiple API calls happen in parallel, dramatically increasing throughput.

At rate_limit=1000/min with 10 concurrent workers:
- Old (sequential): ~60 msgs/min (bottlenecked by 1s API latency)
- New (concurrent): ~1000 msgs/min (10 parallel API calls)
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone

from loguru import logger

DEFAULT_RATE_LIMIT = int(os.getenv("CAMPAIGN_RATE_LIMIT", "60"))

from campaign_db import (
    get_campaign,
    get_pending_recipients,
    refresh_campaign_stats,
    update_campaign,
    update_recipient_status,
)
from whatsapp_messaging import send_marketing_template, send_whatsapp_template

# Use Marketing Messages API for campaigns (better delivery rates)
USE_MARKETING_API = os.getenv("USE_MARKETING_API", "true").lower() in ("true", "1", "yes")

# Max concurrent API calls (prevents overwhelming Meta's API)
MAX_CONCURRENT = int(os.getenv("CAMPAIGN_MAX_CONCURRENT", "20"))

# Global tracking of running campaigns (campaign_id → should_pause flag)
_running_campaigns: dict[str, bool] = {}


def is_campaign_running(campaign_id: str) -> bool:
    return campaign_id in _running_campaigns


def request_pause(campaign_id: str):
    """Signal a running campaign to pause after current batch."""
    if campaign_id in _running_campaigns:
        _running_campaigns[campaign_id] = True


async def run_campaign(campaign_id: str) -> None:
    """Main entry point — sends templates to all pending recipients.

    Sends messages concurrently (up to MAX_CONCURRENT) with token bucket
    rate limiting for high throughput.
    """
    campaign = await get_campaign(campaign_id)
    if not campaign:
        logger.error(f"Campaign {campaign_id} not found")
        return

    if campaign["status"] not in ("draft", "paused"):
        logger.warning(f"Campaign {campaign_id} status is {campaign['status']}, cannot start")
        return

    rate_limit = campaign.get("rate_limit_per_min") or DEFAULT_RATE_LIMIT
    template_name = campaign["template_name"]
    language = campaign.get("language", "en")
    template_params = campaign.get("template_params") or []
    campaign_header_image = campaign.get("header_image_url", "")
    campaign_category = (campaign.get("template_category") or "").upper()
    use_mm = USE_MARKETING_API and campaign_category == "MARKETING"

    # Mark as running
    _running_campaigns[campaign_id] = False
    now = datetime.now(timezone.utc).isoformat()
    await update_campaign(campaign_id, status="running", started_at=now)

    # Rate limiting: token bucket that releases tokens at rate_limit/min
    interval = 60.0 / max(rate_limit, 1)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    total_sent = 0
    total_failed = 0
    send_lock = asyncio.Lock()
    last_stats_refresh = time.monotonic()

    logger.info(f"Campaign {campaign_id} started (rate: {rate_limit}/min, concurrent: {MAX_CONCURRENT}, mm_lite: {use_mm})")

    async def send_one(recipient: dict) -> None:
        """Send to a single recipient with rate limiting."""
        nonlocal total_sent, total_failed

        rid = recipient["id"]
        phone = recipient["phone"]
        name = recipient.get("name", "")

        # Parse per-recipient extra data
        extra_data = {}
        if recipient.get("extra_data"):
            try:
                extra_data = json.loads(recipient["extra_data"])
            except (ValueError, TypeError):
                pass

        if not extra_data.get("image_url") and campaign_header_image:
            extra_data["image_url"] = campaign_header_image

        components = _build_components(template_params, name, extra_data)

        # Rate limit: wait for our turn
        async with send_lock:
            await asyncio.sleep(interval)

        async with semaphore:
            try:
                if use_mm:
                    result = await send_marketing_template(
                        to_phone=phone,
                        template_name=template_name,
                        language=language,
                        components=components if components else None,
                    )
                else:
                    result = await send_whatsapp_template(
                        to_phone=phone,
                        template_name=template_name,
                        language=language,
                        components=components if components else None,
                    )

                if result.get("success"):
                    wa_mid = result.get("wa_message_id", "")
                    await update_recipient_status(rid, "sent", wa_message_id=wa_mid)
                    total_sent += 1
                else:
                    error_msg = result.get("error", "API returned error")
                    await update_recipient_status(rid, "failed", error_message=error_msg)
                    total_failed += 1

            except Exception as e:
                logger.error(f"Campaign {campaign_id}: send to {phone} failed: {e}")
                await update_recipient_status(rid, "failed", error_message=str(e)[:200])
                total_failed += 1

    batch_size = 200
    try:
        while True:
            if _running_campaigns.get(campaign_id, False):
                await update_campaign(campaign_id, status="paused")
                await refresh_campaign_stats(campaign_id)
                logger.info(f"Campaign {campaign_id} paused after {total_sent} sent")
                break

            recipients = await get_pending_recipients(campaign_id, limit=batch_size)
            if not recipients:
                now_str = datetime.now(timezone.utc).isoformat()
                await update_campaign(campaign_id, status="completed", completed_at=now_str)
                await refresh_campaign_stats(campaign_id)
                logger.info(f"Campaign {campaign_id} completed: {total_sent} sent, {total_failed} failed")
                break

            # Filter out if pause requested mid-batch
            tasks = []
            for recipient in recipients:
                if _running_campaigns.get(campaign_id, False):
                    break
                tasks.append(asyncio.create_task(send_one(recipient)))

            # Wait for all tasks in this batch to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # Refresh stats periodically
            now_mono = time.monotonic()
            if (now_mono - last_stats_refresh) > 30:
                await refresh_campaign_stats(campaign_id)
                last_stats_refresh = now_mono
                logger.info(f"Campaign {campaign_id} progress: {total_sent} sent, {total_failed} failed")

    except Exception as e:
        logger.error(f"Campaign {campaign_id} runner error: {e}")
        await update_campaign(campaign_id, status="failed")
        await refresh_campaign_stats(campaign_id)
    finally:
        _running_campaigns.pop(campaign_id, None)


def _build_components(
    template_params: list,
    recipient_name: str,
    extra_data: dict | None = None,
) -> list:
    """Build WhatsApp template components from parameter definitions.

    Priority for body params:
      1. extra_data.template_params (per-recipient, for rooms campaigns)
      2. campaign-level template_params (with {{name}} substitution)
    """
    components = []

    if extra_data and extra_data.get("image_url"):
        components.append({
            "type": "header",
            "parameters": [{
                "type": "image",
                "image": {"link": extra_data["image_url"]},
            }],
        })

    # Per-recipient params override campaign-level params
    per_recipient_params = extra_data.get("template_params") if extra_data else None
    params_to_use = per_recipient_params if per_recipient_params else template_params

    if params_to_use:
        body_params = []
        for param in params_to_use:
            value = str(param).replace("{{name}}", recipient_name or "there")
            body_params.append({"type": "text", "text": value})
        if body_params:
            components.append({"type": "body", "parameters": body_params})

    return components
