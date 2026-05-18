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
    append_wamid,
    get_campaign,
    get_pending_recipients,
    get_recipients_needing_retry,
    refresh_campaign_stats,
    reset_recipient_for_retry,
    update_campaign,
    update_recipient_extra,
    update_recipient_phone,
    update_recipient_status,
)
from whatsapp_messaging import (
    NOT_ON_WHATSAPP_CODES,
    send_marketing_template,
    send_whatsapp_template,
)

# Use Marketing Messages API for campaigns (better delivery rates)
USE_MARKETING_API = os.getenv("USE_MARKETING_API", "true").lower() in ("true", "1", "yes")

# Max concurrent API calls (prevents overwhelming Meta's API)
MAX_CONCURRENT = int(os.getenv("CAMPAIGN_MAX_CONCURRENT", "20"))

# Global tracking of running campaigns (campaign_id → should_pause flag)
_running_campaigns: dict[str, bool] = {}

# Deferred retry workers for dues campaigns (campaign_id → asyncio.Task)
_running_retry_workers: dict[str, asyncio.Task] = {}

# Default deferred retry window for dues campaigns
DUES_FAILED_RETRY_DELAY_MIN = int(os.getenv("DUES_FAILED_RETRY_DELAY_MIN", "10"))
DUES_STUCK_UNDELIVERED_MIN = int(os.getenv("DUES_STUCK_UNDELIVERED_MIN", "120"))
DUES_DEFERRED_RETRY_INTERVAL_SEC = int(os.getenv("DUES_DEFERRED_RETRY_INTERVAL_SEC", "60"))


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
        """Send to a single recipient with rate limiting + per-recipient fallback chain.

        If extra_data has a `fallback_chain` (list) and `fallback_index` (int),
        the runner walks the chain until a number accepts or the chain is exhausted.
        Single-number campaigns (no chain) behave as before.
        """
        nonlocal total_sent, total_failed

        rid = recipient["id"]
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

        # Determine fallback chain
        chain = extra_data.get("fallback_chain") or []
        start_idx = extra_data.get("fallback_index")
        if not isinstance(start_idx, int) or start_idx < 0:
            start_idx = 0
        if not isinstance(chain, list) or not chain:
            chain = [recipient["phone"]]
            start_idx = 0

        # Rate limit: wait for our turn (one slot per recipient, not per attempt)
        async with send_lock:
            await asyncio.sleep(interval)

        async with semaphore:
            success = False
            last_error = ""
            attempted = list(extra_data.get("attempted_numbers") or [])

            for idx in range(start_idx, len(chain)):
                phone = chain[idx]
                if not phone:
                    continue

                # Build components AFTER chain advance, since media_id is per-recipient (not per-number)
                components = _build_components(template_params, name, extra_data)

                # Update the active phone column so webhook routing & UI reflect current attempt
                await update_recipient_phone(rid, phone)

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
                except Exception as e:
                    logger.error(f"Campaign {campaign_id}: send to {phone} exception: {e}")
                    result = {"success": False, "error": str(e)[:200], "code": None}

                code = result.get("code")
                attempted.append({
                    "number": phone,
                    "idx": idx,
                    "ok": bool(result.get("success")),
                    "wamid": result.get("wa_message_id", "") if result.get("success") else "",
                    "error_code": code,
                    "error": (result.get("error", "") or "")[:200],
                    "ts": datetime.now(timezone.utc).isoformat(),
                })

                if result.get("success"):
                    wa_mid = result.get("wa_message_id", "")
                    await update_recipient_status(rid, "sent", wa_message_id=wa_mid)
                    await append_wamid(rid, wa_mid)
                    await update_recipient_extra(
                        rid,
                        fallback_index=idx,
                        attempted_numbers=attempted,
                    )
                    success = True
                    total_sent += 1
                    break

                # Failure path. Decide whether to advance.
                last_error = result.get("error", "API returned error")
                advance = code in NOT_ON_WHATSAPP_CODES
                if advance and idx < len(chain) - 1:
                    # Persist progress so a crash leaves the chain in a known state
                    await update_recipient_extra(
                        rid,
                        fallback_index=idx + 1,
                        attempted_numbers=attempted,
                    )
                    continue  # Try next number in chain

                # No more numbers OR error wasn't "not on whatsapp" → mark failed
                await update_recipient_status(rid, "failed", error_message=last_error)
                await update_recipient_extra(
                    rid,
                    fallback_index=idx,
                    attempted_numbers=attempted,
                )
                total_failed += 1
                break

            if not success and not last_error:
                # Chain was empty / all numbers blank
                await update_recipient_status(rid, "failed", error_message="no valid number in fallback chain")
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

    if extra_data and extra_data.get("media_id"):
        components.append({
            "type": "header",
            "parameters": [{
                "type": "document",
                "document": {
                    "id": extra_data["media_id"],
                    "filename": extra_data.get("pdf_filename", "document.pdf"),
                },
            }],
        })
    elif extra_data and extra_data.get("image_url"):
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


# --- Deferred retry worker (for dues campaigns) ---


def is_retry_worker_running(campaign_id: str) -> bool:
    task = _running_retry_workers.get(campaign_id)
    return task is not None and not task.done()


def stop_retry_worker(campaign_id: str) -> bool:
    task = _running_retry_workers.get(campaign_id)
    if task and not task.done():
        task.cancel()
        return True
    return False


async def start_deferred_retry_worker(
    campaign_id: str,
    failed_delay_min: int | None = None,
    stuck_undelivered_min: int | None = None,
    interval_sec: int | None = None,
) -> None:
    """Spawn a background coroutine that retries recipients on the next number in
    their fallback chain when:
      - status='failed' (from webhook) AND sent_at older than failed_delay_min, OR
      - status='sent' AND delivered_at is NULL AND sent_at older than stuck_undelivered_min

    Idempotent — calling it again for a still-running campaign returns immediately.
    """
    if is_retry_worker_running(campaign_id):
        logger.info(f"Retry worker already running for campaign {campaign_id}")
        return

    failed_min = failed_delay_min if failed_delay_min is not None else DUES_FAILED_RETRY_DELAY_MIN
    stuck_min = stuck_undelivered_min if stuck_undelivered_min is not None else DUES_STUCK_UNDELIVERED_MIN
    interval = interval_sec if interval_sec is not None else DUES_DEFERRED_RETRY_INTERVAL_SEC

    task = asyncio.create_task(
        _retry_worker_loop(campaign_id, failed_min, stuck_min, interval)
    )
    _running_retry_workers[campaign_id] = task
    logger.info(
        f"Deferred retry worker started for campaign {campaign_id} "
        f"(failed>={failed_min}m, stuck>={stuck_min}m, every {interval}s)"
    )


async def _retry_worker_loop(
    campaign_id: str,
    failed_delay_min: int,
    stuck_undelivered_min: int,
    interval_sec: int,
) -> None:
    """Worker loop — exits when the campaign is no longer active."""
    try:
        idle_checks = 0
        while True:
            try:
                await asyncio.sleep(interval_sec)
                campaign = await get_campaign(campaign_id)
                if not campaign:
                    logger.warning(f"Retry worker: campaign {campaign_id} not found, exiting")
                    return
                # Stop if campaign is terminally complete/failed AND no candidates remain
                candidates = await get_recipients_needing_retry(
                    campaign_id, failed_delay_min, stuck_undelivered_min
                )
                if not candidates:
                    if campaign["status"] in ("completed", "failed") :
                        idle_checks += 1
                        # Exit after 5 consecutive empty checks once campaign is terminal
                        if idle_checks >= 5:
                            logger.info(f"Retry worker exiting (campaign {campaign_id} terminal, no candidates)")
                            return
                    continue
                idle_checks = 0

                # For each candidate: advance index, reset for retry, dispatch
                for r in candidates:
                    try:
                        extra = json.loads(r.get("extra_data") or "{}")
                    except (ValueError, TypeError):
                        extra = {}
                    chain = extra.get("fallback_chain") or []
                    idx = extra.get("fallback_index")
                    if not isinstance(idx, int):
                        idx = 0
                    if idx >= len(chain) - 1:
                        continue  # Nothing to advance to

                    next_idx = idx + 1
                    reason = "failed" if r["status"] == "failed" else "stuck_undelivered"
                    attempted = list(extra.get("attempted_numbers") or [])
                    attempted.append({
                        "advance_reason": reason,
                        "from_idx": idx,
                        "to_idx": next_idx,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    })
                    await update_recipient_extra(
                        r["id"],
                        fallback_index=next_idx,
                        attempted_numbers=attempted,
                    )
                    # Reset row to pending so the runner will pick it up.
                    await reset_recipient_for_retry(r["id"])
                    # Set the active phone column to the new number
                    if 0 <= next_idx < len(chain) and chain[next_idx]:
                        await update_recipient_phone(r["id"], chain[next_idx])
                    logger.info(
                        f"Retry worker: campaign {campaign_id} recipient {r['id']} "
                        f"advancing {idx}->{next_idx} (reason: {reason})"
                    )

                # If the runner isn't currently active, kick off a fresh run pass
                # so the newly-pending rows get sent. run_campaign refuses to
                # start unless status is draft/paused, so flip the campaign to
                # paused first.
                if not is_campaign_running(campaign_id):
                    if campaign["status"] in ("completed", "failed"):
                        await update_campaign(campaign_id, status="paused")
                    asyncio.create_task(run_campaign(campaign_id))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Retry worker error for campaign {campaign_id}: {e}")
    except asyncio.CancelledError:
        logger.info(f"Retry worker cancelled for campaign {campaign_id}")
        raise
    finally:
        _running_retry_workers.pop(campaign_id, None)
