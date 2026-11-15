"""Call Campaign Runner — AI outbound calls, one at a time

Works through a call campaign's recipients: asks VoiceLink to dial each one
(voicelink_client.place_call), then waits for that call to finish before the
next. The phone number has a limited number of channels (concurrent calls),
shared across all campaigns via a semaphore sized by
VOICELINK_MAX_CONCURRENT_CALLS.

How an attempt ends:
- answered: voicelink_bot.py marks the recipient in_call, then completed
- not answered: VoiceLink's call webhook arrives while still 'dialing' ->
  handle_voicelink_webhook() marks no_answer / busy / failed
- nothing heard back within DIAL_TIMEOUT_SECS -> no_answer
Unanswered attempts are retried after the campaign's retry gap until
max_attempts. Calls are only placed inside the campaign's IST call window.
"""

import asyncio
import os
import re
import time
from datetime import datetime, timedelta, timezone

from loguru import logger

import voicelink_client
from call_campaign_db import (
    ACTIVE_RECIPIENT_STATUSES,
    get_call_campaign,
    get_call_recipient,
    get_call_recipient_by_queue_id,
    list_call_campaigns,
    mark_attempt_unanswered,
    next_call_recipient,
    parse_custom_parameters,
    reset_interrupted_recipients,
    update_call_campaign,
    update_call_recipient,
)

IST = timezone(timedelta(hours=5, minutes=30))  # fixed offset: slim images ship no tzdata

MAX_CONCURRENT_CALLS = max(1, int(os.getenv("VOICELINK_MAX_CONCURRENT_CALLS", "1")))
DIAL_TIMEOUT_SECS = 120          # ringing this long with no answer/webhook -> no_answer
CALL_MAX_SECS = 20 * 60          # stop waiting on a call after this (the bot still finalizes it)
GAP_BETWEEN_CALLS_SECS = 5       # let VoiceLink free the channel before the next dial
MAX_CONSECUTIVE_API_ERRORS = 3   # e.g. wallet empty / bad credentials -> pause the campaign

_call_slots = asyncio.Semaphore(MAX_CONCURRENT_CALLS)
# campaign_id -> pause requested
_running_campaigns: dict[str, bool] = {}
# Keep task references so they aren't garbage-collected mid-run.
_tasks: set[asyncio.Task] = set()


def is_call_campaign_running(campaign_id: str) -> bool:
    return campaign_id in _running_campaigns


def request_call_campaign_pause(campaign_id: str):
    """Pause after the call in progress finishes."""
    if campaign_id in _running_campaigns:
        _running_campaigns[campaign_id] = True


def start_call_campaign_task(campaign_id: str):
    task = asyncio.create_task(run_call_campaign(campaign_id))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def resume_running_call_campaigns():
    """On startup, pick up campaigns that were running before a restart/redeploy."""
    for campaign in await list_call_campaigns(status="running"):
        await reset_interrupted_recipients(campaign["id"])
        logger.info(f"Call campaign {campaign['id']}: resuming after restart")
        start_call_campaign_task(campaign["id"])


def in_call_window(campaign: dict, now: datetime | None = None) -> bool:
    now = (now or datetime.now(IST)).astimezone(IST)
    try:
        start = datetime.strptime(campaign.get("call_window_start") or "10:00", "%H:%M").time()
        end = datetime.strptime(campaign.get("call_window_end") or "19:00", "%H:%M").time()
    except ValueError:
        return True
    return start <= now.time() < end


async def _sleep_unless_paused(campaign_id: str, seconds: float):
    """Sleep in short steps so a pause request takes effect quickly."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not _running_campaigns.get(campaign_id, True):
        await asyncio.sleep(min(5, deadline - time.monotonic()))


async def run_call_campaign(campaign_id: str) -> None:
    campaign = await get_call_campaign(campaign_id)
    if not campaign:
        logger.error(f"Call campaign {campaign_id} not found")
        return
    if campaign["status"] not in ("draft", "paused", "running", "completed"):
        logger.warning(f"Call campaign {campaign_id} is {campaign['status']}, cannot start")
        return
    if campaign_id in _running_campaigns:
        return

    _running_campaigns[campaign_id] = False
    now = datetime.now(timezone.utc).isoformat()
    await update_call_campaign(
        campaign_id, status="running", last_error="",
        **({} if campaign.get("started_at") else {"started_at": now}),
    )
    logger.info(f"Call campaign {campaign_id} running ({campaign['name']})")

    consecutive_errors = 0
    waiting_logged = False
    try:
        while True:
            if _running_campaigns.get(campaign_id):
                await update_call_campaign(campaign_id, status="paused")
                logger.info(f"Call campaign {campaign_id} paused")
                break

            campaign = await get_call_campaign(campaign_id)  # pick up edits made while running
            if not campaign:
                break

            if not in_call_window(campaign):
                if not waiting_logged:
                    logger.info(
                        f"Call campaign {campaign_id}: outside call hours "
                        f"{campaign['call_window_start']}-{campaign['call_window_end']} IST, waiting"
                    )
                    waiting_logged = True
                await _sleep_unless_paused(campaign_id, 60)
                continue
            waiting_logged = False

            recipient, retry_at = await next_call_recipient(campaign_id, campaign["max_attempts"])
            if recipient is None:
                if retry_at is None:
                    await update_call_campaign(
                        campaign_id, status="completed",
                        completed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    logger.info(f"Call campaign {campaign_id} completed")
                    break
                wait = (datetime.fromisoformat(retry_at) - datetime.now(timezone.utc)).total_seconds()
                await _sleep_unless_paused(campaign_id, max(5, min(60, wait)))
                continue

            async with _call_slots:
                if _running_campaigns.get(campaign_id):
                    continue
                ok = await _call_one(campaign, recipient)

            consecutive_errors = 0 if ok else consecutive_errors + 1
            if consecutive_errors >= MAX_CONSECUTIVE_API_ERRORS:
                recipient = await get_call_recipient(recipient["id"])
                error = (recipient or {}).get("error_message") or "VoiceLink rejected the calls"
                await update_call_campaign(
                    campaign_id, status="paused",
                    last_error=f"Paused after {consecutive_errors} failed dial attempts: {error}"[:500],
                )
                logger.error(f"Call campaign {campaign_id} auto-paused: {error}")
                break

            await _sleep_unless_paused(campaign_id, GAP_BETWEEN_CALLS_SECS)

    except Exception as e:
        logger.exception(f"Call campaign {campaign_id} runner error")
        await update_call_campaign(campaign_id, status="failed", last_error=str(e)[:500])
    finally:
        _running_campaigns.pop(campaign_id, None)


async def _call_one(campaign: dict, recipient: dict) -> bool:
    """Dial one recipient and wait for the attempt to end. False = VoiceLink API error."""
    rid = recipient["id"]
    phone = recipient["phone"]
    await update_call_recipient(
        rid,
        status="dialing",
        attempts=(recipient.get("attempts") or 0) + 1,
        last_attempt_at=datetime.now(timezone.utc).isoformat(),
        error_message="",
        hangup_reason="",
    )
    params = {
        "direction": "outbound",
        "name": recipient.get("name") or "",
        "purpose": campaign["purpose"],
        "call_campaign_id": campaign["id"],
        "recipient_id": rid,
    }
    logger.info(f"Call campaign {campaign['id']}: dialing {phone} (attempt {(recipient.get('attempts') or 0) + 1})")
    try:
        queue_id = await asyncio.to_thread(voicelink_client.place_call, phone, params)
    except Exception as e:
        logger.error(f"Call campaign {campaign['id']}: dial {phone} failed: {e}")
        await update_call_recipient(rid, error_message=str(e)[:300])
        await mark_attempt_unanswered(rid, "failed", campaign["retry_gap_minutes"])
        return False
    await update_call_recipient(rid, outbound_queue_id=queue_id)

    started = time.monotonic()
    while True:
        await asyncio.sleep(3)
        current = await get_call_recipient(rid)
        if not current or current["status"] not in ACTIVE_RECIPIENT_STATUSES:
            return True
        elapsed = time.monotonic() - started
        if current["status"] == "dialing" and elapsed > DIAL_TIMEOUT_SECS:
            await mark_attempt_unanswered(
                rid, "no_answer", campaign["retry_gap_minutes"], "no answer (timed out)"
            )
            return True
        if elapsed > CALL_MAX_SECS:
            logger.warning(f"Call campaign {campaign['id']}: call to {phone} still open after {CALL_MAX_SECS}s, moving on")
            return True


_CAUSE_CODE = re.compile(r"\b(\d{1,3})\b")


def classify_unanswered(call) -> tuple[str, str]:
    """Map VoiceLink's hangup info (Q.850 cause) to no_answer / busy / failed."""
    cause = " ".join(str(x) for x in (call.hangup_cause, call.hangup_reason) if x)
    text = f"{cause} {call.status or ''} {call.call_status or ''}".lower()
    match = _CAUSE_CODE.search(str(call.hangup_cause or ""))
    code = int(match.group(1)) if match else None
    if code == 17 or "busy" in text:
        return "busy", cause
    # 16 Normal Clearing before our bot connected = rang out or caller-side cancel.
    if code in (16, 18, 19, 21) or "no answer" in text or "noanswer" in text or "cancel" in text:
        return "no_answer", cause
    return "failed", cause


async def handle_voicelink_webhook(event_name: str, call) -> None:
    """Close out a campaign attempt that ended without the bot ever connecting."""
    if event_name not in ("call.ended", "call.completed", "call.failed"):
        return
    params = parse_custom_parameters(call.custom_parameters)
    recipient = None
    if params.get("recipient_id"):
        recipient = await get_call_recipient(str(params["recipient_id"]))
    if not recipient and call.outbound_queue_id:
        recipient = await get_call_recipient_by_queue_id(call.outbound_queue_id)
    if not recipient or recipient["status"] != "dialing":
        return  # not a campaign call, or answered (the bot finalizes those)
    campaign = await get_call_campaign(recipient["campaign_id"])
    status, reason = classify_unanswered(call)
    await mark_attempt_unanswered(
        recipient["id"], status, (campaign or {}).get("retry_gap_minutes", 60), reason
    )
    logger.info(f"Call campaign {recipient['campaign_id']}: {recipient['phone']} -> {status} ({reason})")
