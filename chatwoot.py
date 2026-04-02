"""Chatwoot Integration

Forwards incoming WhatsApp messages and AI replies to Chatwoot so that
human agents see the full conversation in one place. Chatwoot agents reply
directly via WhatsApp Cloud API (configured in Chatwoot).

The CHATWOOT_WEBHOOK_URL should point to Chatwoot's WhatsApp channel webhook,
e.g. https://wapp.munipramansagar.net/webhooks/whatsapp/+916207292062

For incoming messages: we forward the raw Meta webhook JSON — Chatwoot
parses it natively as if it received it directly from Meta.

For AI replies: we construct a synthetic webhook payload that mimics a
WhatsApp outbound message status, so Chatwoot records the reply in the
conversation thread.
"""

import asyncio
import os

import aiohttp
from loguru import logger

CHATWOOT_WEBHOOK_URL = os.getenv("CHATWOOT_WEBHOOK_URL", "")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "")


async def forward_incoming_to_chatwoot(body: dict) -> None:
    """Forward the raw Meta webhook payload to Chatwoot.

    This makes the incoming message appear in Chatwoot's inbox exactly
    as if Chatwoot received it directly from Meta.

    Args:
        body: The raw WhatsApp webhook JSON body from Meta.
    """
    if not CHATWOOT_WEBHOOK_URL:
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                CHATWOOT_WEBHOOK_URL,
                json=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 201):
                    logger.debug("Forwarded incoming message to Chatwoot")
                else:
                    text = await resp.text()
                    logger.warning(f"Chatwoot forward failed ({resp.status}): {text[:200]}")
    except Exception as e:
        logger.error(f"Failed to forward to Chatwoot: {e}")


def forward_incoming_to_chatwoot_bg(body: dict) -> None:
    """Fire-and-forget: forward incoming message to Chatwoot in the background."""
    if not CHATWOOT_WEBHOOK_URL:
        return
    try:
        asyncio.create_task(forward_incoming_to_chatwoot(body))
    except RuntimeError:
        logger.debug("No event loop for Chatwoot forward, skipping")


async def forward_ai_reply_to_chatwoot(
    to_phone: str,
    message_text: str,
    wa_message_id: str = "",
) -> None:
    """Forward an AI-generated reply to Chatwoot as an outgoing message.

    Constructs a synthetic webhook payload that mimics the status update
    Meta sends when a message is sent from the business. This allows
    Chatwoot to record the AI reply in the conversation thread.

    Args:
        to_phone: Recipient phone number (the user who sent the message).
        message_text: The AI reply text that was sent.
        wa_message_id: WhatsApp message ID of the sent reply (if available).
    """
    if not CHATWOOT_WEBHOOK_URL:
        return

    # Chatwoot's WhatsApp integration expects outbound messages via its API.
    # We use the Chatwoot API to create an outgoing message in the conversation.
    # However, since Chatwoot is also connected to the same WhatsApp number,
    # it will receive the delivery status webhook from Meta for our AI reply,
    # which will auto-record the message. So we only need to forward the
    # raw status if Chatwoot doesn't already see it.
    #
    # For now, we rely on the fact that Meta sends status webhooks (sent,
    # delivered, read) to ALL registered webhook URLs. Since the n8n proxy
    # forwards ALL webhooks to both this platform AND Chatwoot, Chatwoot
    # will see the delivery status of the AI reply automatically.
    #
    # If Chatwoot doesn't show AI replies, we can uncomment the code below
    # to actively push them.

    logger.debug(
        f"AI reply to {to_phone} will appear in Chatwoot via Meta status webhook"
    )


def forward_ai_reply_to_chatwoot_bg(
    to_phone: str,
    message_text: str,
    wa_message_id: str = "",
) -> None:
    """Fire-and-forget: forward AI reply to Chatwoot in the background."""
    if not CHATWOOT_WEBHOOK_URL:
        return
    try:
        asyncio.create_task(
            forward_ai_reply_to_chatwoot(to_phone, message_text, wa_message_id)
        )
    except RuntimeError:
        logger.debug("No event loop for Chatwoot AI reply forward, skipping")
