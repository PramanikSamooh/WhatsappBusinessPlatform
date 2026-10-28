"""VoiceLink control-plane config (phone calls on the Indian number).

Used by server.py (outbound calls) and voicelink_provision.py (one-time setup).
VoiceLink calling is enabled only when VOICELINK_URL_TOKEN is set.
"""

import os

from dotenv import load_dotenv
from voicelink import VoiceLinkClient
from voicelink.integrations.pipecat import PipecatProvisioner

load_dotenv(override=True)

URL_TOKEN = os.getenv("VOICELINK_URL_TOKEN", "")
DID_NUMBER = os.getenv("VOICELINK_DID", "")


def is_enabled() -> bool:
    return bool(URL_TOKEN)


def get_client() -> VoiceLinkClient:
    client_id = int(os.environ["VOICELINK_CLIENT_ID"]) if os.getenv("VOICELINK_CLIENT_ID") else None
    token = os.getenv("VOICELINK_API_TOKEN")
    if token:
        return VoiceLinkClient(token, client_id=client_id)
    username, password = os.getenv("VOICELINK_USERNAME"), os.getenv("VOICELINK_PASSWORD")
    if username and password:
        return VoiceLinkClient.login(username, password, client_id=client_id)
    raise RuntimeError("Set VOICELINK_API_TOKEN, or VOICELINK_USERNAME + VOICELINK_PASSWORD")


def public_urls() -> tuple[str, str]:
    """The wss:// media URL and https:// webhook URL VoiceLink should call."""
    base = os.environ["PUBLIC_BASE_URL"].rstrip("/")  # e.g. https://wa.financialskills.in
    wss = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return f"{wss}/voicelink/ws/{URL_TOKEN}", f"{base}/voicelink/webhook/{URL_TOKEN}"


def place_call(customer_number: str, custom_parameters: dict) -> int | None:
    """Ask VoiceLink to dial a number and connect it to the AI. Returns the queue id.

    Blocking (httpx) — call via asyncio.to_thread from async code.
    """
    ws_url, webhook_url = public_urls()
    with get_client() as vl:
        lead = PipecatProvisioner(vl).place_call(
            did=DID_NUMBER,
            customer_number=customer_number,
            country_code="91",  # VoiceLink needs the national number + country code separately
            custom_parameters=custom_parameters,
            websocket_url=ws_url,
            webhook_url=webhook_url,
        )
    return lead.outbound_queue_id
