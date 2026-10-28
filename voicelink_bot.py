"""IFS AI Voice Bot — phone calls over VoiceLink

Same agent as bot.py (Gemini Live, same knowledge, prompts, VAD/turn-taking
config and end_call tool), but for normal phone calls on the VoiceLink Indian
number instead of WhatsApp calls:

    caller <-> VoiceLink (PSTN) <-> wss://.../voicelink/ws/<token> <-> Pipecat <-> Gemini Live

VoiceLink streams base64 G.711 A-law @ 8 kHz as JSON frames. The official
VoiceLinkFrameSerializer (pip install "voicelink[pipecat]") converts that to
PCM and back, and turns barge-in interruptions into VoiceLink "clear" events.

Handles both directions:
- inbound:  someone dials the number -> prompt_voice.md (same as WhatsApp)
- outbound: POST /api/voicelink/call -> VoiceLink dials -> prompt_voice_outbound.md

Calls are stored in the same `calls` table (channel='phone'), so they show up
in the dashboard's Calls panel with transcript and recording.
"""

import asyncio
import datetime
import json
import os

from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.frames.frames import EndFrame, LLMRunFrame, OutputTransportMessageUrgentFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)
from voicelink.integrations.pipecat import VoiceLinkFrameSerializer
from voicelink.media.events import StartEvent, build_transfer

import media_storage
from bot import (
    _DEFAULT_VOICE_PROMPT,
    _build_gemini_params,
    _build_user_params,
    _end_call_function,
    save_recording,
)
from chat_db import add_message, get_or_create_conversation
from contacts_db import get_or_create_contact, is_blocked
from db import complete_call_record, create_call_record
from hooks import send_call_summary
from knowledge import load_knowledge, load_prompt
from utils import detect_handoff, extract_topics, generate_id
from whatsapp_messaging import send_followup_message, send_whatsapp_template

_BUSINESS_NAME = os.getenv("BUSINESS_NAME", "Our Business")
_BUSINESS_SHORT = os.getenv("BUSINESS_SHORT", "")
_BUSINESS_CITY = os.getenv("BUSINESS_CITY", "")

_DID_NUMBER = os.getenv("VOICELINK_DID", "")
# Number the AI transfers to when a caller asks for a human. Blank = no
# transfer tool; the AI offers a callback instead (as on WhatsApp calls).
_TRANSFER_NUMBER = os.getenv("VOICELINK_TRANSFER_NUMBER", "")
# Approved WhatsApp template sent after a phone call ({{1}} = caller name).
# Phone callers usually have no open 24h WhatsApp window, so free-form
# follow-ups (send_followup_message) only reach people who recently messaged us.
_FOLLOWUP_TEMPLATE = os.getenv("VOICELINK_FOLLOWUP_TEMPLATE", "")
_FOLLOWUP_TEMPLATE_LANG = os.getenv("VOICELINK_FOLLOWUP_TEMPLATE_LANG", "en")

# Gemini Live takes 16 kHz in and speaks 24 kHz out; the serializer resamples
# VoiceLink's 8 kHz A-law to and from these.
_PIPELINE_IN_RATE = 16000
_PIPELINE_OUT_RATE = 24000

_DEFAULT_OUTBOUND_PROMPT = """You are an AI voice assistant for {business_name}{business_location}, making an outbound call
to {customer_name} about: {purpose}. Introduce yourself and the reason in one or two sentences, keep replies short,
match their language (Hindi or English), only use the knowledge below. When done, say goodbye and then call end_call.

YOUR KNOWLEDGE:
{knowledge}
"""

_TRANSFER_RULE = (
    "\n\nTRANSFERS (this replaces any callback rule above): If the caller asks for a human, "
    "say \"Sure, let me connect you to our team\" and then call the transfer_call function."
)

_transfer_call_function = FunctionSchema(
    name="transfer_call",
    description=(
        "Transfer the phone call to a human team member. Call this AFTER telling the caller "
        "you are connecting them. Use only when they explicitly ask for a human."
    ),
    properties={
        "reason": {"type": "string", "description": "Why the caller needs a human"},
    },
    required=["reason"],
)


def normalize_phone(raw: str | None) -> str:
    """VoiceLink sends 10-digit, 0-prefixed, 91-prefixed or +91 numbers.

    Return 91XXXXXXXXXX — the same form WhatsApp wa_ids use — so a phone caller
    matches their existing WhatsApp contact.
    """
    digits = "".join(c for c in (raw or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10:
        digits = "91" + digits
    return digits


def _is_outbound(start: StartEvent) -> bool:
    params = start.custom_parameters or {}
    direction = str(params.get("direction", "")).lower()
    if direction in ("outbound", "inbound"):
        return direction == "outbound"
    # VoiceLink injects outboundQueueId into custom parameters for add_lead calls.
    if "outboundQueueId" in params:
        return True
    return bool(_DID_NUMBER) and normalize_phone(start.from_number) == normalize_phone(_DID_NUMBER)


def _build_system_prompt(outbound: bool, knowledge: str, customer_name: str, purpose: str) -> str:
    business_label = _BUSINESS_NAME
    if _BUSINESS_SHORT and _BUSINESS_SHORT != _BUSINESS_NAME:
        business_label = f"{_BUSINESS_NAME} ({_BUSINESS_SHORT})"
    values = dict(
        knowledge=knowledge or "No knowledge documents loaded yet.",
        business_name=business_label,
        business_short=_BUSINESS_SHORT or _BUSINESS_NAME,
        business_location=f", {_BUSINESS_CITY}" if _BUSINESS_CITY else "",
        customer_name=customer_name or "the customer",
        purpose=purpose or "a follow-up on their enquiry",
    )
    if outbound:
        prompt = load_prompt("voice_outbound", _DEFAULT_OUTBOUND_PROMPT).format(**values)
    else:
        prompt = load_prompt("voice", _DEFAULT_VOICE_PROMPT).format(**values)
    return prompt + (_TRANSFER_RULE if _TRANSFER_NUMBER else "")


async def _send_phone_followup(phone, name, handoff_requested, transcript, topics, knowledge):
    """WhatsApp follow-up after a phone call (template if configured, else free-form)."""
    if not _FOLLOWUP_TEMPLATE:
        await send_followup_message(
            phone, name, handoff_requested,
            transcript=transcript, topics=topics, knowledge_context=knowledge,
        )
        return
    result = await send_whatsapp_template(
        to_phone=phone,
        template_name=_FOLLOWUP_TEMPLATE,
        language=_FOLLOWUP_TEMPLATE_LANG,
        components=[{"type": "body", "parameters": [{"type": "text", "text": name or "there"}]}],
    )
    if not result.get("success"):
        logger.error(f"Phone follow-up template to {phone} failed: {result.get('error')}")
        return
    # Record it so the inbox/AI has context if the caller replies on WhatsApp.
    contact = await get_or_create_contact(phone, name)
    conv = await get_or_create_conversation(phone, name, contact["id"])
    await add_message(
        conv["id"], "assistant", f"[Template: {_FOLLOWUP_TEMPLATE}] (after phone call)",
        wa_message_id=result.get("wa_message_id", ""), direction="outbound", source="voicelink",
    )


async def run_voicelink_bot(websocket, start: StartEvent, start_raw: str):
    """Run one AI phone call on an already-accepted VoiceLink WebSocket.

    server.py reads frames until VoiceLink's ``start`` event (caller number,
    custom parameters, media format) and passes it in, so the caller is known
    before the pipeline is built.
    """
    call_id = generate_id()
    outbound = _is_outbound(start)
    direction = "outbound" if outbound else "inbound"
    params = start.custom_parameters or {}
    caller_phone = normalize_phone(start.to_number if outbound else start.from_number)
    caller_name = str(params.get("name") or params.get("customer_name") or "")
    purpose = str(params.get("purpose") or "")
    logger.info(
        f"Call {call_id}: phone {direction} {caller_phone} ({caller_name or 'unknown'}) "
        f"voicelink_call_sid={start.call_sid} params={params}"
    )

    if not outbound and caller_phone and await is_blocked(caller_phone):
        logger.info(f"Call {call_id}: BLOCKED caller {caller_phone}, closing stream")
        await websocket.close()
        return

    knowledge_context = load_knowledge()

    # Feed the start frame we already consumed to the serializer so it learns
    # stream_sid and the media format exactly as if it had read it itself.
    serializer = VoiceLinkFrameSerializer()
    await serializer.deserialize(start_raw)

    transport = FastAPIWebsocketTransport(
        websocket=websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            # 20 ms packets (160 bytes of A-law), the telephony norm and what the
            # WhatsApp bot uses (Pipecat's default is 40 ms).
            audio_out_10ms_chunks=2,
            add_wav_header=False,
            serializer=serializer,
        ),
    )

    llm = GeminiLiveLLMService(
        api_key=os.getenv("GOOGLE_API_KEY"),
        voice_id="Kore",  # same voice as WhatsApp calls
        system_instruction=_build_system_prompt(outbound, knowledge_context, caller_name, purpose),
        params=_build_gemini_params(),
    )

    if outbound:
        kickoff = (
            f"The call to {caller_name or 'the customer'} has just been answered. "
            "Greet them and say why you are calling."
        )
    else:
        kickoff = f"A customer is calling {_BUSINESS_SHORT or _BUSINESS_NAME}. Greet them briefly."
    tools = [_end_call_function] + ([_transfer_call_function] if _TRANSFER_NUMBER else [])
    context = LLMContext(
        [{"role": "user", "content": kickoff}], tools=ToolsSchema(standard_tools=tools)
    )
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context, user_params=_build_user_params()
    )
    audiobuffer = AudioBufferProcessor(num_channels=1)

    pipeline = Pipeline(
        [
            transport.input(),
            user_aggregator,
            llm,
            transport.output(),
            audiobuffer,
            assistant_aggregator,
        ]
    )
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=_PIPELINE_IN_RATE,
            audio_out_sample_rate=_PIPELINE_OUT_RATE,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    recording_rel_path = f"recordings/{call_id}.wav"
    state = {
        "connected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "transferred_to": "",
        "finalized": False,
    }

    try:
        await create_call_record(
            call_id, caller_phone, caller_name, state["connected_at"],
            channel="phone", direction=direction, voicelink_call_sid=start.call_sid or "",
        )
    except Exception as e:
        logger.error(f"Call {call_id}: Failed to create DB record: {e}")
    if caller_phone:
        try:
            await get_or_create_contact(caller_phone, caller_name)
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to create contact: {e}")

    async def finalize_call():
        if state["finalized"]:
            return
        state["finalized"] = True
        disconnected_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        try:
            await audiobuffer.stop_recording()
        except Exception as e:
            logger.error(f"Call {call_id}: stop_recording failed: {e}")

        transcript = []
        for msg in context.messages:
            role, content = msg.get("role", ""), msg.get("content", "")
            if role in ("user", "assistant") and content and content != kickoff:
                transcript.append({"role": role, "content": str(content)})
        handoff_requested, handoff_reason = detect_handoff(transcript)
        if state["transferred_to"]:
            handoff_requested, handoff_reason = True, f"Transferred to {state['transferred_to']}"
        topics = extract_topics(transcript)
        duration_seconds = (
            datetime.datetime.fromisoformat(disconnected_at)
            - datetime.datetime.fromisoformat(state["connected_at"])
        ).total_seconds()
        logger.info(
            f"Call {call_id}: phone call ended after {duration_seconds:.0f}s, "
            f"{len(transcript)} turns, topics={topics}, handoff={handoff_requested}"
        )

        if media_storage.is_configured():
            recording_store_path = media_storage.build_recording_key(call_id)
        else:
            recording_store_path = recording_rel_path
        try:
            await complete_call_record(
                call_id,
                disconnected_at=disconnected_at,
                duration_seconds=duration_seconds,
                transcript=json.dumps(transcript, ensure_ascii=False),
                recording_path=recording_store_path,
                handoff_requested=1 if handoff_requested else 0,
                handoff_reason=handoff_reason,
                topics=json.dumps(topics, ensure_ascii=False),
                transferred_to=state["transferred_to"],
                # A completed transfer already reached a human; only flag callbacks.
                status="handoff_pending" if handoff_requested and not state["transferred_to"] else "completed",
            )
        except Exception as e:
            logger.error(f"Call {call_id}: CRITICAL — failed to update DB record: {e}", exc_info=True)

        # Skip follow-ups for calls where nobody spoke (no answer / instant hangup).
        if caller_phone and any(t["role"] == "user" for t in transcript):
            try:
                await _send_phone_followup(
                    caller_phone, caller_name, handoff_requested, transcript, topics, knowledge_context
                )
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to send WhatsApp follow-up: {e}")

        try:
            await send_call_summary({
                "call_id": call_id,
                "channel": "phone",
                "direction": direction,
                "caller_phone": caller_phone,
                "caller_name": caller_name,
                "connected_at": state["connected_at"],
                "disconnected_at": disconnected_at,
                "duration_seconds": duration_seconds,
                "transcript": transcript,
                "handoff_requested": handoff_requested,
                "handoff_reason": handoff_reason,
                "topics": topics,
                "recording_path": recording_rel_path,
            })
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to send to n8n: {e}")

    async def handle_end_call(fn: FunctionCallParams):
        reason = fn.arguments.get("reason", "conversation complete")
        logger.info(f"Call {call_id}: LLM triggered end_call — {reason}")
        await fn.result_callback({"status": "ending_call", "reason": reason})
        # Let the goodbye finish playing, then close the stream so VoiceLink hangs up.
        await asyncio.sleep(3)
        await task.queue_frame(EndFrame())

    async def handle_transfer_call(fn: FunctionCallParams):
        reason = fn.arguments.get("reason", "")
        logger.info(f"Call {call_id}: transferring to {_TRANSFER_NUMBER} — {reason}")
        await fn.result_callback({"status": "transferring"})
        await asyncio.sleep(2)  # let "connecting you now" finish playing
        state["transferred_to"] = _TRANSFER_NUMBER
        await task.queue_frame(
            OutputTransportMessageUrgentFrame(message=build_transfer(_TRANSFER_NUMBER))
        )

    llm.register_function("end_call", handle_end_call)
    if _TRANSFER_NUMBER:
        llm.register_function("transfer_call", handle_transfer_call)

    @audiobuffer.event_handler("on_audio_data")
    async def on_audio_data(buffer, audio, sample_rate, num_channels):
        save_recording(call_id, audio, sample_rate, num_channels)

    @transport.event_handler("on_client_connected")
    async def on_connected(transport_obj, client):
        await audiobuffer.start_recording()
        await task.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport_obj, client):
        logger.info(f"Call {call_id}: VoiceLink disconnected")
        await task.cancel()

    try:
        await PipelineRunner(handle_sigint=False).run(task)
    finally:
        # Runs whether the caller hung up (VoiceLink "stop"), the AI hung up, or
        # the socket dropped — so every phone call is saved exactly once.
        await finalize_call()
