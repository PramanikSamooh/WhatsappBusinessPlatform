"""IFS AI Voice Bot Pipeline

Pipecat pipeline using Gemini Live for real-time voice conversation.
Gemini handles STT + LLM + TTS natively in a single model.

Phase 2: Adds conversation transcription, call recording, handoff detection,
SQLite storage, WhatsApp follow-up messaging, and enriched n8n hooks.
Phase 3: Auto-hangup after bot says goodbye via LLM function calling.
"""

import asyncio
import datetime
import json
import os
import wave
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import EndFrame, LLMRunFrame, OutputAudioRawFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
    UserTurnStrategies,
)
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
from pipecat.services.google.gemini_live.llm import (
    EndSensitivity,
    GeminiVADParams,
    GeminiLiveLLMService,
    InputParams,
    StartSensitivity,
)
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.base_transport import TransportParams
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.turns.user_start.vad_user_turn_start_strategy import VADUserTurnStartStrategy
from pipecat.turns.user_stop.turn_analyzer_user_turn_stop_strategy import (
    TurnAnalyzerUserTurnStopStrategy,
)

import greeting_audio
import media_storage
from contacts_db import get_or_create_contact, is_blocked
from db import complete_call_record, create_call_record
from hooks import send_call_summary
from knowledge import load_prompt
from utils import detect_handoff, extract_topics, generate_id
from whatsapp_messaging import send_followup_message

load_dotenv(override=True)

# Ensure recordings directory exists
RECORDINGS_DIR = Path(__file__).parent / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)

# Branding from env vars (white-label)
_BUSINESS_NAME = os.getenv("BUSINESS_NAME", "Our Business")
_BUSINESS_SHORT = os.getenv("BUSINESS_SHORT", "")
_BUSINESS_CITY = os.getenv("BUSINESS_CITY", "")


def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


# --- Turn-taking / VAD tuning (env-overridable) ---
# Mode "gemini" (default): use Gemini Live's own fast server-side VAD for
#   turn-taking, tuned to be less trigger-happy on background noise and to wait
#   a bit longer before deciding the caller is done (no mid-sentence cutoffs).
#   Silero VAD still runs locally to drive barge-in/interruption. This is the
#   low-latency path that suits a speech-to-speech model like Gemini Live.
# Mode "smart": make Pipecat the turn-taking authority (Silero + Smart Turn v3)
#   and disable Gemini's server VAD. NOTE: in testing this added large latency
#   (Smart Turn's semantic model repeatedly judged turns "incomplete" and only
#   completed via a 3s silence timeout), so it is NOT the default. Only use it
#   if you specifically want Pipecat-side turn control and accept the latency.
_TURN_MODE = os.getenv("VOICE_TURN_MODE", "gemini").lower()

# Silero VAD params — higher confidence/min_volume = ignore more background noise.
_VAD_CONFIDENCE = _envf("VAD_CONFIDENCE", 0.8)
_VAD_START_SECS = _envf("VAD_START_SECS", 0.2)
_VAD_STOP_SECS = _envf("VAD_STOP_SECS", 0.2)
_VAD_MIN_VOLUME = _envf("VAD_MIN_VOLUME", 0.7)

# Gemini server-side VAD silence window (only used in "gemini" mode), in ms.
_GEMINI_SILENCE_MS = int(_envf("GEMINI_SILENCE_MS", 800))


def _build_user_params() -> LLMUserAggregatorParams:
    """User-turn aggregation params, wired for the selected turn-taking mode."""
    vad = SileroVADAnalyzer(
        params=VADParams(
            confidence=_VAD_CONFIDENCE,
            start_secs=_VAD_START_SECS,
            stop_secs=_VAD_STOP_SECS,
            min_volume=_VAD_MIN_VOLUME,
        )
    )
    if _TURN_MODE == "smart":
        # Smart Turn v3 (model bundled with pipecat) decides true end-of-turn.
        return LLMUserAggregatorParams(
            vad_analyzer=vad,
            user_turn_strategies=UserTurnStrategies(
                start=[VADUserTurnStartStrategy(enable_interruptions=True)],
                stop=[
                    TurnAnalyzerUserTurnStopStrategy(
                        turn_analyzer=LocalSmartTurnAnalyzerV3()
                    )
                ],
            ),
        )
    # "gemini" mode: Silero only drives local interruption/barge-in detection;
    # Gemini's server VAD owns end-of-turn (tuned below in _build_gemini_params).
    return LLMUserAggregatorParams(vad_analyzer=vad)


def _build_gemini_params() -> InputParams:
    """Gemini Live generation + VAD params for the selected turn-taking mode."""
    if _TURN_MODE == "smart":
        # Disable Gemini's server VAD — Pipecat sends activity_start/end signals
        # driven by Silero + Smart Turn, so turn-taking is fully under our control.
        vad = GeminiVADParams(disabled=True)
    else:
        # Tune Gemini's server VAD: LOW sensitivity = less likely to trigger on
        # background noise or end a turn early; longer silence window before ending.
        vad = GeminiVADParams(
            start_sensitivity=StartSensitivity.START_SENSITIVITY_LOW,
            end_sensitivity=EndSensitivity.END_SENSITIVITY_LOW,
            prefix_padding_ms=300,
            silence_duration_ms=_GEMINI_SILENCE_MS,
        )
    return InputParams(vad=vad, temperature=0.7)

# Hardcoded fallback — only used if knowledge/prompt_voice.md is missing
_DEFAULT_VOICE_PROMPT = """You are an AI voice assistant for {business_name}{business_location}.

IMPORTANT RULES:
- You are answering a live phone call. Keep responses brief (1-3 sentences).
- Respond in the SAME LANGUAGE the caller speaks (Hindi or English).
- Only answer from the knowledge provided below. Never make up information.
- When the caller asks for details hard to convey on a call, tell them you will send it on WhatsApp after the call.
- ENDING CALLS: When the conversation is naturally ending (the caller says goodbye, thanks you, or indicates they have no more questions), say your final goodbye message and then call the end_call function. Do NOT call end_call until you have spoken your goodbye.

YOUR KNOWLEDGE:
{knowledge}
"""

# end_call function tool — lets Gemini signal when the call should end
_end_call_function = FunctionSchema(
    name="end_call",
    description=(
        "End the phone call. Call this function AFTER you have said your final goodbye "
        "message to the caller. Use when the conversation is complete, the caller says "
        "goodbye, or the caller has no more questions."
    ),
    properties={
        "reason": {
            "type": "string",
            "description": "Brief reason for ending the call, e.g. 'caller said goodbye', "
                           "'conversation complete', 'caller has no more questions'",
        },
    },
    required=["reason"],
)
_tools = ToolsSchema(standard_tools=[_end_call_function])

async def run_bot(
    webrtc_connection,
    knowledge_context: str = "",
    caller_phone: str = "",
    caller_name: str = "",
):
    """Run the AI voice bot for a single WhatsApp call.

    Captures transcript, records audio, detects handoff requests,
    stores everything in SQLite, and sends post-call notifications.
    """
    call_id = generate_id()
    logger.info(f"Call {call_id}: Starting bot for {caller_phone} ({caller_name})")

    voice_prompt = load_prompt("voice", _DEFAULT_VOICE_PROMPT)
    business_location = f", {_BUSINESS_CITY}" if _BUSINESS_CITY else ""
    business_label = _BUSINESS_NAME
    if _BUSINESS_SHORT and _BUSINESS_SHORT != _BUSINESS_NAME:
        business_label = f"{_BUSINESS_NAME} ({_BUSINESS_SHORT})"
    system_instruction = voice_prompt.format(
        knowledge=knowledge_context or "No knowledge documents loaded yet.",
        business_name=business_label,
        business_short=_BUSINESS_SHORT or _BUSINESS_NAME,
        business_location=business_location,
    )

    transport = SmallWebRTCTransport(
        webrtc_connection=webrtc_connection,
        params=TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_out_10ms_chunks=2,
        ),
    )

    # Pre-generated greeting clip (cached). If present we play it the instant the
    # call connects so the caller never hits dead air; Gemini then takes over.
    greeting_pcm, greeting_rate = await greeting_audio.get_greeting_pcm()
    use_cached_greeting = greeting_pcm is not None

    llm = GeminiLiveLLMService(
        api_key=os.getenv("GOOGLE_API_KEY"),
        voice_id="Kore",  # Options: Aoede, Charon, Fenrir, Kore, Puck
        system_instruction=system_instruction,
        params=_build_gemini_params(),
        # When we play our own greeting clip, don't let Gemini also greet on
        # connect — it would double up. Gemini speaks once the caller does.
        inference_on_context_initialization=not use_cached_greeting,
    )

    # Seed the conversation context. If we're playing a cached greeting, record it
    # as the assistant's first turn so Gemini has continuity and the transcript is
    # accurate. Otherwise ask Gemini to generate the greeting itself.
    if use_cached_greeting:
        initial_messages = [{"role": "assistant", "content": greeting_audio.GREETING_TEXT}]
    else:
        initial_messages = [
            {
                "role": "user",
                "content": f"A customer is calling {_BUSINESS_SHORT or _BUSINESS_NAME}. Greet them briefly.",
            }
        ]
    context = LLMContext(initial_messages, tools=_tools)

    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=_build_user_params(),
    )

    # Audio recording processor — captures both user and bot audio
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
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # --- Shared finalize logic (called on both client disconnect and auto-hangup) ---
    async def finalize_call():
        nonlocal _finalized
        if _finalized:
            logger.info(f"Call {call_id}: Already finalized, skipping")
            return
        _finalized = True

        call_metadata["disconnected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Call {call_id}: Finalizing call")

        transcript = []
        handoff_requested = False
        handoff_reason = ""
        topics = []
        duration_seconds = 0.0

        try:
            await audiobuffer.stop_recording()

            try:
                messages = context.messages
                for msg in messages:
                    role = msg.get("role", "")
                    content = msg.get("content", "")
                    if role in ("user", "assistant") and content:
                        transcript.append({"role": role, "content": str(content)})
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to extract transcript: {e}")

            logger.info(f"Call {call_id}: Transcript has {len(transcript)} turns")

            handoff_requested, handoff_reason = detect_handoff(transcript)
            if handoff_requested:
                logger.info(f"Call {call_id}: HANDOFF DETECTED — {handoff_reason}")

            topics = extract_topics(transcript)
            logger.info(f"Call {call_id}: Topics: {topics}")

            try:
                from dateutil.parser import isoparse
                t1 = isoparse(call_metadata["connected_at"])
                t2 = isoparse(call_metadata["disconnected_at"])
                duration_seconds = (t2 - t1).total_seconds()
            except Exception:
                pass

            if media_storage.is_configured():
                recording_store_path = media_storage.build_recording_key(call_id)
            else:
                recording_store_path = recording_rel_path

            await complete_call_record(
                call_id,
                disconnected_at=call_metadata["disconnected_at"],
                duration_seconds=duration_seconds,
                transcript=json.dumps(transcript, ensure_ascii=False),
                recording_path=recording_store_path,
                handoff_requested=1 if handoff_requested else 0,
                handoff_reason=handoff_reason,
                topics=json.dumps(topics, ensure_ascii=False),
                status="handoff_pending" if handoff_requested else "completed",
            )
            logger.info(f"Call {call_id}: DB record updated")

        except Exception as e:
            logger.error(f"Call {call_id}: CRITICAL — finalize_call failed: {e}", exc_info=True)
            try:
                await complete_call_record(
                    call_id,
                    disconnected_at=call_metadata["disconnected_at"],
                    duration_seconds=duration_seconds,
                    transcript=json.dumps(transcript, ensure_ascii=False) if transcript else "[]",
                    recording_path="",
                    handoff_requested=0,
                    handoff_reason="",
                    topics="[]",
                    status="completed",
                )
            except Exception:
                pass

        # Post-call actions (non-critical)
        if caller_phone:
            try:
                await send_followup_message(
                    caller_phone, caller_name, handoff_requested,
                    transcript=transcript, topics=topics,
                    knowledge_context=knowledge_context,
                )
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to send WhatsApp text: {e}")

        try:
            await send_call_summary({
                "call_id": call_id,
                "caller_phone": caller_phone,
                "caller_name": caller_name,
                "connected_at": call_metadata["connected_at"],
                "disconnected_at": call_metadata["disconnected_at"],
                "duration_seconds": duration_seconds,
                "transcript": transcript,
                "handoff_requested": handoff_requested,
                "handoff_reason": handoff_reason,
                "topics": topics,
                "recording_path": recording_rel_path,
            })
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to send to n8n: {e}")

    # Register end_call function — Gemini calls this to hang up after goodbye
    async def handle_end_call(params: FunctionCallParams):
        reason = params.arguments.get("reason", "conversation complete")
        logger.info(f"Call {call_id}: LLM triggered end_call — {reason}")
        await params.result_callback({"status": "ending_call", "reason": reason})
        # Wait for goodbye audio to finish playing
        await asyncio.sleep(3)
        logger.info(f"Call {call_id}: Auto-hangup executing")
        # End the pipeline first — on_disconnected will handle saving
        await task.queue_frame(EndFrame())
        # If on_disconnected doesn't fire within 5s, force finalize
        await asyncio.sleep(5)
        if not _finalized:
            logger.warning(f"Call {call_id}: on_disconnected did not fire, force-finalizing")
            await finalize_call()

    llm.register_function("end_call", handle_end_call)

    # Call session state
    recording_filename = f"{call_id}.wav"
    recording_rel_path = f"recordings/{recording_filename}"
    call_metadata = {
        "call_id": call_id,
        "caller_phone": caller_phone,
        "caller_name": caller_name,
        "connected_at": None,
        "disconnected_at": None,
    }
    _finalized = False  # Guard against double-finalize

    @audiobuffer.event_handler("on_audio_data")
    async def on_audio_data(buffer, audio, sample_rate, num_channels):
        """Save recorded audio to WAV file when recording stops."""
        recording_path = RECORDINGS_DIR / recording_filename
        logger.info(f"Call {call_id}: on_audio_data fired — {len(audio) if audio else 0} bytes, {sample_rate}Hz, {num_channels}ch")
        if not audio:
            logger.warning(f"Call {call_id}: Empty audio buffer, skipping save")
            return
        try:
            with wave.open(str(recording_path), "wb") as wf:
                wf.setnchannels(num_channels)
                wf.setsampwidth(2)  # 16-bit audio
                wf.setframerate(sample_rate)
                wf.writeframes(audio)
            logger.info(f"Call {call_id}: Audio saved to {recording_path} ({recording_path.stat().st_size} bytes)")

            # Upload to MinIO if configured
            if media_storage.is_configured():
                key = media_storage.build_recording_key(call_id)
                with open(str(recording_path), "rb") as f:
                    wav_bytes = f.read()
                if media_storage.upload_bytes(key, wav_bytes, "audio/wav"):
                    logger.info(f"Call {call_id}: Recording uploaded to MinIO: {key}")
                else:
                    logger.warning(f"Call {call_id}: MinIO upload failed, keeping local copy")
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to save audio: {e}")

    @transport.event_handler("on_client_connected")
    async def on_connected(transport_obj, client):
        call_metadata["connected_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        logger.info(f"Call {call_id}: Connected from {caller_phone}")

        # Block check: disconnect blocked callers immediately
        if caller_phone:
            try:
                if await is_blocked(caller_phone):
                    logger.info(f"Call {call_id}: BLOCKED caller {caller_phone}, disconnecting")
                    await task.queue_frame(EndFrame())
                    return
            except Exception as e:
                logger.error(f"Call {call_id}: Block check failed: {e}")

        # Start audio recording + greet IMMEDIATELY
        # (don't let DB/contact ops delay the greeting)
        await audiobuffer.start_recording()
        if use_cached_greeting:
            # Play the pre-generated greeting clip instantly — zero cold-start
            # latency, so the caller hears us before they start talking.
            await task.queue_frames(
                [OutputAudioRawFrame(audio=greeting_pcm, sample_rate=greeting_rate, num_channels=1)]
            )
        else:
            # No cached clip — fall back to a dynamic Gemini-generated greeting.
            await task.queue_frames([LLMRunFrame()])

        # Housekeeping — runs after greeting is already queued
        try:
            await create_call_record(
                call_id, caller_phone, caller_name, call_metadata["connected_at"]
            )
        except Exception as e:
            logger.error(f"Call {call_id}: Failed to create DB record: {e}")

        if caller_phone:
            try:
                await get_or_create_contact(caller_phone, caller_name or "")
                logger.info(f"Call {call_id}: Contact ensured for {caller_phone}")
            except Exception as e:
                logger.error(f"Call {call_id}: Failed to create contact: {e}")

    @transport.event_handler("on_client_disconnected")
    async def on_disconnected(transport_obj, client):
        logger.info(f"Call {call_id}: Client disconnected")
        await finalize_call()
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)
