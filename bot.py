"""IFS AI Voice Bot Pipeline

Pipecat pipeline for real-time voice conversation. VOICE_ENGINE picks the AI:
Gemini Live (STT + LLM + TTS in one model, the default) or Sarvam
(Saaras v4 STT -> Sarvam LLM -> Bulbul v4 Flash TTS).

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
from pipecat.turns.user_stop.speech_timeout_user_turn_stop_strategy import (
    SpeechTimeoutUserTurnStopStrategy,
)
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

# How long to wait after the caller pauses before treating their turn as done.
_USER_SPEECH_TIMEOUT = _envf("USER_SPEECH_TIMEOUT", 0.6)

# Gemini "thinking" adds reasoning latency (slow first reply). Off by default for
# a snappy phone receptionist. Set GEMINI_DISABLE_THINKING=false to re-enable.
_DISABLE_THINKING = os.getenv("GEMINI_DISABLE_THINKING", "true").lower() in ("1", "true", "yes")

# Log the effective config at startup so it's never ambiguous which mode is live.
logger.info(
    f"VOICE CONFIG → turn_mode={_TURN_MODE!r} "
    f"vad(confidence={_VAD_CONFIDENCE}, start={_VAD_START_SECS}, "
    f"stop={_VAD_STOP_SECS}, min_volume={_VAD_MIN_VOLUME}) "
    f"gemini_silence_ms={_GEMINI_SILENCE_MS} "
    f"user_speech_timeout={_USER_SPEECH_TIMEOUT} "
    f"thinking={'off' if _DISABLE_THINKING else 'on'}"
)
if _TURN_MODE == "smart":
    logger.warning(
        "VOICE_TURN_MODE=smart is active — this added large latency in testing and "
        "can stall calls. Set VOICE_TURN_MODE=gemini (or unset it) for the fast path."
    )


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
    # "gemini" mode: Gemini's server VAD owns end-of-turn AND barge-in.
    #  - Replace pipecat's default stop strategy (LocalSmartTurnAnalyzerV3) with a
    #    plain VAD/transcript timeout, else Smart Turn fights Gemini → 3s stalls.
    #  - enable_interruptions=False: do NOT let pipecat's VAD broadcast its own
    #    interruption. Gemini Live already detects barge-in via its server VAD and
    #    broadcasts the interruption itself. Having BOTH made the interrupted turn
    #    end via the "None" path so inference never fired and the reply was dropped.
    return LLMUserAggregatorParams(
        vad_analyzer=vad,
        user_turn_strategies=UserTurnStrategies(
            start=[VADUserTurnStartStrategy(enable_interruptions=False)],
            stop=[SpeechTimeoutUserTurnStopStrategy(user_speech_timeout=_USER_SPEECH_TIMEOUT)],
        ),
    )


def _build_gemini_params() -> InputParams:
    """Gemini Live generation + VAD params for the selected turn-taking mode."""
    if _TURN_MODE == "smart":
        # Disable Gemini's server VAD — Pipecat sends activity_start/end signals
        # driven by Silero + Smart Turn, so turn-taking is fully under our control.
        vad = GeminiVADParams(disabled=True)
    else:
        # Use Gemini's NATIVE default server VAD (this is what worked on 0.0.102).
        # Over-tuning it — especially end_sensitivity=LOW — made Gemini reluctant
        # to finalize a turn, so it would not respond to short/fragmented/barge-in
        # speech and the reply was dropped. Only override the silence window if the
        # operator explicitly asks via GEMINI_SILENCE_MS; otherwise leave defaults.
        _silence = os.getenv("GEMINI_SILENCE_MS")
        vad = GeminiVADParams(silence_duration_ms=int(_silence)) if _silence else None
    kwargs = {"vad": vad, "temperature": 0.7}
    if _DISABLE_THINKING:
        # Turn off model "thinking" to cut first-reply latency (the reasoning
        # tokens we saw added several seconds before the bot spoke).
        try:
            from google.genai.types import ThinkingConfig

            kwargs["thinking"] = ThinkingConfig(thinking_budget=0)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"Could not disable Gemini thinking, leaving default: {e}")
    return InputParams(**kwargs)


# --- Voice engine ---
# "gemini" (default): Gemini Live does listening, thinking and speaking in one model.
# "sarvam": Sarvam Saaras v4 (speech-to-text) -> Sarvam LLM -> Bulbul v4 Flash
#   (text-to-speech). Needs SARVAM_API_KEY; no Google key used for calls.
VOICE_ENGINE = os.getenv("VOICE_ENGINE", "gemini").lower()
_SARVAM_STT_MODEL = os.getenv("SARVAM_STT_MODEL", "saaras:v4")
_SARVAM_LLM_MODEL = os.getenv("SARVAM_LLM_MODEL", "sarvam-105b-conversations")
_SARVAM_TTS_MODEL = os.getenv("SARVAM_TTS_MODEL", "bulbul:v4-flash")
# Code-mixed Hindi/English customer-support persona; see Sarvam's Voices guide.
_SARVAM_TTS_SPEAKER = os.getenv("SARVAM_TTS_SPEAKER", "simran_enhi_customer")
_SARVAM_TTS_LANGUAGE = os.getenv("SARVAM_TTS_LANGUAGE", "hi-IN")
_SARVAM_TTS_PACE = _envf("SARVAM_TTS_PACE", 1.0)
# Comma-separated words the caller is likely to say (course names, brand), to
# help speech-to-text spell them right.
_SARVAM_KEYTERMS = [t.strip() for t in os.getenv("SARVAM_KEYTERMS", "").split(",") if t.strip()]

# Gemini speaks for itself; a text LLM's words go to a TTS voice, so it needs these.
_SPOKEN_OUTPUT_RULES = """

SPOKEN OUTPUT (your words are read aloud by a text-to-speech voice):
- Plain spoken sentences only: no markdown, bullet points, emojis, or URLs.
- Write Hindi in Devanagari script and English words in English script.
- Write numbers above four digits with commas (10,000) and say "rupees" instead of the ₹ sign.
"""

logger.info(
    f"VOICE ENGINE → {VOICE_ENGINE}"
    + (
        f" (stt={_SARVAM_STT_MODEL}, llm={_SARVAM_LLM_MODEL}, tts={_SARVAM_TTS_MODEL}, "
        f"speaker={_SARVAM_TTS_SPEAKER}, language={_SARVAM_TTS_LANGUAGE})"
        if VOICE_ENGINE == "sarvam" else ""
    )
)


def build_voice_services(system_instruction: str, greet_on_connect: bool = True):
    """Build the speech/LLM services for a call on the configured VOICE_ENGINE.

    Returns (listen, respond, llm, user_params): ``listen`` goes between the
    transport input and the user aggregator (speech-to-text must run before the
    aggregator, which collects its transcripts into the conversation);
    ``respond`` goes between the user aggregator and the transport output; and
    ``llm`` is where the call's function tools (end_call, transfer_call) are
    registered. Used by both WhatsApp calls (this file) and phone calls
    (voicelink_bot.py).
    """
    if VOICE_ENGINE != "sarvam":
        llm = GeminiLiveLLMService(
            api_key=os.getenv("GOOGLE_API_KEY"),
            voice_id=os.getenv("VOICE_ID", "Kore"),  # Aoede, Charon, Fenrir, Kore, Puck
            system_instruction=system_instruction,
            params=_build_gemini_params(),
            inference_on_context_initialization=greet_on_connect,
        )
        return [], [llm], llm, _build_user_params()

    from pipecat.services.sarvam.llm import SarvamLLMService
    from pipecat.services.sarvam.stt import SarvamRealtimeSTTService
    from pipecat.services.sarvam.tts import SarvamTTSService
    from pipecat.transcriptions.language import Language

    api_key = os.getenv("SARVAM_API_KEY", "")
    # Saaras detects each utterance's language and when the caller has finished
    # speaking (server-side endpointing), and handles barge-in.
    stt = SarvamRealtimeSTTService(
        api_key=api_key,
        keyterms=_SARVAM_KEYTERMS or None,
        settings=SarvamRealtimeSTTService.Settings(model=_SARVAM_STT_MODEL, language=None),
    )
    llm = SarvamLLMService(
        api_key=api_key,
        settings=SarvamLLMService.Settings(
            model=_SARVAM_LLM_MODEL,
            system_instruction=system_instruction + _SPOKEN_OUTPUT_RULES,
            temperature=0.7,
            max_tokens=300,  # replies are a few spoken sentences
        ),
    )
    tts = SarvamTTSService(
        api_key=api_key,
        settings=SarvamTTSService.Settings(
            model=_SARVAM_TTS_MODEL,
            voice=_SARVAM_TTS_SPEAKER,
            language=Language(_SARVAM_TTS_LANGUAGE),
            pace=_SARVAM_TTS_PACE,
        ),
    )
    vad = SileroVADAnalyzer(
        params=VADParams(
            confidence=_VAD_CONFIDENCE,
            start_secs=_VAD_START_SECS,
            stop_secs=_VAD_STOP_SECS,
            min_volume=_VAD_MIN_VOLUME,
        )
    )
    # No user_turn_strategies: the aggregator adopts the ones Saaras recommends.
    return [stt], [llm, tts], llm, LLMUserAggregatorParams(vad_analyzer=vad)


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


def save_recording(call_id: str, audio: bytes, sample_rate: int, num_channels: int):
    """Write a call's audio to recordings/{call_id}.wav and upload it to MinIO if configured.

    Shared by WhatsApp calls (this file) and phone calls (voicelink_bot.py), so the
    dashboard's /api/recordings/{call_id} player works for both.
    """
    recording_path = RECORDINGS_DIR / f"{call_id}.wav"
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

    listen, respond, llm, user_params = build_voice_services(
        system_instruction,
        # When we play our own greeting clip, don't let Gemini also greet on
        # connect — it would double up. Gemini speaks once the caller does.
        greet_on_connect=not use_cached_greeting,
    )

    # Seed the conversation context. If we're playing a cached greeting, record it
    # as the assistant's first turn so the AI has continuity and the transcript is
    # accurate. Otherwise ask the AI to generate the greeting itself.
    if use_cached_greeting:
        initial_messages = [{"role": "assistant", "content": greeting_audio.GREETING_TEXT}]
    else:
        initial_messages = [
            {
                "role": "user",
                "content": os.getenv(
                    "VOICE_GREETING",
                    f"A customer is calling {_BUSINESS_SHORT or _BUSINESS_NAME}. Greet them briefly.",
                ),
            }
        ]
    context = LLMContext(initial_messages, tools=_tools)

    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=user_params,
    )

    # Audio recording processor — captures both user and bot audio
    audiobuffer = AudioBufferProcessor(num_channels=1)

    pipeline = Pipeline(
        [
            transport.input(),
            *listen,
            user_aggregator,
            *respond,
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
        save_recording(call_id, audio, sample_rate, num_channels)

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
            logger.info(
                f"Call {call_id}: playing cached greeting clip ({len(greeting_pcm)} bytes @ {greeting_rate}Hz)"
            )
            await task.queue_frames(
                [OutputAudioRawFrame(audio=greeting_pcm, sample_rate=greeting_rate, num_channels=1)]
            )
        else:
            # No cached clip — fall back to a dynamic Gemini-generated greeting.
            logger.info(f"Call {call_id}: triggering dynamic Gemini greeting (no cached clip)")
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
