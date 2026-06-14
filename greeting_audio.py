"""Cached greeting audio.

Generates a short spoken greeting ONCE via OpenAI TTS and caches the raw PCM
bytes in memory. Playing this clip the instant a call connects removes the
"dead air" while Gemini Live cold-starts — the caller hears a friendly greeting
immediately instead of silence (which is what made callers talk over the bot).

If generation fails or no OpenAI key is set, callers fall back to the dynamic
Gemini greeting (see bot.py), so this can never break a call.
"""

import asyncio
import os

from loguru import logger

# OpenAI TTS returns raw little-endian 16-bit mono PCM at 24 kHz for
# response_format="pcm". The transport resamples to the negotiated call rate.
GREETING_SAMPLE_RATE = 24000

_BUSINESS_NAME = os.getenv("BUSINESS_NAME", "our business")
_BUSINESS_SHORT = os.getenv("BUSINESS_SHORT", "") or _BUSINESS_NAME

# Override the spoken greeting without re-recording anything — just set env var.
GREETING_TEXT = os.getenv(
    "GREETING_TEXT",
    f"Hello! Thank you for calling {_BUSINESS_SHORT}. How can I help you today?",
)
_GREETING_VOICE = os.getenv("GREETING_TTS_VOICE", "shimmer")
_GREETING_MODEL = os.getenv("GREETING_TTS_MODEL", "gpt-4o-mini-tts")
_GREETING_ENABLED = os.getenv("GREETING_ENABLED", "true").lower() in ("1", "true", "yes")

# Module-level cache: generated once, reused for every subsequent call.
_cache = {"pcm": None, "tried": False}
_lock = asyncio.Lock()


async def get_greeting_pcm():
    """Return (pcm_bytes, sample_rate) for the cached greeting, or (None, rate).

    Generated lazily on first call and cached. None means "no cached greeting —
    fall back to the dynamic Gemini greeting".
    """
    if not _GREETING_ENABLED:
        return None, GREETING_SAMPLE_RATE

    async with _lock:
        if _cache["tried"]:
            return _cache["pcm"], GREETING_SAMPLE_RATE
        _cache["tried"] = True

        if not GREETING_TEXT or not os.getenv("OPENAI_API_KEY"):
            logger.info("Greeting TTS skipped (no text or OPENAI_API_KEY); using LLM greeting")
            return None, GREETING_SAMPLE_RATE

        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI()
            async with client.audio.speech.with_streaming_response.create(
                model=_GREETING_MODEL,
                voice=_GREETING_VOICE,
                input=GREETING_TEXT,
                response_format="pcm",
            ) as response:
                chunks = [chunk async for chunk in response.iter_bytes()]
            pcm = b"".join(chunks)
            if pcm:
                _cache["pcm"] = pcm
                logger.info(
                    f"Greeting TTS generated and cached: {len(pcm)} bytes "
                    f"(~{len(pcm) / 2 / GREETING_SAMPLE_RATE:.1f}s) voice={_GREETING_VOICE}"
                )
            else:
                logger.warning("Greeting TTS returned no audio; using LLM greeting")
        except Exception as e:
            logger.error(f"Greeting TTS generation failed, using LLM greeting: {e}")
            _cache["pcm"] = None

        return _cache["pcm"], GREETING_SAMPLE_RATE


async def prewarm():
    """Pre-generate the greeting at startup so even the first call is instant."""
    try:
        await get_greeting_pcm()
    except Exception as e:
        logger.error(f"Greeting prewarm failed: {e}")
