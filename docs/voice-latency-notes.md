# Voice Latency + Reliability Notes

Notes from the June 2026 voice debugging session. Apply the same changes to any
sibling deployment (e.g. the IFS project) — the codebase is the same, just the
env vars differ.

## TL;DR — what we actually did

| Area | Change | Where | Expected effect |
|---|---|---|---|
| Audio output chunking | `audio_out_10ms_chunks=2 -> 1` | `bot.py` `SmallWebRTCTransport` params | ~10 ms first-audio latency |
| Greeting prompt | "Greet them briefly" → exact sentence, no thinking | `bot.py` initial `LLMContext` | ~300 ms |
| Silero VAD | Default → `confidence=0.8, start_secs=0.25, stop_secs=0.6, min_volume=0.6` | `bot.py` `LLMUserAggregatorParams` | ~200 ms snappier turn taking + ignores background noise |
| Gemini Live model | Opt-in via `GEMINI_LIVE_MODEL` env (was hardcoded once, caused outage) | `bot.py` `GeminiLiveLLMService` kwargs | Up to ~400 ms with `gemini-2.5-flash-preview-native-audio-dialog` |
| Voice ID | New `VOICE_ID` env override (default `Kore`) | same kwargs | Cosmetic |

Realistic combined saving: ~800-1000 ms per turn once the model upgrade is
allowed on the project.

## Env vars (all optional)

Add to Coolify env vars only the ones you want to override:

```
# Latency-sensitive Gemini Live model (requires key allowlist).
# Leave blank for Pipecat's default — safest.
GEMINI_LIVE_MODEL=
# Examples that worked for some keys:
#   gemini-2.5-flash-preview-native-audio-dialog
#   gemini-live-2.5-flash-preview
#   gemini-2.0-flash-live-001         (stable fallback)

# Override the pre-formed greeting if BUSINESS_SHORT isn't enough.
# VOICE_GREETING=Say exactly one short sentence: 'Hello, this is IFS, how can I help?'

# Voice (any of Aoede, Charon, Fenrir, Kore, Puck)
VOICE_ID=Kore

# Silero VAD knobs — tune per deployment based on caller-side noise.
VAD_CONFIDENCE=0.8       # ↑ if background noise keeps triggering
VAD_START_SECS=0.25      # ↑ to ignore brief noise spikes
VAD_STOP_SECS=0.6        # ↓ for snappier reply; ↑ to stop cutting callers off
VAD_MIN_VOLUME=0.6       # ↓ if quiet callers aren't picked up
```

## Code changes (commits 02c893e, 755714f)

### `bot.py`

```python
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams   # new

# 10 ms chunks instead of 20 ms
transport = SmallWebRTCTransport(
    webrtc_connection=webrtc_connection,
    params=TransportParams(
        audio_in_enabled=True,
        audio_out_enabled=True,
        audio_out_10ms_chunks=1,                       # was 2
    ),
)

# Opt-in model — only pass if env var is set
gemini_live_kwargs = {
    "api_key": os.getenv("GOOGLE_API_KEY"),
    "voice_id": os.getenv("VOICE_ID", "Kore"),
    "system_instruction": system_instruction,
}
gemini_live_model = os.getenv("GEMINI_LIVE_MODEL", "").strip()
if gemini_live_model:
    gemini_live_kwargs["model"] = gemini_live_model
llm = GeminiLiveLLMService(**gemini_live_kwargs)

# Pre-formed greeting — no "thinking" first
greeting_business = _BUSINESS_SHORT or _BUSINESS_NAME
initial_greeting = os.getenv(
    "VOICE_GREETING",
    f"Say exactly one short sentence to greet the caller: 'Namaste, this is {greeting_business}. How may I help you?'",
)
context = LLMContext([{"role": "user", "content": initial_greeting}], tools=_tools)

# Silero tuned for noisy environments
user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
    context,
    user_params=LLMUserAggregatorParams(
        vad_analyzer=SileroVADAnalyzer(
            params=VADParams(
                confidence=float(os.getenv("VAD_CONFIDENCE", "0.8")),
                start_secs=float(os.getenv("VAD_START_SECS", "0.25")),
                stop_secs=float(os.getenv("VAD_STOP_SECS", "0.6")),
                min_volume=float(os.getenv("VAD_MIN_VOLUME", "0.6")),
            ),
        ),
    ),
)
```

Reference commits on `feature/dues-pdf-campaign`:
- `02c893e perf(voice): pin Gemini Live model, tune VAD for noisy environments`
- `755714f fix(voice): only pass Gemini Live model when explicitly set`

## Gotchas we hit (apply the lessons, not the panic)

### 1. Hardcoding a Gemini Live model broke voice

The first attempt hardcoded `model="gemini-2.5-flash-preview-native-audio-dialog"`
as the default. Most keys aren't allowlisted for it →
`GeminiLiveLLMService.__init__` raised silently inside the background task →
the call connected, no greeting played, transcript showed only the seed turn.

Lesson: model selection must be opt-in via env, never the default.

### 2. Gemini API was disabled on the project

Error `1008 None. Gemini API has not been used in project 581863450386 before
or it is disabled.`

Fix: enable Generative Language API on the project (URL pattern):
```
https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com?project=<PROJECT_ID>
```

The key tied to the project must also have either:
- API restrictions = "Don't restrict key", or
- Generative Language API explicitly allowed in the API restrictions allowlist.

### 3. Prepayment credits depleted

Error `1011 None. Your prepayment credits are depleted.`

Fix: top up at https://ai.studio/projects, or — better for production — link a
Google Cloud Billing account to the project so postpaid billing replaces the
silent prepaid cap:
```
https://console.cloud.google.com/billing?project=<PROJECT_ID>
```

### 4. `GOOGLE_API_KEY` is shared between voice and Drive

Same env var is read by `bot.py` (Gemini Live) and `gdrive.py` (Drive listing /
download for the dues feature). When swapping keys, the new key needs **both**
APIs enabled:
- Generative Language API
- Google Drive API

If you want them split, change `bot.py` to read `GEMINI_API_KEY` first with
`GOOGLE_API_KEY` as fallback (1-line patch — not done yet).

### 5. "Unexpected media stream error" cascade is a downstream symptom

When you see `Received an unexpected media stream error while reading the audio`
repeating at ~100 Hz, the upstream cause is almost always one of:
- Gemini connection failed (look earlier in logs for `1008` / `1011` / `Connection refused`)
- Pipecat version mismatch
- WebRTC handshake timeout

Don't waste time on the cascade — find the real error a few seconds earlier in
the logs.

### 6. Pipecat is unpinned in `requirements.txt`

```
pipecat-ai[google,silero,webrtc]>=0.0.102,<1.0
```

Every Docker rebuild pulls the latest patch. If voice suddenly breaks after a
deploy that didn't touch `bot.py`, that's usually the culprit. Fix:
1. Inside the running container: `pip show pipecat-ai | grep Version`
2. Pin that exact version in `requirements.txt`

## Deployment checklist when you redo this on IFS

1. Verify `GOOGLE_API_KEY` project has:
   - Generative Language API enabled
   - Billing account attached (or sufficient prepaid credits)
   - No API restrictions blocking Generative Language API
2. Decide on `GEMINI_LIVE_MODEL`:
   - Skip the env var → safest default
   - Set to `gemini-2.0-flash-live-001` → small latency win, stable
   - Set to `gemini-2.5-flash-preview-native-audio-dialog` → biggest win, may need allowlist; test first
3. Set `VOICE_GREETING` to the exact sentence (Hindi/English) you want callers to hear.
4. Tune `VAD_*` once you've made 3-5 real test calls. Defaults are a good starting point.
5. Pin Pipecat version after the first successful deploy.
6. Watch the first few real calls in Coolify logs — look for `1008` / `1011` / any `ERROR` from `pipecat.services.google.gemini_live.llm`.

## Outstanding cleanups (not blocking)

- [ ] Migrate `voice_id="Kore"` → `settings=GeminiLiveLLMService.Settings(voice="Kore")` (Pipecat deprecation warning; will become an error in a future version).
- [ ] Optional `GEMINI_API_KEY` split from `GOOGLE_API_KEY`.
- [ ] Pin `pipecat-ai==<version>` once we confirm a known-good version.
