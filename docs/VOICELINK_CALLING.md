# AI phone calls on the VoiceLink number

The same AI agent that answers WhatsApp calls also answers, and can place, normal phone calls on
the Indian VoiceLink number. Phone calls show up in the dashboard's **Calls** panel next to
WhatsApp calls, marked with a **Phone** badge. They share the same contacts, block list,
transcript, recording, WhatsApp follow-up and n8n hook.

```
Caller ⇄ VoiceLink (PSTN) ⇄ wss://<host>/voicelink/ws/<token> ⇄ Pipecat ⇄ Gemini Live
                         └─ https://<host>/voicelink/webhook/<token>   (call events)
```

VoiceLink streams base64 G.711 A-law at 8 kHz as JSON over a WebSocket. The official
`voicelink[pipecat]` SDK provides `VoiceLinkFrameSerializer`, which converts it to PCM and back.
No SIP is involved. The agent's code is in `voicelink_bot.py`, and it reuses `bot.py`'s Gemini,
VAD and `end_call` setup.

## Behaviour

| | |
|---|---|
| Inbound | Uses `knowledge/prompt_voice.md`, the same prompt as WhatsApp calls. Blocked contacts are dropped. |
| Outbound | `POST /api/voicelink/call` with `{to, name, purpose}`. VoiceLink dials the number, then the AI opens with the purpose (`knowledge/prompt_voice_outbound.md`). |
| Transfer | Only active when `VOICELINK_TRANSFER_NUMBER` is set. A caller who asks for a human is transferred through a VoiceLink `transfer` event. |
| Hang-up | Gemini calls `end_call` after saying goodbye. The server then closes the stream. |
| Records | Rows go into the `calls` table with `channel='phone'`. The server also records a WAV file (and uploads to MinIO if configured), so the dashboard player works. VoiceLink's own recording URL and hangup reason are attached from webhooks. |
| Follow-up | Sends `VOICELINK_FOLLOWUP_TEMPLATE` if set. Otherwise sends the usual free-form follow-up, which only reaches callers who messaged us in the last 24 hours. |
| n8n | Same `call_ended` hook as WhatsApp calls, with extra `channel` and `direction` fields. |

## Outbound calling

You can place outbound calls in three ways:

- **One-off call from the dashboard:** use the **AI Call** button on a contact (Contacts tab) and type
  what the AI should call about.
- **One-off call from another system** (n8n, OneHub): `POST /api/voicelink/call` with
  `Authorization: Bearer <WA_API_KEY>` and the body `{"to": "98XXXXXXXX", "name": "...", "purpose": "..."}`.
- **Call campaigns:** open **Campaigns → AI Calls**. The AI calls a list of people one at a time,
  for any purpose (an event invite, an admissions follow-up, a fee reminder, ...).

### Call campaigns

1. **New Call Campaign.** Fill in these fields:
   - **Purpose**: what the AI opens with, e.g. "invite them to the alumni meet on 15 Nov".
   - **Brief**: details the AI can share and answer from, such as date, venue, fees and FAQs. It
     applies to this campaign only, so the shared knowledge base doesn't need editing for each
     campaign.
   - **WhatsApp template** (optional): sent to each person after a call where they spoke, for
     example one carrying the registration link.
   - **Call hours** (IST, default 10:00–19:00), **attempts per person** (default 2) and **retry gap**
     (default 60 min).
2. **Add people** with **Upload CSV** (a `phone` column and an optional `name` column) or **Add from
   CRM** (filter by stage or tags, e.g. tag `alumni`). Duplicates and invalid numbers are skipped.
3. **Start Calling.** People are dialled one at a time, waiting for each call to end. The number of
   simultaneous calls is `VOICELINK_MAX_CONCURRENT_CALLS`, which defaults to 1 and should equal the
   number of channels you own.

Each person moves through these statuses:

| Status | Meaning |
|---|---|
| pending | Not called yet |
| dialing | VoiceLink is ringing them |
| in call | Talking to the AI |
| completed | Talked. Shows as "wants a callback" if they asked for a person |
| no answer / busy | Retried after the retry gap until the attempts run out |
| failed | VoiceLink refused the dial (shown with the reason) |

Answered calls also appear in the **Calls** tab with transcript and recording.

Other behaviour:

- **Pause** finishes the call in progress, then stops. **Resume** carries on where it left off.
- A campaign that was running when the server restarted (for example on a redeploy) resumes by
  itself.
- If VoiceLink refuses 3 dials in a row (empty wallet, bad credentials, ...), the campaign pauses
  itself and shows the error at the top of the campaign.
- **Export Results** downloads a CSV of every person's outcome.

Before a large campaign, check:

- **Wallet:** the *client* account's wallet pays for calls, at roughly ₹0.50 per minute.
- **Throughput:** about 30 calls an hour per channel.
- **Regulation:** for promotional calling in India, TRAI expects a registered 140-series number.
  Only call people who have a relationship with you and haven't opted out.

## Setup

1. Fill in the VoiceLink block in `.env` (see `.env.example`). Set `PUBLIC_BASE_URL`, then
   `VOICELINK_URL_TOKEN`, `VOICELINK_API_TOKEN` and `VOICELINK_DID`. Phone calling stays
   disabled while `VOICELINK_URL_TOKEN` is blank.
2. Redeploy. The reverse proxy must allow WebSocket upgrades on `/voicelink/ws/`.
3. In the running container, connect the number to the bot (one-time, idempotent). Run the
   first command to inspect the current setup, then the second to apply:
   ```bash
   python voicelink_provision.py --show
   python voicelink_provision.py
   ```
   The script prints the previous routing so you can restore it from the VoiceLink portal.
4. Call the number. For an outbound test (dials a real phone and uses wallet balance):
   ```bash
   curl -X POST https://<host>/api/voicelink/call \
     -H "Authorization: Bearer $WA_API_KEY" -H "Content-Type: application/json" \
     -d '{"to":"98XXXXXXXX","name":"Test","purpose":"a test call"}'
   ```

## Check on the first live calls

VoiceLink's docs and its SDK disagree in places. The code follows the SDK, which is based on
captured calls. Confirm these on the first live calls:

- **Hang-up**: VoiceLink documents no hang-up event from the bot side. Check that the call drops
  when the AI says goodbye.
- **Transfer**: the SDK marks the `{"event":"transfer","target":"..."}` event as unverified.
  Test it once with a 10-digit mobile.
- **Webhook shape**: all raw events are stored. Read them at `GET /api/voicelink/events` and
  check that the recording URL and hangup reason are attached to the call.

## Troubleshooting (from VoiceLink's SDK docs)

| Symptom | Fix |
|---|---|
| Call answers, then drops in under 1 s (cause 32) | Turn off the bot's "WebSocket API" toggle in the portal, or the wss host isn't reachable. |
| Outbound fails with "38 – Network out of order" | Number format. The code already sends the national number with `country_code=91`. |
| Answered but silent | The app is up but the AI isn't running. Check the logs and `GOOGLE_API_KEY`. |
| Logs show alaw @ 8000 though the bot is set to l16_16k | Normal. VoiceLink ignores that setting and the serializer adapts. |
