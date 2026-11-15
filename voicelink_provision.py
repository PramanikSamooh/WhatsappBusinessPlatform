"""One-time VoiceLink setup: register the AI bot and route the DID to it.

    python voicelink_provision.py            # inbound + outbound
    python voicelink_provision.py --inbound  # only answer incoming calls with AI
    python voicelink_provision.py --show     # print current bot/routing, change nothing

Safe to re-run (idempotent). Prints the previous routing so it can be
restored from the VoiceLink portal (Call Routing) if needed.
"""

import argparse
import os

from voicelink.enums import AudioFormat
from voicelink.integrations.pipecat import PipecatProvisioner

import voicelink_client

BOT_NAME = os.getenv("VOICELINK_BOT_NAME", "AI Receptionist")
# VoiceLink streams A-law @ 8 kHz regardless of this setting, and the bot replies in
# the same format - registering anything else (the SDK default is l16_16k) made
# VoiceLink play the replies as garbled noise.
AUDIO_FORMAT = AudioFormat(os.getenv("VOICELINK_AUDIO_FORMAT", "alaw"))


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--inbound", action="store_true", help="route inbound only")
    g.add_argument("--outbound", action="store_true", help="route outbound only")
    g.add_argument("--show", action="store_true", help="show current config, change nothing")
    args = ap.parse_args()

    did = voicelink_client.DID_NUMBER
    if not did or not voicelink_client.is_enabled():
        raise SystemExit("Set VOICELINK_DID and VOICELINK_URL_TOKEN in .env first")

    with voicelink_client.get_client() as vl:
        prov = PipecatProvisioner(vl)
        if args.show:
            print("Bot:", prov.find_bot(BOT_NAME))
            print(f"Routing for {did}:", prov.find_routing(did))
            return

        ws_url, webhook_url = voicelink_client.public_urls()
        print(f"Bot name : {BOT_NAME}\nAudio    : {AUDIO_FORMAT.value}\nDID      : {did}\nMedia    : {ws_url}\nWebhook  : {webhook_url}\n")
        if not args.outbound:
            p = prov.setup_inbound(bot_name=BOT_NAME, websocket_url=ws_url, did=did, webhook_url=webhook_url,
                                    audio_format=AUDIO_FORMAT)
            print(f"Inbound  -> bot {p.bot.id} ({'created' if p.bot_created else 'updated'}), routing {p.routing.id}")
            print(f"  previous routing: {p.previous_routing}")
        if not args.inbound:
            p = prov.setup_outbound(bot_name=BOT_NAME, websocket_url=ws_url, did=did, webhook_url=webhook_url,
                                    audio_format=AUDIO_FORMAT)
            print(f"Outbound -> bot {p.bot.id}, routing {p.routing.id}")
            print(f"  previous routing: {p.previous_routing}")


if __name__ == "__main__":
    main()
