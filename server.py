"""IFS WhatsApp AI Voice Agent + Text Chatbot Server

FastAPI server that:
- Auto-accepts incoming WhatsApp calls -> Gemini Live AI voice agent
- Handles incoming WhatsApp text messages -> GPT-4o text chatbot
- Serves a password-protected dashboard for monitoring calls and chats
- Provides a knowledge file editor for managing AI knowledge base

Receives webhooks forwarded from n8n, handles WebRTC via aiortc,
and posts call/chat summaries back to n8n for automation.
"""

import argparse
import asyncio
import os
import re
import secrets
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import uvicorn
from dotenv import load_dotenv
from fastapi import (
    BackgroundTasks,
    Cookie,
    Depends,
    FastAPI,
    HTTPException,
    Request,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.whatsapp.api import WhatsAppWebhookRequest
from pipecat.transports.whatsapp.client import WhatsAppClient

from bot import run_bot
from chat_db import (
    get_conversation,
    get_recent_conversations,
    resolve_conversation,
)
from chatbot import handle_text_message
from db import get_call, get_recent_calls, get_stats, init_db, resolve_call
from knowledge import KNOWLEDGE_DIR, load_knowledge

load_dotenv(override=True)

# Config
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN")
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_APP_SECRET = os.getenv("WHATSAPP_APP_SECRET", "")
WHATSAPP_WEBHOOK_VERIFICATION_TOKEN = os.getenv("WHATSAPP_WEBHOOK_VERIFICATION_TOKEN", "")
PORT = int(os.getenv("PORT", "7860"))

# Security config
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
SESSION_EXPIRY_HOURS = 24

if not all([WHATSAPP_TOKEN, WHATSAPP_PHONE_NUMBER_ID]):
    missing = [v for v, val in [
        ("WHATSAPP_TOKEN", WHATSAPP_TOKEN),
        ("WHATSAPP_PHONE_NUMBER_ID", WHATSAPP_PHONE_NUMBER_ID),
    ] if not val]
    raise ValueError(f"Missing required env vars: {', '.join(missing)}")

if not WHATSAPP_WEBHOOK_VERIFICATION_TOKEN:
    logger.warning("WHATSAPP_WEBHOOK_VERIFICATION_TOKEN not set — webhook verification may fail")

if not DASHBOARD_PASSWORD:
    logger.warning("DASHBOARD_PASSWORD not set — dashboard auth is DISABLED (dev mode)")

# Ensure directories exist
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
RECORDINGS_DIR = Path(__file__).parent / "recordings"
RECORDINGS_DIR.mkdir(exist_ok=True)
KNOWLEDGE_DIR.mkdir(exist_ok=True)

# Global state
whatsapp_client: Optional[WhatsAppClient] = None
shutdown_event = asyncio.Event()
knowledge_context = ""

# In-memory session store: {token: expiry_datetime}
active_sessions: dict[str, datetime] = {}


# --- Auth helpers ---


def create_session() -> str:
    """Create a new session token and store it."""
    token = secrets.token_urlsafe(32)
    active_sessions[token] = datetime.now(timezone.utc) + timedelta(hours=SESSION_EXPIRY_HOURS)
    # Clean expired sessions
    now = datetime.now(timezone.utc)
    expired = [t for t, exp in active_sessions.items() if exp < now]
    for t in expired:
        del active_sessions[t]
    return token


def verify_session(token: str) -> bool:
    """Check if a session token is valid and not expired."""
    if not token or token not in active_sessions:
        return False
    if active_sessions[token] < datetime.now(timezone.utc):
        del active_sessions[token]
        return False
    return True


async def require_auth(session_token: str = Cookie(default="")):
    """FastAPI dependency that requires a valid session cookie.

    If DASHBOARD_PASSWORD is not set, auth is disabled (dev mode).
    """
    if not DASHBOARD_PASSWORD:
        return  # Auth disabled in dev mode
    if not verify_session(session_token):
        raise HTTPException(status_code=401, detail="Unauthorized")


# --- Knowledge file helpers ---


def validate_knowledge_filename(filename: str) -> str:
    """Validate and sanitize a knowledge filename. Returns sanitized name."""
    if not filename.endswith(".md"):
        raise HTTPException(status_code=400, detail="Filename must end with .md")
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not re.match(r'^[a-zA-Z0-9_\-]+\.md$', filename):
        raise HTTPException(status_code=400, detail="Filename can only contain letters, numbers, hyphens, and underscores")
    return filename


def signal_handler():
    logger.info("Shutdown signal received")
    shutdown_event.set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global whatsapp_client, knowledge_context

    # Initialize database
    await init_db()

    # Load knowledge docs at startup
    knowledge_context = load_knowledge()
    logger.info(f"Knowledge loaded: {len(knowledge_context)} characters")

    async with aiohttp.ClientSession() as session:
        whatsapp_client = WhatsAppClient(
            whatsapp_token=WHATSAPP_TOKEN,
            phone_number_id=WHATSAPP_PHONE_NUMBER_ID,
            session=session,
        )
        logger.info("WhatsApp client initialized")
        try:
            yield
        finally:
            if whatsapp_client:
                await whatsapp_client.terminate_all_calls()
            logger.info("Cleanup done")


app = FastAPI(title="IFS WhatsApp AI Voice Agent", version="3.1.0", lifespan=lifespan)

# Mount static files for dashboard (login page is always accessible,
# but dashboard.html checks auth via JS)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# --- Auth Endpoints (no auth required) ---


@app.post("/auth/login")
async def auth_login(request: Request):
    """Login with dashboard password. Sets session cookie."""
    if not DASHBOARD_PASSWORD:
        return JSONResponse({"status": "ok", "message": "Auth disabled"})

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    password = body.get("password", "")
    if password != DASHBOARD_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid password")

    token = create_session()
    response = JSONResponse({"status": "ok"})
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=SESSION_EXPIRY_HOURS * 3600,
    )
    logger.info("Dashboard login successful")
    return response


@app.post("/auth/logout")
async def auth_logout(session_token: str = Cookie(default="")):
    """Logout and clear session."""
    if session_token in active_sessions:
        del active_sessions[session_token]
    response = JSONResponse({"status": "ok"})
    response.delete_cookie("session_token")
    return response


@app.get("/auth/check")
async def auth_check(session_token: str = Cookie(default="")):
    """Check if current session is valid."""
    if not DASHBOARD_PASSWORD:
        return {"authenticated": True, "auth_required": False}
    if verify_session(session_token):
        return {"authenticated": True, "auth_required": True}
    return JSONResponse({"authenticated": False, "auth_required": True}, status_code=401)


# --- WhatsApp Voice Call Webhooks (no auth — must be open for WhatsApp) ---


@app.get("/")
async def verify_webhook(request: Request):
    """WhatsApp webhook verification (GET)."""
    params = dict(request.query_params)
    try:
        result = await whatsapp_client.handle_verify_webhook_request(
            params=params,
            expected_verification_token=WHATSAPP_WEBHOOK_VERIFICATION_TOKEN,
        )
        logger.info("Webhook verified")
        return result
    except ValueError as e:
        logger.warning(f"Webhook verification failed: {e}")
        raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/")
async def handle_webhook(body: WhatsAppWebhookRequest, background_tasks: BackgroundTasks):
    """Handle incoming WhatsApp call webhooks from n8n.

    Auto-accepts every inbound call and spawns an AI bot session.
    """
    if body.object != "whatsapp_business_account":
        raise HTTPException(status_code=400, detail="Invalid object type")

    logger.info(f"Webhook received: {body.dict()}")

    # Extract caller info from webhook payload
    caller_phone = ""
    caller_name = ""
    try:
        for entry in body.entry:
            for change in entry.changes:
                value = change.value
                if hasattr(value, "contacts") and value.contacts:
                    contact = value.contacts[0]
                    caller_phone = contact.wa_id
                    if hasattr(contact, "profile") and contact.profile:
                        caller_name = contact.profile.name
                    break
            if caller_phone:
                break
    except Exception as e:
        logger.warning(f"Could not extract caller info: {e}")

    logger.info(f"Caller: {caller_phone} ({caller_name})")

    # Reload knowledge on each call (allows updating docs without restart)
    current_knowledge = load_knowledge()

    async def connection_callback(connection: SmallWebRTCConnection):
        try:
            logger.info(f"Auto-accepted call, starting AI bot: {connection.pc_id}")
            background_tasks.add_task(
                run_bot, connection, current_knowledge, caller_phone, caller_name
            )
        except Exception as e:
            logger.error(f"Failed to start bot: {e}")
            try:
                await connection.disconnect()
            except Exception:
                pass

    try:
        result = await whatsapp_client.handle_webhook_request(body, connection_callback)
        return {"status": "success"}
    except ValueError as ve:
        logger.warning(f"Invalid webhook: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")


# --- WhatsApp Text Message Webhook (validated by secret, not session auth) ---


@app.post("/webhook/text")
async def handle_text_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle incoming WhatsApp text message webhooks from n8n.

    Validates WEBHOOK_SECRET if configured. Expects raw WhatsApp webhook JSON.
    """
    # Validate webhook secret if configured
    if WEBHOOK_SECRET:
        header_secret = request.headers.get("X-Webhook-Secret", "")
        query_secret = request.query_params.get("secret", "")
        if header_secret != WEBHOOK_SECRET and query_secret != WEBHOOK_SECRET:
            logger.warning("Text webhook rejected: invalid secret")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info("Text webhook received")

    # Extract sender info and message from WhatsApp webhook format
    sender_phone = ""
    sender_name = ""
    message_text = ""
    wa_message_id = ""

    try:
        entries = body.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})

                # Get sender contact info
                contacts = value.get("contacts", [])
                if contacts:
                    sender_phone = contacts[0].get("wa_id", "")
                    profile = contacts[0].get("profile", {})
                    sender_name = profile.get("name", "")

                # Get message text
                messages = value.get("messages", [])
                if messages:
                    msg = messages[0]
                    wa_message_id = msg.get("id", "")
                    sender_phone = sender_phone or msg.get("from", "")

                    if msg.get("type") == "text":
                        message_text = msg.get("text", {}).get("body", "")

                if message_text:
                    break
            if message_text:
                break
    except Exception as e:
        logger.error(f"Failed to parse text webhook: {e}")
        raise HTTPException(status_code=400, detail="Could not parse message")

    if not message_text or not sender_phone:
        logger.warning("No text message found in webhook payload")
        return {"status": "skipped", "reason": "no text message"}

    logger.info(f"Text from {sender_phone} ({sender_name}): {message_text[:100]}")

    # Reload knowledge (allows live updates)
    current_knowledge = load_knowledge()

    # Handle message in background
    background_tasks.add_task(
        handle_text_message,
        sender_phone,
        sender_name,
        message_text,
        wa_message_id,
        current_knowledge,
    )

    return {"status": "success"}


# --- Protected: Call History API ---


@app.get("/calls", dependencies=[Depends(require_auth)])
async def list_calls(limit: int = 50):
    """List recent calls for monitoring/dashboard."""
    calls = await get_recent_calls(limit)
    return {"calls": calls, "count": len(calls)}


@app.get("/calls/{call_id}", dependencies=[Depends(require_auth)])
async def get_call_detail(call_id: str):
    """Get details for a specific call including transcript."""
    call = await get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    return call


# --- Protected: Conversation API ---


@app.get("/api/conversations", dependencies=[Depends(require_auth)])
async def list_conversations(limit: int = 50):
    """List recent text conversations."""
    conversations = await get_recent_conversations(limit)
    return {"conversations": conversations, "count": len(conversations)}


@app.get("/api/conversations/{conversation_id}", dependencies=[Depends(require_auth)])
async def get_conversation_detail(conversation_id: str):
    """Get conversation detail with all messages."""
    conv = await get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conv


# --- Protected: Resolve Handoffs ---


@app.patch("/api/calls/{call_id}/resolve", dependencies=[Depends(require_auth)])
async def resolve_call_handoff(call_id: str):
    """Mark a call handoff as resolved."""
    call = await get_call(call_id)
    if not call:
        raise HTTPException(status_code=404, detail="Call not found")
    await resolve_call(call_id)
    return {"status": "resolved", "call_id": call_id}


@app.patch("/api/conversations/{conversation_id}/resolve", dependencies=[Depends(require_auth)])
async def resolve_conversation_handoff(conversation_id: str):
    """Mark a chat handoff as resolved."""
    conv = await get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    await resolve_conversation(conversation_id)
    return {"status": "resolved", "conversation_id": conversation_id}


# --- Protected: Dashboard Stats ---


@app.get("/api/stats", dependencies=[Depends(require_auth)])
async def dashboard_stats():
    """Get aggregate stats for the dashboard."""
    return await get_stats()


# --- Protected: Recordings (served through auth endpoint, not static mount) ---


@app.get("/api/recordings/{call_id}", dependencies=[Depends(require_auth)])
async def get_recording(call_id: str):
    """Stream a call recording WAV file. Requires auth."""
    # Sanitize call_id to prevent path traversal
    if ".." in call_id or "/" in call_id or "\\" in call_id:
        raise HTTPException(status_code=400, detail="Invalid call ID")
    recording_path = RECORDINGS_DIR / f"{call_id}.wav"
    if not recording_path.exists():
        raise HTTPException(status_code=404, detail="Recording not found")
    return FileResponse(str(recording_path), media_type="audio/wav")


# --- Protected: Knowledge API ---


@app.get("/api/knowledge", dependencies=[Depends(require_auth)])
async def list_knowledge_files():
    """List all knowledge markdown files."""
    files = []
    for md_file in sorted(KNOWLEDGE_DIR.glob("*.md")):
        stat = md_file.stat()
        files.append({
            "name": md_file.name,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        })
    return {"files": files, "count": len(files)}


@app.get("/api/knowledge/{filename}", dependencies=[Depends(require_auth)])
async def get_knowledge_file(filename: str):
    """Get content of a knowledge file."""
    filename = validate_knowledge_filename(filename)
    file_path = KNOWLEDGE_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    content = file_path.read_text(encoding="utf-8")
    return {"name": filename, "content": content}


@app.put("/api/knowledge/{filename}", dependencies=[Depends(require_auth)])
async def update_knowledge_file(filename: str, request: Request):
    """Update or create a knowledge file."""
    filename = validate_knowledge_filename(filename)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    content = body.get("content", "")
    file_path = KNOWLEDGE_DIR / filename
    file_path.write_text(content, encoding="utf-8")
    logger.info(f"Knowledge file updated: {filename} ({len(content)} chars)")
    return {"status": "saved", "name": filename}


@app.post("/api/knowledge", dependencies=[Depends(require_auth)])
async def create_knowledge_file(request: Request):
    """Create a new knowledge file."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    filename = validate_knowledge_filename(body.get("name", ""))
    content = body.get("content", "")

    file_path = KNOWLEDGE_DIR / filename
    if file_path.exists():
        raise HTTPException(status_code=409, detail="File already exists")

    file_path.write_text(content, encoding="utf-8")
    logger.info(f"Knowledge file created: {filename}")
    return {"status": "created", "name": filename}


@app.delete("/api/knowledge/{filename}", dependencies=[Depends(require_auth)])
async def delete_knowledge_file(filename: str):
    """Delete a knowledge file."""
    filename = validate_knowledge_filename(filename)
    file_path = KNOWLEDGE_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    file_path.unlink()
    logger.info(f"Knowledge file deleted: {filename}")
    return {"status": "deleted", "name": filename}


@app.post("/api/knowledge/{filename}/rename", dependencies=[Depends(require_auth)])
async def rename_knowledge_file(filename: str, request: Request):
    """Rename a knowledge file."""
    filename = validate_knowledge_filename(filename)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    new_name = validate_knowledge_filename(body.get("new_name", ""))
    old_path = KNOWLEDGE_DIR / filename
    new_path = KNOWLEDGE_DIR / new_name

    if not old_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    if new_path.exists():
        raise HTTPException(status_code=409, detail="Target filename already exists")

    old_path.rename(new_path)
    logger.info(f"Knowledge file renamed: {filename} -> {new_name}")
    return {"status": "renamed", "old_name": filename, "new_name": new_name}


# --- Dashboard ---


@app.get("/dashboard")
async def dashboard():
    """Redirect to dashboard HTML."""
    return RedirectResponse(url="/static/dashboard.html")


# --- Health (no auth) ---


@app.get("/health")
async def health():
    """Health check for Coolify."""
    return {"status": "ok", "service": "ifs-voice-agent", "version": "3.1.0"}


async def run_server(host: str, port: int):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    config = uvicorn.Config(app, host=host, port=port, log_config=None)
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    logger.info(f"IFS Voice Agent running on {host}:{port}")

    await shutdown_event.wait()

    if whatsapp_client:
        await whatsapp_client.terminate_all_calls()

    server.should_exit = True
    await server_task


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IFS WhatsApp AI Voice Agent")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    logger.remove(0)
    logger.add(sys.stderr, level="TRACE" if args.verbose else "DEBUG")

    try:
        asyncio.run(run_server(args.host, args.port))
    except KeyboardInterrupt:
        logger.info("Server stopped")
