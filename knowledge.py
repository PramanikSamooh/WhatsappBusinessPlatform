"""Knowledge Base Loader

Reads all markdown files from the knowledge/ directory and combines them
into a single context string for the AI agent's system prompt.

Uses in-memory caching with a 60-second TTL so docs can be updated via
the dashboard without restarting, while avoiding disk reads on every webhook.
"""

import os
import time
from pathlib import Path

from loguru import logger

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

# Cache with TTL
_cache: dict = {"knowledge": "", "prompts": {}, "timestamp": 0}
_CACHE_TTL = 60  # seconds — re-read from disk at most once per minute


def load_knowledge() -> str:
    """Load all .md files from knowledge/ directory, excluding prompt_* files.

    Results are cached for 60 seconds to avoid disk reads on every webhook.

    Returns:
        Combined content of all knowledge documents.
    """
    now = time.time()
    if _cache["knowledge"] and (now - _cache["timestamp"]) < _CACHE_TTL:
        return _cache["knowledge"]

    if not KNOWLEDGE_DIR.exists():
        logger.warning(f"Knowledge directory not found: {KNOWLEDGE_DIR}")
        return ""

    documents = []
    for md_file in sorted(KNOWLEDGE_DIR.glob("*.md")):
        # Skip prompt files — they are loaded separately via load_prompt()
        if md_file.name.startswith("prompt_"):
            continue
        try:
            content = md_file.read_text(encoding="utf-8").strip()
            if content:
                documents.append(f"--- {md_file.stem.upper()} ---\n{content}")
        except Exception as e:
            logger.error(f"Failed to read {md_file}: {e}")

    if not documents:
        logger.warning("No knowledge documents found")
        return ""

    combined = "\n\n".join(documents)
    _cache["knowledge"] = combined
    _cache["timestamp"] = now
    logger.info(f"Loaded {len(documents)} knowledge docs ({len(combined)} chars total)")
    return combined


def invalidate_cache():
    """Force knowledge to be reloaded on next call. Use after editing files."""
    _cache["knowledge"] = ""
    _cache["timestamp"] = 0
    _cache["prompts"] = {}


def load_prompt(name: str, default: str = "") -> str:
    """Load a prompt template from knowledge/prompt_{name}.md.

    Results are cached for 60 seconds.

    Args:
        name: Prompt name (e.g. 'voice', 'chatbot', 'followup').
        default: Fallback text if the file doesn't exist.

    Returns:
        Prompt template string with {placeholders} intact.
    """
    now = time.time()
    cached = _cache["prompts"].get(name)
    if cached and (now - cached["timestamp"]) < _CACHE_TTL:
        return cached["content"]

    prompt_file = KNOWLEDGE_DIR / f"prompt_{name}.md"
    if not prompt_file.exists():
        if default:
            return default
        logger.warning(f"Prompt file not found: {prompt_file}")
        return ""

    try:
        content = prompt_file.read_text(encoding="utf-8").strip()
        _cache["prompts"][name] = {"content": content, "timestamp": now}
        return content
    except Exception as e:
        logger.error(f"Failed to read prompt {prompt_file}: {e}")
        return default
