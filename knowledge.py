"""Knowledge Base Loader

Reads all markdown files from the knowledge/ directory and combines them
into a single context string for the AI agent's system prompt.

Files are re-read on each call so you can update docs without restarting the server.
"""

import os
from pathlib import Path

from loguru import logger

KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


def load_knowledge() -> str:
    """Load all .md files from knowledge/ directory, excluding prompt_* files.

    Returns:
        Combined content of all knowledge documents.
    """
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
                logger.debug(f"Loaded knowledge: {md_file.name} ({len(content)} chars)")
        except Exception as e:
            logger.error(f"Failed to read {md_file}: {e}")

    if not documents:
        logger.warning("No knowledge documents found")
        return ""

    combined = "\n\n".join(documents)
    logger.info(f"Loaded {len(documents)} knowledge docs ({len(combined)} chars total)")
    return combined


def load_prompt(name: str, default: str = "") -> str:
    """Load a prompt template from knowledge/prompt_{name}.md.

    Args:
        name: Prompt name (e.g. 'voice', 'chatbot', 'followup').
        default: Fallback text if the file doesn't exist.

    Returns:
        Prompt template string with {placeholders} intact.
    """
    prompt_file = KNOWLEDGE_DIR / f"prompt_{name}.md"
    if not prompt_file.exists():
        if default:
            logger.debug(f"Prompt file {prompt_file.name} not found, using default")
            return default
        logger.warning(f"Prompt file not found: {prompt_file}")
        return ""

    try:
        content = prompt_file.read_text(encoding="utf-8").strip()
        logger.debug(f"Loaded prompt: {prompt_file.name} ({len(content)} chars)")
        return content
    except Exception as e:
        logger.error(f"Failed to read prompt {prompt_file}: {e}")
        return default
