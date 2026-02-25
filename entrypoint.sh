#!/bin/bash
# Copy any new knowledge files from the Docker image into the persistent volume.
# Existing files are NOT overwritten so user edits are preserved.
# This ensures new prompt_*.md files (or any new knowledge docs added in code)
# appear in the volume on the next deploy.

if [ -d /app/knowledge.defaults ]; then
    for f in /app/knowledge.defaults/*.md; do
        basename=$(basename "$f")
        if [ ! -f "/app/knowledge/$basename" ]; then
            cp "$f" "/app/knowledge/$basename"
            echo "Copied new knowledge file: $basename"
        fi
    done
fi

exec "$@"
