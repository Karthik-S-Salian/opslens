"""Chunking for runbooks and postmortems.

Paragraph-aware, size-bounded, with overlap so a remediation step is never cut in half.
"""

from __future__ import annotations

DEFAULT_SIZE = 1200
DEFAULT_OVERLAP = 150


def chunk(text_value: str, size: int = DEFAULT_SIZE, overlap: int = DEFAULT_OVERLAP) -> list[str]:
    text_value = text_value.strip()
    if not text_value:
        return []
    if len(text_value) <= size:
        return [text_value]

    chunks: list[str] = []
    current = ""
    for paragraph in text_value.split("\n\n"):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        if len(current) + len(paragraph) + 2 <= size:
            current = f"{current}\n\n{paragraph}" if current else paragraph
            continue
        if current:
            chunks.append(current)
            current = current[-overlap:] if overlap else ""
        while len(paragraph) > size:
            chunks.append(paragraph[:size])
            paragraph = paragraph[size - overlap :]
        current = f"{current}\n\n{paragraph}" if current else paragraph
    if current:
        chunks.append(current)
    return chunks
