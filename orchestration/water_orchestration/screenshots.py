"""Screenshot prompts: logged at the moment the evidence is visible, and kept as asset metadata."""
from __future__ import annotations

from dagster import MetadataValue


def prompt(context, url: str, what: str) -> dict:
    context.log.info(f"[SCREENSHOT] Open {url} and capture: {what}")
    return {"screenshot": MetadataValue.md(f"[SCREENSHOT] [{url}]({url}): {what}")}
