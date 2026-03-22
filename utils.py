"""Shared helpers used across the Databricks AI Assistant."""

import re

# ─── Dangerous keyword detection ────────────────────────────────────────────

_DANGEROUS_SQL = re.compile(
    r"\b(DELETE|DROP|TRUNCATE|UPDATE|ALTER)\b", re.IGNORECASE
)

_DANGEROUS_PYSPARK = re.compile(
    r"(\.drop\s*\(|\.delete\s*\(|\btruncate\b|\.write\.mode\s*\(\s*[\"']overwrite[\"']\s*\))"
    r"|\b(DELETE|DROP|TRUNCATE|UPDATE)\b",
    re.IGNORECASE,
)


def is_dangerous(code: str, mode: str = "sql") -> bool:
    """Return True if *code* contains destructive keywords for the given mode."""
    if _DANGEROUS_SQL.search(code):
        return True
    if mode == "pyspark" and _DANGEROUS_PYSPARK.search(code):
        return True
    return False


def dangerous_keyword(code: str, mode: str = "sql") -> str | None:
    """Return the first dangerous keyword found, or None."""
    m = _DANGEROUS_SQL.search(code)
    if m:
        return m.group(0).upper()
    if mode == "pyspark":
        m = _DANGEROUS_PYSPARK.search(code)
        if m:
            return (m.group(0) or "").strip().upper() or "DESTRUCTIVE OPERATION"
    return None


# ─── Schema formatting ──────────────────────────────────────────────────────

def format_schema_for_prompt(schema: list[dict]) -> str | None:
    """Format a list of table dicts into a text block for the Claude system prompt."""
    if not schema:
        return None
    lines: list[str] = []
    for tbl in schema:
        cols = "\n".join(
            f"  - {c['name']} ({c.get('type_name') or c.get('type_text') or 'unknown'})"
            for c in tbl.get("columns", [])
        )
        lines.append(f"TABLE: {tbl['full_name']}\n{cols}")
    return "\n\n".join(lines)


# ─── Code cleanup ───────────────────────────────────────────────────────────

def strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences that Claude may add despite instructions."""
    text = re.sub(r"^```(?:sql|pyspark|python)?\s*\n?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n?```\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()
