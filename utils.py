import re

_DANGEROUS_SQL = re.compile(r"\b(DELETE|DROP|TRUNCATE|UPDATE|ALTER)\b", re.IGNORECASE)

_DANGEROUS_PYSPARK = re.compile(
    r"(\.drop\s*\(|\.delete\s*\(|\btruncate\b|\.write\.mode\s*\(\s*[\"']overwrite[\"']\s*\))"
    r"|\b(DELETE|DROP|TRUNCATE|UPDATE)\b",
    re.IGNORECASE,
)


def is_dangerous(code, mode="sql"):
    if _DANGEROUS_SQL.search(code):
        return True
    if mode == "pyspark" and _DANGEROUS_PYSPARK.search(code):
        return True
    return False


def dangerous_keyword(code, mode="sql"):
    m = _DANGEROUS_SQL.search(code)
    if m:
        return m.group(0).upper()
    if mode == "pyspark":
        m = _DANGEROUS_PYSPARK.search(code)
        if m:
            return (m.group(0) or "").strip().upper() or "DESTRUCTIVE OPERATION"
    return None


def format_schema_for_prompt(schema):
    if not schema:
        return None
    lines = []
    for tbl in schema:
        cols = "\n".join(
            f"  - {c['name']} ({c.get('type_name') or c.get('type_text') or 'unknown'})"
            for c in tbl.get("columns", [])
        )
        lines.append(f"TABLE: {tbl['full_name']}\n{cols}")
    return "\n\n".join(lines)


def strip_markdown_fences(text):
    text = re.sub(r"^```(?:sql|pyspark|python)?\s*\n?", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\n?```\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()
