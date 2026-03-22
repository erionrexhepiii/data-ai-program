import anthropic
from utils import format_schema_for_prompt, strip_markdown_fences

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 2048

# language prompts - just add a new key here for other languages later
LANGUAGE_PROMPTS = {
    "en": {
        "sql": {
            "role": "You are a Databricks SQL assistant. The user will describe what they want in English.",
            "instruction": (
                "Return ONLY the raw SQL code. No explanation, no markdown fences, "
                "no commentary — just the executable Databricks SQL statement. "
                "Use standard Databricks SQL syntax."
            ),
        },
        "pyspark": {
            "role": "You are a Databricks PySpark assistant. The user will describe what they want in English.",
            "instruction": (
                "Return ONLY the raw PySpark code using SparkSession "
                "(use `spark` as the session variable). No explanation, no markdown "
                "fences, no commentary — just the executable PySpark code. "
                "Use Databricks-compatible PySpark syntax. "
                "Always end with a display() or show() call so results are visible."
            ),
        },
    },
}

CURRENT_LANG = "en"


def _build_system_prompt(mode, schema=None):
    mode_key = mode.lower()
    lang = LANGUAGE_PROMPTS[CURRENT_LANG][mode_key]
    dialect = "SQL" if mode_key == "sql" else "PySpark"

    prompt = (
        f"{lang['role']}\n\n"
        f"{lang['instruction']}\n\n"
        f"IMPORTANT:\n"
        f"- Always use Databricks {dialect} dialect.\n"
        f"- Do not wrap output in markdown code fences.\n"
        f"- Do not add any natural language before or after the code.\n"
        f"- If the user request is ambiguous, make a reasonable assumption and write the code."
    )

    schema_text = format_schema_for_prompt(schema)
    if schema_text:
        prompt += (
            "\n\nAVAILABLE DATABASE SCHEMA:\n"
            "The following tables and columns are available in the Databricks workspace. "
            "Use these exact table and column names when writing queries.\n\n"
            + schema_text
        )

    return prompt


def generate_code(api_key, user_message, mode="SQL", schema=None):
    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=_build_system_prompt(mode, schema),
        messages=[{"role": "user", "content": user_message}],
    )

    text = message.content[0].text if message.content else ""
    return strip_markdown_fences(text)
