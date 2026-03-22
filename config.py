"""Configuration management — all credentials live in st.session_state only."""

import streamlit as st

# Keys used in session_state
_FIELDS = {
    "workspace_url": {"label": "Databricks Workspace URL", "placeholder": "https://adb-xxxxx.azuredatabricks.net", "type": "text"},
    "token": {"label": "Databricks Access Token", "placeholder": "dapi...", "type": "password"},
    "warehouse_id": {"label": "SQL Warehouse ID", "placeholder": "e.g. abc123def456", "type": "text"},
    "cluster_id": {"label": "Cluster ID (required for PySpark)", "placeholder": "e.g. 0123-456789-abcde123", "type": "text"},
    "claude_api_key": {"label": "Claude API Key", "placeholder": "sk-ant-...", "type": "password"},
}


def init_session_state() -> None:
    """Ensure every config key exists in session_state (empty string default)."""
    for key in _FIELDS:
        if key not in st.session_state:
            st.session_state[key] = ""
    if "query_history" not in st.session_state:
        st.session_state["query_history"] = []
    if "schema" not in st.session_state:
        st.session_state["schema"] = None
    if "generated_code" not in st.session_state:
        st.session_state["generated_code"] = ""
    if "mode" not in st.session_state:
        st.session_state["mode"] = "SQL"


def get(key: str) -> str:
    """Read a config value from session_state."""
    return st.session_state.get(key, "")


def render_sidebar_fields() -> None:
    """Render the credential input fields in the sidebar and persist on change."""
    for key, meta in _FIELDS.items():
        if meta["type"] == "password":
            st.session_state[key] = st.text_input(
                meta["label"],
                value=st.session_state.get(key, ""),
                type="password",
                placeholder=meta["placeholder"],
                key=f"input_{key}",
            )
        else:
            st.session_state[key] = st.text_input(
                meta["label"],
                value=st.session_state.get(key, ""),
                placeholder=meta["placeholder"],
                key=f"input_{key}",
            )


def has_sql_config() -> bool:
    """Return True if the minimum config for SQL execution is present."""
    return bool(get("workspace_url") and get("token") and get("warehouse_id"))


def has_pyspark_config() -> bool:
    """Return True if the minimum config for PySpark execution is present."""
    return bool(get("workspace_url") and get("token") and get("cluster_id"))


def has_claude_config() -> bool:
    """Return True if the Claude API key is set."""
    return bool(get("claude_api_key"))
