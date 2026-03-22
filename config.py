import streamlit as st

_FIELDS = {
    "workspace_url": {"label": "Databricks Workspace URL", "placeholder": "https://adb-xxxxx.azuredatabricks.net", "type": "text"},
    "token": {"label": "Databricks Access Token", "placeholder": "dapi...", "type": "password"},
    "warehouse_id": {"label": "SQL Warehouse ID", "placeholder": "e.g. abc123def456", "type": "text"},
    "cluster_id": {"label": "Cluster ID (required for PySpark)", "placeholder": "e.g. 0123-456789-abcde123", "type": "text"},
    "claude_api_key": {"label": "Claude API Key", "placeholder": "sk-ant-...", "type": "password"},
}


def init_session_state():
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


def get(key):
    return st.session_state.get(key, "")


def render_sidebar_fields():
    st.markdown(
        """<script>
        document.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('input').forEach(function(el) {
                el.setAttribute('autocomplete', 'off');
            });
        });
        // also catch inputs added later by Streamlit
        new MutationObserver(function(mutations) {
            document.querySelectorAll('input').forEach(function(el) {
                el.setAttribute('autocomplete', 'off');
            });
        }).observe(document.body, {childList: true, subtree: true});
        </script>""",
        unsafe_allow_html=True,
    )

    for key, meta in _FIELDS.items():
        if meta["type"] == "password":
            st.session_state[key] = st.text_input(
                meta["label"],
                value=st.session_state.get(key, ""),
                type="password",
                placeholder=meta["placeholder"],
                key=f"input_{key}",
                autocomplete="off",
            )
        else:
            st.session_state[key] = st.text_input(
                meta["label"],
                value=st.session_state.get(key, ""),
                placeholder=meta["placeholder"],
                key=f"input_{key}",
                autocomplete="off",
            )


def has_sql_config():
    return bool(get("workspace_url") and get("token") and get("warehouse_id"))


def has_pyspark_config():
    return bool(get("workspace_url") and get("token") and get("cluster_id"))


def has_claude_config():
    return bool(get("claude_api_key"))
